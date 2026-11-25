package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import io.github.ppzxc.strand.nio.internal.TestNio;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.IntUnaryOperator;
import org.jspecify.annotations.Nullable;

final class Peer implements AutoCloseable {

    static final ExecutorService HANDLERS = TestNio.newExecutor("integration-handler");

    private static final long POLL_NANOS = TimeUnit.MICROSECONDS.toNanos(200);

    static final String REQUEST_RECEIVED = "REQUEST_RECEIVED";
    static final String RESULT = "RESULT";
    static final String CLOSED = "CLOSED";

    static final class Inbox {
        final BlockingQueue<Wire.Incoming> requests = new LinkedBlockingQueue<>();
        final List<String> log = Collections.synchronizedList(new ArrayList<>());

        Wire.Handler handler(@Nullable IntUnaryOperator statusForType) {
            return request -> {
                log.add(REQUEST_RECEIVED);
                requests.add(request);
                if (statusForType != null) {
                    request.respond(statusForType.applyAsInt(request.type()), Fixtures.payload("ok"));
                }
            };
        }
    }

    static final class Sent {
        private final RequestHandle handle;

        private Sent(RequestHandle handle) {
            this.handle = handle;
        }

        boolean isResultRetained() {
            return handle.result().isDone();
        }

        CompletableFuture<RequestResult> result() {
            return handle.result().toCompletionStage(HANDLERS).toCompletableFuture();
        }

        void cancel() {
            handle.cancel();
        }
    }

    private final Wire.Conn connection;
    private final Wire.@Nullable Client owner;
    private final @Nullable ControlledPassage passage;
    private final Inbox inbox;
    private final CompletableFuture<CloseNotice> closed = new CompletableFuture<>();
    private final List<CloseNotice> closedNotices = new CopyOnWriteArrayList<>();
    private final List<CompletableFuture<Void>> resultsLogged = new CopyOnWriteArrayList<>();

    Peer(Wire.Conn connection, Wire.@Nullable Client owner, @Nullable ControlledPassage passage, Inbox inbox) {
        this.connection = connection;
        this.owner = owner;
        this.passage = passage;
        this.inbox = inbox;
        connection.closed().observe(notice -> {
            inbox.log.add(CLOSED);
            closedNotices.add(notice);
            closed.complete(notice);
        });
    }

    static Peer connect(Transport transport, int port, @Nullable IntUnaryOperator statusForType) throws Exception {
        return connectLater(transport, port, statusForType).get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    static CompletableFuture<Peer> connectLater(
            Transport transport, int port, @Nullable IntUnaryOperator statusForType) {
        return connectLater(transport, port, statusForType, null);
    }

    static Peer connect(
            Transport transport, int port, @Nullable IntUnaryOperator statusForType, Duration idlePingInterval)
            throws Exception {
        return connectLater(transport, port, statusForType, idlePingInterval)
                .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private static CompletableFuture<Peer> connectLater(
            Transport transport,
            int port,
            @Nullable IntUnaryOperator statusForType,
            @Nullable Duration idlePingInterval) {
        Inbox inbox = new Inbox();
        Wire wire = transport.wire();
        Wire.Client client = wire.newClient(inbox.handler(statusForType), idlePingInterval);
        CompletableFuture<Peer> ready = new CompletableFuture<>();
        client.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port))
                .whenCompleteAsync(
                        (connection, failure) -> {
                            if (failure != null) {
                                ready.completeExceptionally(failure);
                                return;
                            }
                            try {
                                ready.complete(new Peer(connection, client, wire.claimPassage(false), inbox));
                            } catch (RuntimeException | AssertionError e) {
                                ready.completeExceptionally(e);
                            }
                        },
                        HANDLERS);
        return ready;
    }

    ControlledPassage passage() {
        ControlledPassage wrapped = passage;
        if (wrapped == null) {
            throw new IllegalStateException("PassageObservation으로 감싼 통로가 아니다");
        }
        return wrapped;
    }

    Sent request(int type, byte[] payload) {
        return new Sent(connection.request(Request.of(type, payload)));
    }

    void closeConnection() {
        connection.close();
    }

    Wire.Incoming awaitRequest() throws InterruptedException {
        Wire.Incoming request = inbox.requests.poll(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (request == null) {
            throw new AssertionError("요청을 " + Fixtures.TIMEOUT_MS + "ms 안에 받지 못했다");
        }
        return request;
    }

    List<Wire.Incoming> drainRequests() {
        List<Wire.Incoming> drained = new ArrayList<>();
        inbox.requests.drainTo(drained);
        return drained;
    }

    OptionalLong authenticated() {
        return connection.credentialLifetime();
    }

    CloseNotice awaitClosed() throws InterruptedException {
        try {
            return closed.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError("연결 종료 알림을 " + Fixtures.TIMEOUT_MS + "ms 안에 받지 못했다", e);
        }
    }

    List<String> log() {
        synchronized (inbox.log) {
            return new ArrayList<>(inbox.log);
        }
    }

    CompletableFuture<Void> logResult(Sent sent) {
        CompletableFuture<Void> logged = new CompletableFuture<>();
        sent.handle.result().observe(result -> {
            inbox.log.add(RESULT);
            logged.complete(null);
        });
        resultsLogged.add(logged);
        return logged;
    }

    void awaitResultsLogged() throws Exception {
        for (CompletableFuture<Void> logged : resultsLogged) {
            logged.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    List<CloseNotice> closedNotices() {
        return new ArrayList<>(closedNotices);
    }

    void awaitReceivedBytes(long atLeast) {
        awaitUntil(
                () -> passage().readBytes() >= atLeast,
                "받은 바이트가 " + atLeast + "에 이르지 못했다: " + passage().readBytes());
    }

    static void awaitUntil(BooleanSupplier condition, String failure) {
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Fixtures.TIMEOUT_MS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError(failure);
            }

            LockSupport.parkNanos(POLL_NANOS);
        }
    }

    void stop() {
        ControlledPassage wrapped = passage;
        if (wrapped != null) {
            wrapped.release();
            wrapped.unlimitWrites();
            wrapped.releaseWrites();
        }
        connection.abort();
        try {
            connection
                    .closed()
                    .toCompletionStage(HANDLERS)
                    .toCompletableFuture()
                    .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (owner != null) {
                owner.shutdown().get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError("연결이 " + Fixtures.TIMEOUT_MS + "ms 안에 정리되지 않았다", e);
        }
    }

    @Override
    public void close() {
        stop();
    }
}
