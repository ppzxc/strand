package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.nio.IncomingRequest;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandConnection;
import io.github.ppzxc.strand.nio.RequestHandler;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
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

/**
 * 제품 공개 API({@code NioStrandServer}, {@code NioStrandClient})가 맺어 준 연결 하나와 그 알림(받은 요청, 종료)을 모아 두는 통합 테스트용 껍데기. 연결을 돌리는 일은
 * 모두 제품 엔진이 한다. 여기에는 드라이브 루프가 없다.
 *
 * <p>연결을 감싼 통로({@link ControlledPassage})는 {@link PassageObservation}이 설치돼 있을 때만 있다. 이 통로로 오간 프레임과 바이트를 본다.
 */
final class Peer implements AutoCloseable {

    /** 핸들러와 알림은 전송 스레드를 막지 않게 이 실행기에서 돈다. */
    static final ExecutorService HANDLERS = TestNio.newExecutor("integration-handler");

    private static final long POLL_NANOS = TimeUnit.MICROSECONDS.toNanos(200);

    static final String REQUEST_RECEIVED = "REQUEST_RECEIVED";
    static final String RESULT = "RESULT";
    static final String CLOSED = "CLOSED";

    /** 연결이 앱에 보이기 전부터 받는 요청과 기록. 서버가 연결을 인증한 직후나 클라이언트 연결이 준비된 직후에 상대가 먼저 요청을 낼 수 있어서 연결과 따로 만든다. */
    static final class Inbox {
        final BlockingQueue<IncomingRequest> requests = new LinkedBlockingQueue<>();
        final List<String> log = Collections.synchronizedList(new ArrayList<>());

        /** 받은 요청을 기록하고 {@code statusForType}의 status로 응답한다({@code null}이면 응답하지 않는다). */
        RequestHandler handler(@Nullable IntUnaryOperator statusForType) {
            return request -> {
                log.add(REQUEST_RECEIVED);
                requests.add(request);
                if (statusForType != null) {
                    request.respond(statusForType.applyAsInt(request.type()), Fixtures.payload("ok"));
                }
            };
        }
    }

    /** 낸 요청. 결과는 {@code Future}로도 받는다. */
    static final class Sent {
        private final RequestHandle handle;

        private Sent(RequestHandle handle) {
            this.handle = handle;
        }

        CompletableFuture<RequestResult> result() {
            return handle.result().toCompletionStage(HANDLERS).toCompletableFuture();
        }

        void cancel() {
            handle.cancel();
        }
    }

    private final NioStrandConnection connection;
    private final @Nullable NioStrandClient owner;
    private final @Nullable ControlledPassage passage;
    private final Inbox inbox;
    private final CompletableFuture<CloseNotice> closed = new CompletableFuture<>();
    private final List<CloseNotice> closedNotices = new CopyOnWriteArrayList<>();
    private final List<CompletableFuture<Void>> resultsLogged = new CopyOnWriteArrayList<>();

    Peer(
            NioStrandConnection connection,
            @Nullable NioStrandClient owner,
            @Nullable ControlledPassage passage,
            Inbox inbox) {
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

    /** 서버에 TCP로 연결하는 클라이언트. 받은 요청마다 {@code statusForType}으로 응답한다({@code null}이면 응답하지 않는다). */
    static Peer connect(int port, @Nullable IntUnaryOperator statusForType) throws Exception {
        return connectLater(port, statusForType).get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * {@link #connect}와 같되 연결이 준비되기를 기다리지 않는다. 제품 클라이언트의 연결은 AUTH 관문까지 마쳐야 앱에 보이므로 날것의 상대가 말을 걸어 줘야 끝나는 시험이 쓴다.
     */
    static CompletableFuture<Peer> connectLater(int port, @Nullable IntUnaryOperator statusForType) {
        return connectLater(port, statusForType, null);
    }

    /** {@link #connect}와 같되, 연결이 놀고 있을 때 클라이언트가 PING을 내는 간격을 정한다. 시험이 PING이 나가는 때를 시간으로 정하려고 쓴다. */
    static Peer connect(int port, @Nullable IntUnaryOperator statusForType, Duration idlePingInterval)
            throws Exception {
        return connectLater(port, statusForType, idlePingInterval).get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private static CompletableFuture<Peer> connectLater(
            int port, @Nullable IntUnaryOperator statusForType, @Nullable Duration idlePingInterval) {
        Inbox inbox = new Inbox();
        NioStrandClient.Builder builder = TestNio.clientBuilder(HANDLERS).handler(inbox.handler(statusForType));
        if (idlePingInterval != null) {
            builder.idlePingInterval(idlePingInterval);
        }
        NioStrandClient client = builder.build();
        TcpServer server = TcpServer.byPort(port);
        CompletableFuture<Peer> ready = new CompletableFuture<>();
        client.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port))
                .whenComplete((connection, failure) -> {
                    if (failure != null) {
                        ready.completeExceptionally(failure);
                        return;
                    }
                    try {
                        if (server != null) {
                            // 서버가 핸들러를 준비하기 전에 온 요청은 거절되므로(규격 4.5절) 서버 쪽 연결이 준비된 뒤에 시험에 돌려준다.
                            server.awaitPrepared();
                        }
                        ready.complete(new Peer(
                                connection, client, PassageObservation.claim(PassageWrapping.Side.CLIENT), inbox));
                    } catch (RuntimeException | AssertionError e) {
                        ready.completeExceptionally(e);
                    }
                });
        return ready;
    }

    /** 이 연결이 {@link PassageObservation}으로 감싼 통로. 설치되지 않았으면 실패한다. */
    ControlledPassage passage() {
        ControlledPassage wrapped = passage;
        if (wrapped == null) {
            throw new IllegalStateException("PassageObservation으로 감싼 통로가 아니다");
        }
        return wrapped;
    }

    NioStrandConnection connection() {
        return connection;
    }

    Sent request(int type, byte[] payload) {
        return new Sent(connection.request(Request.of(type, payload)));
    }

    /**
     * 전송 스레드가 이 호출 전에 접수된 명령을 모두 처리했음을 확인한다. 기한이 0인 요청은 접수하는 즉시 처리 안 됨으로 끝나고 통로에 쓰이지 않으며, 그 결과는 앞서 접수된 명령이 처리된 뒤에야 나온다.
     */
    void awaitEngineTurn() {
        try {
            Fixtures.await(connection
                    .request(
                            Request.of(Fixtures.APPLICATION_TYPE, new byte[0]),
                            RequestOptions.deadlineAfter(Duration.ZERO))
                    .result());
        } catch (Exception e) {
            throw new AssertionError("전송 스레드가 명령을 처리하지 않았다", e);
        }
    }

    /** 연결 종료(CLOSE) 절차를 시작한다. */
    void closeConnection() {
        connection.close();
    }

    /** 받은 요청을 받은 순서대로 하나 기다려 꺼낸다. 상한 안에 오지 않으면 실패한다. */
    IncomingRequest awaitRequest() throws InterruptedException {
        IncomingRequest request = inbox.requests.poll(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (request == null) {
            throw new AssertionError("요청을 " + Fixtures.TIMEOUT_MS + "ms 안에 받지 못했다");
        }
        return request;
    }

    /** 아직 꺼내지 않은 받은 요청을 모두 꺼낸다. */
    List<IncomingRequest> drainRequests() {
        List<IncomingRequest> drained = new ArrayList<>();
        inbox.requests.drainTo(drained);
        return drained;
    }

    /** 서버가 AUTH 관문 통과 때 알린 자격 증명 수명. 제품 연결은 AUTH 관문을 통과해야 앱에 보이므로 이 호출 전에 이미 통과했다. */
    OptionalLong authenticated() {
        return connection.credentialLifetime();
    }

    /** 연결 종료 알림을 기다린다. 오지 않으면 실패한다. */
    CloseNotice awaitClosed() throws InterruptedException {
        try {
            return closed.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError("연결 종료 알림을 " + Fixtures.TIMEOUT_MS + "ms 안에 받지 못했다", e);
        }
    }

    /**
     * 이 연결에서 일어난 일을 일어난 순서대로 적은 기록({@link #REQUEST_RECEIVED}, {@link #RESULT}, {@link #CLOSED}). 받은 요청은 핸들러가 응답하기 전에,
     * 종료 알림은 종료 결과가 확정될 때 적는다. 결과는 {@link #logResult}로 맡긴 결과가 끝날 때 적는다. 제품 연결은 남은 요청의 결과를 보존한 뒤에 종료 결과를 확정하므로, 연결 종료
     * 때 나온 결과가 종료 알림보다 먼저 적힌다.
     */
    List<String> log() {
        synchronized (inbox.log) {
            return new ArrayList<>(inbox.log);
        }
    }

    /** 이 요청의 결과가 끝나는 순간을 {@link #log()}에 적게 한다. 결과가 나오기 전에, 즉 요청을 낸 직후에 걸어야 순서를 놓치지 않는다. */
    CompletableFuture<Void> logResult(Sent sent) {
        CompletableFuture<Void> logged = new CompletableFuture<>();
        sent.handle.result().observe(result -> {
            inbox.log.add(RESULT);
            logged.complete(null);
        });
        resultsLogged.add(logged);
        return logged;
    }

    /** {@link #logResult}로 맡긴 결과가 모두 {@link #log()}에 적히길 기다린다. 기록을 읽기 전에 부른다. */
    void awaitResultsLogged() throws Exception {
        for (CompletableFuture<Void> logged : resultsLogged) {
            logged.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    /** 지금까지 받은 종료 알림. 연결당 정확히 한 번이어야 한다. */
    List<CloseNotice> closedNotices() {
        return new ArrayList<>(closedNotices);
    }

    /** 통로가 읽어 엔진에 넘긴 바이트가 {@code atLeast}에 이를 때까지 기다린다. 상대가 CLOSE 같은 프레임을 받았는지 볼 때 쓴다. 이르지 못하면 실패한다. */
    void awaitReceivedBytes(long atLeast) {
        awaitUntil(
                () -> passage().readBytes() >= atLeast,
                "받은 바이트가 " + atLeast + "에 이르지 못했다: " + passage().readBytes());
    }

    /** 상한 시간 안에서 조건이 참이 될 때까지 기다린다. 이르지 못하면 실패한다. */
    static void awaitUntil(BooleanSupplier condition, String failure) {
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Fixtures.TIMEOUT_MS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError(failure);
            }
            // 관찰 지점 카운터는 바뀔 때 알려 주지 않는다. 바쁘게 돌면 CPU만 쓰므로 짧게 쉬며 상한까지 확인한다.
            LockSupport.parkNanos(POLL_NANOS);
        }
    }

    /** 연결을 즉시 끊고 종료가 확정되길 기다린다. 다른 연결에는 영향이 없다. 여러 번 불러도 된다. */
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
                owner.shutdown().toCompletableFuture().get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
