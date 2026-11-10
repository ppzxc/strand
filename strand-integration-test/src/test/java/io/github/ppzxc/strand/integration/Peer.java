package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.ConnectionListener;
import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.RequestHandle;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.IntUnaryOperator;

/**
 * 전송 모듈의 연결 드라이버 하나와 그 알림(받은 요청, AUTH 관문 통과, 종료)을 모아 두는 통합 테스트용 껍데기. 연결을 돌리는 일은 모두 {@link
 * ConnectionDriver}가 한다. 여기에는 드라이브 루프가 없다.
 */
final class Peer implements AutoCloseable {

    /** 핸들러와 알림은 전송 스레드를 막지 않게 이 실행기에서 돈다. */
    private static final ExecutorService HANDLERS = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "integration-handler");
        thread.setDaemon(true);
        return thread;
    });

    private static final long POLL_NANOS = TimeUnit.MICROSECONDS.toNanos(200);

    static final String REQUEST_RECEIVED = "REQUEST_RECEIVED";
    static final String RESULT = "RESULT";
    static final String CLOSED = "CLOSED";

    private final ConnectionDriver driver;
    private final BlockingQueue<IncomingRequest> requests = new LinkedBlockingQueue<>();
    private final CompletableFuture<OptionalLong> authenticated = new CompletableFuture<>();
    private final CompletableFuture<ConnectionClosed> closed = new CompletableFuture<>();
    private final List<ConnectionClosed> closedNotices = new CopyOnWriteArrayList<>();
    private final List<String> log = Collections.synchronizedList(new ArrayList<>());
    private final ControlledPassage controlled;

    private Peer(Passage passage, boolean client, IntUnaryOperator statusForType) {
        this.controlled = passage instanceof ControlledPassage ? (ControlledPassage) passage : null;
        EndpointConfig config = EndpointConfig.defaults();
        ConnectionDriver.Builder builder =
                client ? ConnectionDriver.client(config, passage) : ConnectionDriver.server(config, passage);
        this.driver = builder.handler(HANDLERS, request -> {
                    log.add(REQUEST_RECEIVED);
                    requests.add(request);
                    if (statusForType != null) {
                        request.respond(statusForType.applyAsInt(request.type()), Fixtures.payload("ok"));
                    }
                })
                .listener(new ConnectionListener() {
                    @Override
                    public void onAuthenticated(OptionalLong credentialLifetime) {
                        authenticated.complete(credentialLifetime);
                    }

                    @Override
                    public void onClosed(ConnectionClosed event) {
                        log.add(CLOSED);
                        closedNotices.add(event);
                        closed.complete(event);
                    }
                })
                .start();
    }

    /** 이미 맺어진 TCP 채널 위에서 읽기 단위 제한 없이 도는 서버 쪽. 받은 요청마다 {@code statusForType}의 status로 응답한다({@code null}이면 응답하지 않는다). */
    static Peer server(Passage passage, IntUnaryOperator statusForType) {
        return new Peer(passage, false, statusForType);
    }

    /** {@link #server}의 클라이언트 쪽. */
    static Peer client(Passage passage, IntUnaryOperator statusForType) {
        return new Peer(passage, true, statusForType);
    }

    /** 서버에 TCP로 연결하는 클라이언트. 받은 요청마다 {@code statusForType}으로 응답한다({@code null}이면 응답하지 않는다). */
    static Peer connect(int port, IntUnaryOperator statusForType) throws IOException {
        return client(tcp(open(port), TcpPassage.DEFAULT_READ_BUFFER_BYTES), statusForType);
    }

    /** {@link #connect(int, IntUnaryOperator)}와 같되, 한 번에 {@code readChunkBytes}바이트까지만 읽는다. 프레임이 쪼개져 오는 경우를 만든다. */
    static Peer connect(int port, IntUnaryOperator statusForType, int readChunkBytes) throws IOException {
        return client(tcp(open(port), readChunkBytes), statusForType);
    }

    static SocketChannel open(int port) throws IOException {
        return SocketChannel.open(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
    }

    static Passage tcp(SocketChannel channel, int readChunkBytes) {
        try {
            return TcpPassage.of(channel, readChunkBytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 이 연결이 {@link ControlledPassage}로 감싼 통로일 때 그 통로. */
    ControlledPassage passage() {
        if (controlled == null) {
            throw new IllegalStateException("ControlledPassage로 감싼 통로가 아니다");
        }
        return controlled;
    }

    ConnectionDriver driver() {
        return driver;
    }

    RequestHandle request(int type, byte[] payload) {
        return driver.request(type, payload);
    }

    /** 받은 요청을 받은 순서대로 하나 기다려 꺼낸다. 상한 안에 오지 않으면 실패한다. */
    IncomingRequest awaitRequest() throws InterruptedException {
        IncomingRequest request = requests.poll(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (request == null) {
            throw new AssertionError("요청을 " + Fixtures.TIMEOUT_MS + "ms 안에 받지 못했다");
        }
        return request;
    }

    /** 아직 꺼내지 않은 받은 요청을 모두 꺼낸다. */
    List<IncomingRequest> drainRequests() {
        List<IncomingRequest> drained = new ArrayList<>();
        requests.drainTo(drained);
        return drained;
    }

    /** AUTH 관문 통과(클라이언트) 알림을 기다린다. 오지 않으면 실패한다. */
    OptionalLong awaitAuthenticated() throws InterruptedException {
        try {
            return authenticated.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError("AUTH 관문 통과를 " + Fixtures.TIMEOUT_MS + "ms 안에 보지 못했다", e);
        }
    }

    /** 연결 종료 알림을 기다린다. 오지 않으면 실패한다. */
    ConnectionClosed awaitClosed() throws InterruptedException {
        try {
            return closed.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError("연결 종료 알림을 " + Fixtures.TIMEOUT_MS + "ms 안에 받지 못했다", e);
        }
    }

    /**
     * 이 연결에서 일어난 일을 일어난 순서대로 적은 기록({@link #REQUEST_RECEIVED}, {@link #RESULT}, {@link #CLOSED}). 받은 요청은 핸들러가 응답하기 전에,
     * 종료 알림은 리스너가 불릴 때 적는다. 결과는 {@link #logResult}로 맡긴 Future가 끝날 때 전송 스레드에서 적으므로, 연결 종료 때 core가 낸 결과가
     * 종료 알림보다 먼저 적힌다.
     */
    List<String> log() {
        synchronized (log) {
            return new ArrayList<>(log);
        }
    }

    /** 이 요청 결과가 끝나는 순간을 {@link #log()}에 적게 한다. 결과가 나오기 전에, 즉 요청을 낸 직후에 걸어야 순서를 놓치지 않는다. */
    CompletableFuture<Void> logResult(CompletableFuture<?> result) {
        return result.thenRun(() -> log.add(RESULT));
    }

    /** 지금까지 받은 종료 알림. 연결당 정확히 한 번이어야 한다. */
    List<ConnectionClosed> closedNotices() {
        return new ArrayList<>(closedNotices);
    }

    /** 처리를 끝낸 받은 바이트가 {@code atLeast}에 이를 때까지 기다린다. 상대가 CLOSE 같은 프레임을 처리했는지 볼 때 쓴다. 이르지 못하면 실패한다. */
    void awaitReceivedBytes(long atLeast) {
        awaitUntil(
                () -> driver.receivedBytes() >= atLeast, "받은 바이트가 " + atLeast + "에 이르지 못했다: " + driver.receivedBytes());
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

    /** 연결을 즉시 끊고 전송 스레드가 끝나길 기다린다. 다른 연결에는 영향이 없다. 여러 번 불러도 된다. */
    void stop() {
        if (controlled != null) {
            controlled.release();
        }
        driver.close();
    }

    @Override
    public void close() {
        stop();
    }
}
