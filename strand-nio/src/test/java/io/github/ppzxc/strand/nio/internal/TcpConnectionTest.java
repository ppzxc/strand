package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.EOFException;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** TCP 통로 위에서 서버와 클라이언트 연결 드라이버가 요청·응답을 주고받고, 상대가 끊으면(EOF, 리셋) 끝나는지 전송 모듈의 interface로 확인한다. */
class TcpConnectionTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_UNAVAILABLE = 14;

    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private ConnectionDriver start(ConnectionDriver.Builder builder) {
        ConnectionDriver driver = builder.start();
        started.add(driver);
        return driver;
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** 닫힘 알림을 받아 두는 리스너. */
    private static final class ClosedNotices implements ConnectionListener {
        final CompletableFuture<ConnectionClosed> first = new CompletableFuture<>();
        final CompletableFuture<Void> authenticated = new CompletableFuture<>();

        @Override
        public void onAuthenticated(OptionalLong credentialLifetime) {
            authenticated.complete(null);
        }

        @Override
        public synchronized void onClosed(ConnectionClosed event) {
            first.complete(event);
        }
    }

    @Test
    void aClientAndAServerOverTcpExchangeRequestsAndResponsesInBothDirections() throws Exception {
        TcpLoopback link = TcpLoopback.open();
        ConnectionDriver server = start(ConnectionDriver.server(EndpointConfig.defaults(), TcpPassage.of(link.accepted))
                .handler(
                        handlers,
                        request -> request.respond(0, bytes("s:" + text(request.body())))
                                .join()));
        ConnectionDriver client = start(ConnectionDriver.client(EndpointConfig.defaults(), TcpPassage.of(link.client))
                .handler(
                        handlers,
                        request -> request.respond(0, bytes("c:" + text(request.body())))
                                .join()));

        RequestResult fromClient =
                await(client.request(APPLICATION_TYPE, bytes("a")).result());
        RequestResult fromServer =
                await(server.request(APPLICATION_TYPE, bytes("b")).result());

        assertThat(fromClient.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(text(fromClient.body())).isEqualTo("s:a");
        assertThat(fromServer.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(text(fromServer.body())).isEqualTo("c:b");
    }

    private void peerGoneEndsPendingRequests(boolean reset) throws Exception {
        TcpLoopback link = TcpLoopback.open();
        CompletableFuture<IncomingRequest> held = new CompletableFuture<>();
        ConnectionDriver server = start(ConnectionDriver.server(EndpointConfig.defaults(), TcpPassage.of(link.accepted))
                .handler(handlers, held::complete));
        ClosedNotices notices = new ClosedNotices();
        ConnectionDriver client = start(ConnectionDriver.client(EndpointConfig.defaults(), TcpPassage.of(link.client))
                .executor(handlers)
                .listener(notices));
        RequestHandle handle = client.request(APPLICATION_TYPE, bytes("never answered"));
        await(held);
        if (reset) {
            // 닫을 때 RST를 보내게 한다.
            link.accepted.setOption(StandardSocketOptions.SO_LINGER, 0);
        }

        server.close();

        RequestResult result = await(handle.result());
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
        ConnectionClosed closed = await(notices.first);
        assertThat(closed.reason().kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
        if (reset) {
            assertThat(closed.ioException()).get().isNotInstanceOf(EOFException.class);
        } else {
            assertThat(closed.ioException()).get().isInstanceOf(EOFException.class);
        }
        client.close();
    }

    @Test
    void whenThePeerEndsTheConnectionPendingRequestsEndAndTheListenerIsNotified() throws Exception {
        peerGoneEndsPendingRequests(false);
    }

    @Test
    void whenThePeerResetsTheConnectionPendingRequestsEndAndTheListenerIsNotified() throws Exception {
        peerGoneEndsPendingRequests(true);
    }

    /** 전송 스레드를 읽기 기다림에서 붙잡아 두는 통로. 상대가 살아 있는데 읽지 않는 상황을 만든다. */
    private static final class StallablePassage implements Passage {
        private final Passage delegate;
        private final CountDownLatch resumed = new CountDownLatch(1);
        private final CountDownLatch stalled = new CountDownLatch(1);
        private volatile boolean stalling;

        StallablePassage(Passage delegate) {
            this.delegate = delegate;
        }

        void stall() {
            stalling = true;
            delegate.wakeup();
        }

        void awaitStalled() throws InterruptedException {
            assertThat(stalled.await(5, TimeUnit.SECONDS)).isTrue();
        }

        void resume() {
            resumed.countDown();
        }

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
            if (stalling) {
                stalled.countDown();
                try {
                    resumed.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("멈춘 채 중단됨", e);
                }
            }
            return delegate.await(timeoutMs, waitForRead, waitForWrite);
        }

        @Override
        public int write(OutboundMessage message, int offset) throws IOException {
            return delegate.write(message, offset);
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /** 쓴 바이트 수가 한동안 늘지 않을 때까지 기다린다. 쓰기가 소켓 버퍼에 막혀 더 나가지 않는 상태다. */
    private static void awaitWritesStopped(ConnectionDriver driver) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        long last = -1;
        int stable = 0;
        while (System.nanoTime() < limit) {
            long now = driver.writtenBytes();
            stable = now == last && now > 0 ? stable + 1 : 0;
            if (stable >= 3) {
                return;
            }
            last = now;
            Thread.sleep(200);
        }
        throw new AssertionError("쓰기가 5초 안에 멈추지 않았다");
    }

    /**
     * 서로 소켓 버퍼를 넘는 요청을 동시에 많이 보내도 두 끝이 교착하지 않는다. 쓰기가 막힌 끝도 쓰기 대기열이 읽기 멈춤 상한 아래인 동안은 읽기를 멈추지 않아 상대의 쓰기가 풀리기 때문이다. 요청 수와 크기가 흐름 제어 범위 안이라는 것에 기댄 시험이다.
     */
    @Test
    void bothEndsSendingLargeRequestsAtOnceDoNotDeadlock() throws Exception {
        TcpLoopback link = TcpLoopback.open(4096);
        RequestHandler echo = request -> request.respond(0, new byte[0]).join();
        ConnectionDriver server = start(ConnectionDriver.server(EndpointConfig.defaults(), TcpPassage.of(link.accepted))
                .handler(handlers, echo));
        ConnectionDriver client = start(ConnectionDriver.client(EndpointConfig.defaults(), TcpPassage.of(link.client))
                .handler(handlers, echo));
        List<RequestHandle> handles = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            handles.add(client.request(APPLICATION_TYPE, new byte[60_000]));
            handles.add(server.request(APPLICATION_TYPE, new byte[60_000]));
        }

        for (RequestHandle handle : handles) {
            assertThat(await(handle.result()).certainty()).isEqualTo(Certainty.RESPONSE);
        }
    }

    /**
     * 상대가 살아 있지만 읽지 않아 쓰기가 막혀 있어도 즉시 끊기는 끝나야 한다. 응답하지 않는 상대 때문에 종료가 늘어지지 않게 하려는 것이 즉시
     * 끊기의 목적이다. 상대의 전송 스레드를 붙잡아 읽지 않게 하고, 클라이언트가 소켓 버퍼를 넘게 요청을 보내 쓰기가 막힌 뒤에 끊는다. 남은 요청의 Future는
     * core가 내는 결과로 끝나고, 스스로 끊은 연결의 종료 알림에는 입출력 예외가 실리지 않는다.
     */
    @Test
    void immediateCloseEndsEvenWhileAWriteIsBlockedByAPeerThatDoesNotRead() throws Exception {
        TcpLoopback link = TcpLoopback.open(4096);
        StallablePassage serverPassage = new StallablePassage(TcpPassage.of(link.accepted));
        start(ConnectionDriver.server(EndpointConfig.defaults(), serverPassage).handler(handlers, request -> {}));
        ClosedNotices notices = new ClosedNotices();
        ConnectionDriver client = start(ConnectionDriver.client(EndpointConfig.defaults(), TcpPassage.of(link.client))
                .executor(handlers)
                .listener(notices));
        try {
            // 핸드셰이크를 마친 연결에서 상대가 읽기를 멈추게 한다.
            await(notices.authenticated);
            serverPassage.stall();
            serverPassage.awaitStalled();
            List<RequestHandle> handles = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                handles.add(client.request(APPLICATION_TYPE, new byte[60_000]));
            }

            // 클라이언트의 쓰기가 소켓 버퍼가 차서 실제로 막힌 뒤에 끊는다.
            StackWatch.awaitFrame(client.transportThread(), StackWatch::inWriteWait);
            awaitWritesStopped(client);
            CompletableFuture<Void> closing = CompletableFuture.runAsync(client::close);
            closing.get(5, TimeUnit.SECONDS);

            for (RequestHandle handle : handles) {
                RequestResult result = await(handle.result());
                assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
            }
            assertThat(await(notices.first).ioException()).isEmpty();
        } finally {
            serverPassage.resume();
        }
    }
}
