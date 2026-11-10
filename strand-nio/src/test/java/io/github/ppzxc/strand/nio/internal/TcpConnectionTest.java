package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
        assertThat(closed.ioException()).isPresent();
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
}
