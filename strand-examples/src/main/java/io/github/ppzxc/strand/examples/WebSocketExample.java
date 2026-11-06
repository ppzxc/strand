package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** JDK 11 {@code java.net.http} WebSocket 클라이언트로 바이너리 프레임을 주고받는다. 사용법: {@code WebSocketExample ws://host:port/path} */
public final class WebSocketExample {

    private WebSocketExample() {}

    public static void main(String[] args) throws Exception {
        URI uri = URI.create(args.length > 0 ? args[0] : "ws://localhost:8080/");
        System.out.println("strand core " + StrandVersion.get());
        CountDownLatch received = new CountDownLatch(1);
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                System.out.println("수신 " + data.remaining() + " 바이트");
                received.countDown();
                webSocket.request(1);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(1);
            }
        };
        WebSocket socket = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(uri, listener)
                .join();
        // 여기서 core가 인코딩한 바이트를 보낸다. 규격 구현 전이라 자리표시 바이트를 보낸다.
        socket.sendBinary(ByteBuffer.wrap(new byte[] {0x01}), true).join();
        received.await(5, TimeUnit.SECONDS);
        socket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
    }
}
