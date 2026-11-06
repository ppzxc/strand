package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * JDK 11 {@code java.net.http} WebSocket 위에서 클라이언트 {@link Endpoint}를 돌린다. WebSocket은 메시지 하나가 프레임 하나이므로
 * {@link EndpointConfig#webSocket()}으로 설정하고 {@link Endpoint#receiveMessage}로 넣는다. 사용법: {@code WebSocketExample
 * ws://host:port/path} (strand를 WebSocket으로 받는 서버가 필요하다)
 */
public final class WebSocketExample {

    private static final int TIMEOUT_SECONDS = 5;

    private WebSocketExample() {}

    public static void main(String[] args) throws Exception {
        URI uri = URI.create(ExampleSupport.arg(args, 0, "ws://localhost:8080/"));
        System.out.println("strand core " + StrandVersion.get());
        Endpoint endpoint = Endpoint.client(EndpointConfig.defaults().webSocket());
        CountDownLatch finished = new CountDownLatch(1);

        WebSocket.Listener listener = new WebSocket.Listener() {
            // 메시지가 여러 조각으로 올 수 있어 last가 true일 때까지 모은다.
            private final ByteArrayOutputStream pending = new ByteArrayOutputStream();

            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
                byte[] part = new byte[data.remaining()];
                data.get(part);
                pending.write(part, 0, part.length);
                if (last) {
                    byte[] message = pending.toByteArray();
                    pending.reset();
                    synchronized (endpoint) {
                        endpoint.receiveMessage(WebSocketMessageKind.BINARY, message);
                        handle(webSocket, endpoint, finished);
                    }
                }
                webSocket.request(1);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                // 규격은 텍스트 메시지를 연결 오류로 본다. 알리고 core가 닫게 한다.
                synchronized (endpoint) {
                    endpoint.receiveMessage(
                            WebSocketMessageKind.TEXT, data.toString().getBytes(StandardCharsets.UTF_8));
                    handle(webSocket, endpoint, finished);
                }
                webSocket.request(1);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                synchronized (endpoint) {
                    endpoint.disconnect();
                    handle(webSocket, endpoint, finished);
                }
                finished.countDown();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                System.out.println("[클라이언트] 오류: " + error);
                finished.countDown();
            }
        };

        WebSocket socket = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(uri, listener)
                .join();
        synchronized (endpoint) {
            endpoint.request(ExampleSupport.DEMO_REQUEST_TYPE, "hello".getBytes(StandardCharsets.UTF_8));
            handle(socket, endpoint, finished);
        }
        if (!finished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            System.out.println("[클라이언트] " + TIMEOUT_SECONDS + "초 안에 끝나지 않아 닫는다");
            socket.abort();
        }
    }

    /** core가 쌓은 메시지를 WebSocket으로 내보내고, 이벤트에 따라 다음 동작을 정한다. {@code endpoint}를 잠근 채 부른다. */
    private static void handle(WebSocket socket, Endpoint endpoint, CountDownLatch finished) {
        for (Event event : endpoint.drainEvents()) {
            System.out.println("[클라이언트] " + ExampleSupport.describe(event));
            if (event.kind() == Event.Kind.RESULT) {
                endpoint.close(ExampleSupport.NO_ERROR);
            } else if (event.kind() == Event.Kind.CLOSED) {
                finished.countDown();
            }
        }
        for (OutboundMessage message : endpoint.drainMessages()) {
            if (message.kind() == OutboundMessage.Kind.BINARY) {
                socket.sendBinary(ByteBuffer.wrap(message.bytes()), true);
            } else {
                socket.sendClose(message.webSocketCloseCode(), "");
            }
        }
    }
}
