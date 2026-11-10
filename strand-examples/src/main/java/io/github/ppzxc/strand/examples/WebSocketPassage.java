package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.Passage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JDK 11 {@code java.net.http.WebSocket} 위의 메시지 통로. WebSocket은 메시지 하나가 프레임 하나이므로 {@link Inbound#message}로 넘기고, 연결
 * 드라이버는 {@code EndpointConfig.webSocket()}으로 설정해 쓴다. JDK 11 API가 필요해서 core·nio 모듈이 아니라 예제 모듈에 둔다.
 *
 * <p>JDK는 콜백으로 메시지를 밀어 넣으므로 {@link #listener()}가 큐에 쌓고 {@link #await}가 꺼낸다. 메시지 조각은 하나로 모아 넘긴다. 다음 메시지는
 * 앞서 넘긴 메시지를 {@code await}가 꺼낸 뒤에야 요청하므로 읽지 않는 쪽으로 메시지가 무한히 쌓이지 않는다. 조각이 이어지는 동안에는 다음 조각을 바로
 * 요청한다.
 *
 * <p>{@link #await}와 {@link #write}는 전송 스레드 하나만 부른다. {@link #wakeup}과 {@link #close}는 어느 스레드에서든 부른다.
 */
public final class WebSocketPassage implements Passage {

    private static final int NORMAL_CLOSURE = 1000;
    private static final long BACKSTOP_MILLIS = 1000;

    /** 연결(핸드셰이크 완료)을 기다리는 기본 상한. */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);

    /** 메시지 크기의 기본 상한(바이트). 규격의 최대 프레임 크기 상한(1 MiB)에 헤더를 더하고도 남는 값이다. */
    public static final int DEFAULT_MAX_MESSAGE_BYTES = 2 * 1024 * 1024;

    /** 큐에 쌓는 것은 {@link Inbound}(깨움은 {@link Inbound#none()}) 또는 {@link Throwable}이다. */
    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();

    private final AtomicBoolean woken = new AtomicBoolean();
    private final ByteArrayOutputStream pendingBinary = new ByteArrayOutputStream();
    private final ByteArrayOutputStream pendingText = new ByteArrayOutputStream();
    private final WebSocket.Listener listener = new Receiver();
    private final int maxMessageBytes;
    private volatile WebSocket socket;
    private volatile boolean closed;
    private boolean ended;
    private long encodedChars;

    /** 열린 소켓은 {@link #listener()}의 {@code onOpen}으로 들어온다. 직접 열려면 {@link #connect}를 쓴다. */
    WebSocketPassage() {
        this(DEFAULT_MAX_MESSAGE_BYTES);
    }

    WebSocketPassage(int maxMessageBytes) {
        if (maxMessageBytes < 1) {
            throw new IllegalArgumentException("maxMessageBytes는 1 이상이어야 한다: " + maxMessageBytes);
        }
        this.maxMessageBytes = maxMessageBytes;
    }

    /**
     * 서버에 WebSocket으로 연결하고 하위 프로토콜 {@code strand}(규격 1.2.1절)를 제시해 맺어진 통로를 돌려준다.
     *
     * @param uri {@code ws://} 또는 {@code wss://} 주소
     * @return 맺어진 통로
     * @throws IOException 연결하지 못했거나 서버가 하위 프로토콜 {@code strand}를 고르지 않았을 때
     */
    public static WebSocketPassage connect(URI uri) throws IOException {
        return connect(uri, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_CONNECT_TIMEOUT);
    }

    /**
     * {@link #connect(URI)}와 같고 받는 메시지 하나의 크기 상한을 정한다. 조각을 모은 크기가 상한을 넘으면 소켓을 끊고 다음 {@link #await}가 {@link
     * IOException}을 던진다.
     *
     * @param uri {@code ws://} 또는 {@code wss://} 주소
     * @param maxMessageBytes 메시지 하나의 최대 바이트(1 이상)
     * @return 맺어진 통로
     * @throws IOException 연결하지 못했거나 서버가 하위 프로토콜 {@code strand}를 고르지 않았을 때
     */
    public static WebSocketPassage connect(URI uri, int maxMessageBytes) throws IOException {
        return connect(uri, maxMessageBytes, DEFAULT_CONNECT_TIMEOUT);
    }

    /**
     * {@link #connect(URI, int)}와 같고 연결을 기다리는 상한을 정한다. 서버가 핸드셰이크에 응답하지 않아 상한이 지나면 JDK가 소켓을 정리하고
     * {@link IOException}으로 끝난다.
     *
     * @param uri {@code ws://} 또는 {@code wss://} 주소
     * @param maxMessageBytes 메시지 하나의 최대 바이트(1 이상)
     * @param connectTimeout 연결을 기다리는 상한
     * @return 맺어진 통로
     * @throws IOException 연결하지 못했거나 시간이 지났거나 서버가 하위 프로토콜 {@code strand}를 고르지 않았을 때
     */
    public static WebSocketPassage connect(URI uri, int maxMessageBytes, Duration connectTimeout) throws IOException {
        WebSocketPassage passage = new WebSocketPassage(maxMessageBytes);
        CompletableFuture<WebSocket> connecting = HttpClient.newHttpClient()
                .newWebSocketBuilder()
                // 핸드셰이크 응답을 기다리는 상한. 넘기면 JDK가 연결을 닫고 future를 예외로 끝낸다. 아래 get의 상한은 그보다 조금 더 둔 보험이다.
                .connectTimeout(connectTimeout)
                .subprotocols("strand")
                .buildAsync(uri, passage.listener);
        try {
            WebSocket opened = connecting.get(connectTimeout.toMillis() + BACKSTOP_MILLIS, TimeUnit.MILLISECONDS);
            passage.socket = opened;
            // JDK는 서버가 하위 프로토콜을 고르지 않은 경우도 거부하지 않는다. strand 서버가 아니면 더 진행하지 않는다.
            if (!"strand".equals(opened.getSubprotocol())) {
                passage.close();
                throw new IOException("서버가 하위 프로토콜 strand를 고르지 않았다: '" + opened.getSubprotocol() + "'");
            }
            return passage;
        } catch (ExecutionException e) {
            throw new IOException("WebSocket 연결 실패", e.getCause());
        } catch (TimeoutException e) {
            // 더 기다리지 않으니 연결 시도를 그만둔다.
            connecting.cancel(true);
            throw new IOException("WebSocket 연결 시간 초과", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            connecting.cancel(true);
            throw new IOException("WebSocket 연결 중 인터럽트", e);
        }
    }

    /** 텍스트 메시지를 UTF-8로 인코딩한 글자 수의 합. 조각을 한 번씩만 인코딩하는지 시험이 본다. */
    long encodedCharCount() {
        return encodedChars;
    }

    /** JDK 콜백을 이 통로로 이어 주는 리스너. {@code WebSocket}을 만들 때 넘긴다. */
    WebSocket.Listener listener() {
        return listener;
    }

    @Override
    public Inbound await(long timeoutMs) throws IOException {
        if (closed) {
            throw new IOException("닫힌 통로");
        }
        if (ended) {
            return Inbound.end();
        }
        Object item;
        try {
            if (timeoutMs == 0) {
                item = queue.poll();
            } else if (timeoutMs < 0) {
                item = queue.take();
            } else {
                item = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("기다리는 중 인터럽트", e);
        }
        if (closed) {
            throw new IOException("닫힌 통로");
        }
        if (item == null) {
            return Inbound.none();
        }
        if (item instanceof IOException) {
            throw (IOException) item;
        }
        if (item instanceof Throwable) {
            throw new IOException("WebSocket 오류", (Throwable) item);
        }
        Inbound inbound = (Inbound) item;
        switch (inbound.kind()) {
            case NONE:
                woken.set(false);
                break;
            case END:
                ended = true;
                break;
            default:
                // 넘긴 메시지를 꺼냈으니 다음 메시지를 받을 수 있다.
                socket.request(1);
                break;
        }
        return inbound;
    }

    @Override
    public void write(OutboundMessage message) throws IOException {
        WebSocket open = socket;
        if (open == null) {
            throw new IOException("아직 열리지 않은 통로");
        }
        try {
            if (message.kind() == OutboundMessage.Kind.BINARY) {
                open.sendBinary(ByteBuffer.wrap(message.bytes()), true).get();
            } else if (!open.isOutputClosed()) {
                open.sendClose(message.webSocketCloseCode(), "").get();
            }
        } catch (ExecutionException e) {
            throw new IOException("WebSocket 쓰기 실패", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("쓰는 중 인터럽트", e);
        }
    }

    @Override
    public void wakeup() {
        if (woken.compareAndSet(false, true)) {
            queue.add(Inbound.none());
        }
    }

    /**
     * 아직 Close를 보내지 않았다면 NORMAL_CLOSURE Close를 보낸다. 상대의 Close 응답을 읽는 것은 {@link #await}가 한다. Close를 보내지 못하면 Close 교환을
     * 할 수 없으므로 소켓을 끊는다.
     */
    @Override
    public void shutdownOutput() {
        WebSocket open = socket;
        if (open != null && !open.isOutputClosed()) {
            open.sendClose(NORMAL_CLOSURE, "").whenComplete((sent, failure) -> {
                if (failure != null) {
                    open.abort();
                }
            });
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        WebSocket open = socket;
        if (open != null) {
            open.abort();
        }
        queue.add(Inbound.none());
    }

    /** JDK가 부르는 콜백. 호출은 순서대로 오고 겹치지 않는다. */
    private final class Receiver implements WebSocket.Listener {

        private boolean overflowed;
        private String heldHighSurrogate = "";

        @Override
        public void onOpen(WebSocket webSocket) {
            socket = webSocket;
            webSocket.request(1);
        }

        /** 메시지가 상한을 넘었다. 쌓은 것을 버리고, 다음 {@link #await}가 오류를 던지게 한 뒤 소켓을 끊는다. 이후 조각은 무시한다. */
        private void overflow(WebSocket webSocket) {
            overflowed = true;
            pendingBinary.reset();
            pendingText.reset();
            queue.add(new IOException("메시지가 상한 " + maxMessageBytes + "바이트를 넘었다"));
            webSocket.abort();
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            if (overflowed) {
                return null;
            }
            if ((long) pendingBinary.size() + data.remaining() > maxMessageBytes) {
                overflow(webSocket);
                return null;
            }
            // JDK가 콜백 뒤에 버퍼를 다시 쓰므로 복사한다.
            byte[] part = new byte[data.remaining()];
            data.get(part);
            pendingBinary.write(part, 0, part.length);
            if (last) {
                byte[] message = pendingBinary.toByteArray();
                pendingBinary.reset();
                queue.add(Inbound.message(WebSocketMessageKind.BINARY, message));
            } else {
                webSocket.request(1);
            }
            return null;
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            if (overflowed) {
                return null;
            }
            // 새 조각만 인코딩해 덧붙인다(조각마다 전체를 다시 인코딩하면 제곱으로 느려진다). 조각 끝의 높은 서러게이트는 다음 조각의 낮은
            // 서러게이트와 한 글자라서 다음 조각까지 들고 있는다.
            String text = heldHighSurrogate + data.toString();
            heldHighSurrogate = "";
            if (!last && !text.isEmpty() && Character.isHighSurrogate(text.charAt(text.length() - 1))) {
                heldHighSurrogate = text.substring(text.length() - 1);
                text = text.substring(0, text.length() - 1);
            }
            byte[] encoded = text.getBytes(StandardCharsets.UTF_8);
            encodedChars += text.length();
            if ((long) pendingText.size() + encoded.length > maxMessageBytes) {
                overflow(webSocket);
                return null;
            }
            pendingText.write(encoded, 0, encoded.length);
            if (last) {
                byte[] message = pendingText.toByteArray();
                pendingText.reset();
                queue.add(Inbound.message(WebSocketMessageKind.TEXT, message));
            } else {
                webSocket.request(1);
            }
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            queue.add(Inbound.end());
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            queue.add(error);
        }
    }
}
