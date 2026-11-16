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
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jspecify.annotations.Nullable;

/**
 * JDK 11 {@code java.net.http.WebSocket} 위의 메시지 통로. WebSocket은 메시지 하나가 프레임 하나이므로 {@link Inbound#message}로 넘기고, 연결
 * 드라이버는 {@code EndpointConfig.webSocket()}으로 설정해 쓴다. JDK 11 API가 필요해서 core·nio 모듈이 아니라 예제 모듈에 둔다.
 *
 * <p>JDK는 콜백으로 메시지를 밀어 넣으므로 {@link #listener()}가 큐에 쌓고 {@link #await}가 꺼낸다. 메시지 조각은 하나로 모아 넘긴다. 다음 메시지는
 * 앞서 넘긴 메시지를 {@code await}가 꺼낸 뒤에야 요청하므로 읽지 않는 쪽으로 메시지가 무한히 쌓이지 않는다. 조각이 이어지는 동안에는 다음 조각을 바로
 * 요청한다.
 *
 * <p>쓰기는 비블로킹이다. JDK는 보내기가 끝나기 전에 다음 보내기를 받지 않으므로, 앞선 보내기가 끝나지 않았으면 {@link #write}는 0을 돌려주고
 * 보내기가 끝나면 쓰기를 기다리던 {@link #await}가 깨어난다. 메시지는 JDK가 한 번에 받으므로 받아 주면 메시지 하나를 통째로 받은 것이다.
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

    // 기다림과 깨움은 이 모니터 하나로 한다. 큐, wakeRequested는 이 모니터로 지키고, 큐에 넣거나 깨우거나 보내기가 끝나면 notifyAll한다.
    private final Object lock = new Object();
    // 쌓는 것은 읽은 메시지(END 포함)인 Inbound 또는 Throwable이다.
    private final ArrayDeque<Object> queue = new ArrayDeque<>();
    private boolean wakeRequested;
    private final ByteArrayOutputStream pendingBinary = new ByteArrayOutputStream();
    private final ByteArrayOutputStream pendingText = new ByteArrayOutputStream();
    private final WebSocket.Listener listener = new Receiver();
    private final int maxMessageBytes;
    private volatile WebSocket socket;
    private volatile boolean closed;
    private boolean ended;
    private long encodedChars;
    // 아직 끝나지 않은 마지막 보내기. 전송 스레드만 바꾸고, 완료 콜백은 완료 여부만 본다.
    private volatile @Nullable CompletableFuture<?> sending;
    // 보내기가 실패한 첫 원인. 한 번 생기면 연결이 끝난 것으로 보고 await와 write가 던진다.
    private volatile @Nullable Throwable sendFailure;

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
    public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        throwIfClosed();
        if (waitForRead && ended) {
            return Inbound.end();
        }
        Object item;
        synchronized (lock) {
            awaitReady(timeoutMs, waitForRead, waitForWrite);
            // 깨움은 어느 기다림에서든 한 번 깨우고 사라진다. 드라이버는 깨어난 스텝마다 명령을 처리한다.
            wakeRequested = false;
            throwIfClosed();
            item = waitForRead ? queue.poll() : removeFailure();
        }
        if (item == null) {
            // 앞서 도착한 입력(상대의 CLOSE 등)은 보내기 실패보다 먼저 넘긴다. 읽을 것이 더 없을 때 실패를 알린다.
            throwIfFailed();
            return Inbound.none();
        }
        if (item instanceof IOException) {
            throw (IOException) item;
        }
        if (item instanceof Throwable) {
            throw new IOException("WebSocket 오류", (Throwable) item);
        }
        Inbound inbound = (Inbound) item;
        if (inbound.kind() == Inbound.Kind.END) {
            ended = true;
        } else {
            // 넘긴 메시지를 꺼냈으니 다음 메시지를 받을 수 있다.
            socket.request(1);
        }
        return inbound;
    }

    /** {@code lock}을 쥔 채 부른다. 끝낼 이유가 생기거나 시간이 다 될 때까지 기다린다. */
    private void awaitReady(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(timeoutMs, 0));
        try {
            while (!ready(waitForRead, waitForWrite)) {
                if (timeoutMs == 0) {
                    return;
                }
                if (timeoutMs < 0) {
                    lock.wait();
                } else {
                    long remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0) {
                        return;
                    }
                    TimeUnit.NANOSECONDS.timedWait(lock, remainingNanos);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("기다리는 중 인터럽트", e);
        }
    }

    /** {@code lock}을 쥔 채 부른다. 읽기를 껐을 때는 쌓인 입력이 아니라 쌓인 오류만 끝낼 이유가 된다. */
    private boolean ready(boolean waitForRead, boolean waitForWrite) {
        return wakeRequested
                || closed
                || sendFailure != null
                || (waitForRead ? !queue.isEmpty() : hasFailure())
                || (waitForWrite && writable());
    }

    private boolean hasFailure() {
        for (Object item : queue) {
            if (item instanceof Throwable) {
                return true;
            }
        }
        return false;
    }

    /** {@code lock}을 쥔 채 부른다. 쌓인 오류 가운데 먼저 온 것을 꺼낸다. 입력은 큐에 그대로 남는다. */
    private @Nullable Object removeFailure() {
        for (Iterator<Object> it = queue.iterator(); it.hasNext(); ) {
            Object item = it.next();
            if (item instanceof Throwable) {
                it.remove();
                return item;
            }
        }
        return null;
    }

    private void enqueue(Object item) {
        synchronized (lock) {
            queue.add(item);
            lock.notifyAll();
        }
    }

    /**
     * 앞선 보내기가 끝나 다음 메시지를 받을 수 있는가. JDK는 보내기를 돌려주는 future를 끝내기 전에 "진행 중인 보내기" 표시를 내리므로(내부 {@code
     * enqueueExclusively}가 {@code whenComplete}로 감싼 future를 돌려준다), future가 끝났으면 다음 보내기를 받는다.
     */
    private boolean writable() {
        CompletableFuture<?> last = sending;
        return last == null || last.isDone();
    }

    private void throwIfClosed() throws IOException {
        if (closed) {
            throw new IOException("닫힌 통로");
        }
    }

    private void throwIfClosedOrFailed() throws IOException {
        throwIfClosed();
        throwIfFailed();
    }

    private void throwIfFailed() throws IOException {
        Throwable failure = sendFailure;
        if (failure != null) {
            throw failure instanceof IOException ? (IOException) failure : new IOException("WebSocket 쓰기 실패", failure);
        }
    }

    private void signal() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    private void fail(Throwable failure) {
        synchronized (lock) {
            // 첫 원인을 남긴다. 뒤따르는 보내기의 연쇄 실패가 가리지 않게 한다.
            if (sendFailure == null) {
                sendFailure = unwrapped(failure);
            }
            lock.notifyAll();
        }
    }

    /** 보내기가 끝나면 쓰기를 기다리는 쪽을 깨우고, 실패는 {@link #sendFailure}로 남겨 다음 {@link #await}가 던지게 한다. */
    private void watch(CompletableFuture<?> send) {
        sending = send;
        send.whenComplete((sent, failure) -> {
            if (failure != null) {
                fail(failure);
            } else {
                signal();
            }
        });
    }

    @Override
    public int write(OutboundMessage message, int offset) throws IOException {
        WebSocket open = socket;
        if (open == null) {
            throw new IOException("아직 열리지 않은 통로");
        }
        throwIfClosedOrFailed();
        CompletableFuture<?> last = sending;
        boolean busy = !writable();
        if (message.kind() == OutboundMessage.Kind.BINARY) {
            if (busy) {
                // 보내기가 끝나면 쓰기 대기가 깨운다.
                return 0;
            }
            CompletableFuture<?> send = open.sendBinary(ByteBuffer.wrap(message.bytes()), true);
            failIfAlreadyFailed(send);
            watch(send);
            return message.length();
        }
        if (!open.isOutputClosed()) {
            // Close는 앞선 보내기가 끝난 뒤에 보낸다. JDK는 보내기가 겹치면 거부한다.
            int code = message.webSocketCloseCode();
            CompletableFuture<?> send = busy ? afterPrevious(last, open, code) : open.sendClose(code, "");
            failIfAlreadyFailed(send);
            watch(send);
        }
        return 0;
    }

    /** 바로 실패로 끝난 보내기는 이 호출에서 알린다. 나중에 실패하는 것은 {@link #await}가 알린다. */
    private static void failIfAlreadyFailed(CompletableFuture<?> send) throws IOException {
        if (send.isCompletedExceptionally()) {
            try {
                send.get();
            } catch (ExecutionException e) {
                throw new IOException("WebSocket 쓰기 실패", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("쓰는 중 인터럽트", e);
            }
        }
    }

    @Override
    public void wakeup() {
        synchronized (lock) {
            wakeRequested = true;
            lock.notifyAll();
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
            // 앞선 보내기(마지막 프레임이나 write가 이어 붙인 Close)가 끝난 뒤에 보낸다. 겹치면 JDK가 거부해 abort로 번진다.
            CompletableFuture<?> last = sending;
            CompletableFuture<?> close =
                    !writable() ? afterPrevious(last, open, NORMAL_CLOSURE) : closeOnce(open, NORMAL_CLOSURE);
            watch(close);
            close.whenComplete((sent, failure) -> {
                if (failure != null) {
                    open.abort();
                }
            });
        }
    }

    private static Throwable unwrapped(Throwable failure) {
        return failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
    }

    /** 앞선 보내기가 끝난 뒤(실패해도) Close를 한 번만 보낸다. JDK는 보내기가 겹치면 거부한다. */
    private static CompletableFuture<?> afterPrevious(CompletableFuture<?> previous, WebSocket open, int code) {
        return previous.handle((sent, failure) -> null).thenCompose(ignored -> closeOnce(open, code));
    }

    private static CompletableFuture<?> closeOnce(WebSocket open, int code) {
        return open.isOutputClosed() ? CompletableFuture.completedFuture(null) : open.sendClose(code, "");
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
        signal();
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
            enqueue(new IOException("메시지가 상한 " + maxMessageBytes + "바이트를 넘었다"));
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
                enqueue(Inbound.message(WebSocketMessageKind.BINARY, message));
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
                enqueue(Inbound.message(WebSocketMessageKind.TEXT, message));
            } else {
                webSocket.request(1);
            }
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            enqueue(Inbound.end());
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            enqueue(error);
        }
    }
}
