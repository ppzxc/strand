package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;

/**
 * 소켓 하나에 {@link Endpoint} 하나를 붙여 읽기 스레드로 돌린다. core가 쌓은 메시지는 매번 소켓에 쓰고, 이벤트는 모아 둔다. {@code Endpoint}는
 * 스레드 안전하지 않으므로 모든 접근을 이 객체의 잠금 안에서 한다.
 *
 * <p>core는 전송 계층을 닫으라는 메시지를 따로 내지 않는다. {@code closed} 이벤트가 나오면 보내는 쪽만 닫고(규격 3.5절) 상대가 끊을 때까지 계속
 * 읽는다. 닫힌 뒤에 감지한 끊김이 {@code closed}를 다시 내지 않는지 보려는 것이다. {@link #close()}는 이 연결 하나만 끊는다.
 */
final class Connection implements AutoCloseable {

    /** 이벤트나 종료를 기다리는 상한. 넘기면 실패시켜 테스트가 멈추지 않게 한다. */
    static final long TIMEOUT_MS = 5000L;

    /** 읽기 단위를 따로 제한하지 않을 때 한 번에 읽는 최대 바이트 수. */
    static final int DEFAULT_READ_CHUNK_BYTES = 4096;

    private final Socket socket;
    private final Endpoint endpoint;
    private final BiConsumer<Endpoint, Event> onEvent;
    private final int readChunkBytes;
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private final List<Event> history = new ArrayList<>();
    private final CompletableFuture<Event> authenticated = new CompletableFuture<>();
    private final Thread reader;
    private final Object bytesLock = new Object();
    /** 처리를 끝낸 받은 바이트와 소켓에 쓴 바이트의 누계. 상대가 CLOSE 같은 프레임을 처리했는지 API로 볼 수 없어서 바이트 수로 가늠한다. */
    private long receivedBytes;

    private long writtenBytes;
    /** 소켓에 쓴 프레임의 type을 쓴 순서대로 적어 둔다. 전선에 나간 프레임의 순서를 본다. */
    private final List<Integer> writtenFrameTypes = new ArrayList<>();
    /** 읽기 스레드가 예기치 못한 예외로 멈췄을 때의 원인. 기다리는 쪽 실패 메시지에 붙인다. */
    private volatile Throwable readerFailure;

    Connection(Socket socket, Endpoint endpoint, String name, int readChunkBytes, BiConsumer<Endpoint, Event> onEvent) {
        this.socket = socket;
        this.endpoint = endpoint;
        this.onEvent = onEvent;
        this.readChunkBytes = readChunkBytes;
        this.reader = new Thread(this::readLoop, name);
        this.reader.setDaemon(true);
    }

    /** 클라이언트가 연결을 열자마자 쌓은 서문과 HELLO를 쓰고 읽기 스레드를 시작한다. 시작 전에 낸 요청은 core가 미뤘다가 보낸다. */
    void start() {
        withEndpoint(endpoint -> {});
        reader.start();
    }

    /** {@link Endpoint}에 대한 동작을 잠금 안에서 실행하고, 쌓인 메시지를 소켓에 쓴다. */
    synchronized void withEndpoint(Consumer<Endpoint> action) {
        action.accept(endpoint);
        flush();
    }

    /** 받은 바이트를 {@code Endpoint}에 넣고 처리한 뒤 바이트 수에 더한다. 이벤트는 그 뒤에 내보내므로, 이벤트를 본 시점의 수에는 그 바이트가 이미 들어 있다. */
    private synchronized void receiveChunk(byte[] chunk) {
        endpoint.receive(chunk);
        synchronized (bytesLock) {
            receivedBytes += chunk.length;
            bytesLock.notifyAll();
        }
        flush();
    }

    /** 지금까지 {@code Endpoint}가 처리를 끝낸 받은 바이트 수. */
    long receivedBytes() {
        synchronized (bytesLock) {
            return receivedBytes;
        }
    }

    /** 지금까지 소켓에 쓴 바이트 수. */
    long writtenBytes() {
        synchronized (bytesLock) {
            return writtenBytes;
        }
    }

    /** 지금까지 소켓에 쓴 프레임의 type을 쓴 순서대로 돌려준다. 프레임 하나를 메시지 하나로 쓴다. 연결 서문은 type이 없는 8바이트라 서문 뒤의 프레임만 본다. */
    List<Integer> writtenFrameTypes() {
        synchronized (bytesLock) {
            return new ArrayList<>(writtenFrameTypes);
        }
    }

    private static int frameType(byte[] frame) {
        return ((frame[4] & 0xff) << 8) | (frame[5] & 0xff);
    }

    /** 처리를 끝낸 받은 바이트가 {@code atLeast}에 이를 때까지 기다린다. 이르지 못하면 실패한다. */
    void awaitReceivedBytes(long atLeast) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        synchronized (bytesLock) {
            while (receivedBytes < atLeast) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new AssertionError(
                            "받은 바이트가 " + TIMEOUT_MS + "ms 안에 " + atLeast + "에 이르지 못했다: " + receivedBytes,
                            readerFailure);
                }
                bytesLock.wait(remainingMs);
            }
        }
    }

    /**
     * 지정한 종류의 다음 이벤트를 기다려 돌려준다. 이벤트는 한 번만 꺼낼 수 있다. 다른 종류의 이벤트는 건너뛰되 {@link #events()}에는 남는다.
     */
    Event awaitEvent(Event.Kind kind) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        while (true) {
            long remaining = deadline - System.nanoTime();
            Event event = remaining <= 0 ? null : queue.poll(remaining, TimeUnit.NANOSECONDS);
            if (event == null) {
                throw new AssertionError(
                        kind + " 이벤트를 " + TIMEOUT_MS + "ms 안에 받지 못했다. 받은 이벤트: " + describeEvents(), readerFailure);
            }
            if (event.kind() == kind) {
                return event;
            }
        }
    }

    /**
     * 종류와 상관없이 다음 이벤트 {@code count}개를 받은 순서대로 기다려 돌려준다. 여러 종류가 섞여 오는 흐름을 한꺼번에 비교할 때 쓴다. {@link
     * #awaitEvent}와 같은 큐에서 꺼내므로 한 연결에서 두 방식을 섞어 쓰지 않는다.
     */
    List<Event> awaitEvents(int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        List<Event> events = new ArrayList<>();
        while (events.size() < count) {
            long remaining = deadline - System.nanoTime();
            Event event = remaining <= 0 ? null : queue.poll(remaining, TimeUnit.NANOSECONDS);
            if (event == null) {
                throw new AssertionError(
                        "이벤트 " + count + "개 중 " + events.size() + "개만 " + TIMEOUT_MS + "ms 안에 받았다. 받은 이벤트: "
                                + describeEvents(),
                        readerFailure);
            }
            events.add(event);
        }
        return events;
    }

    /** 읽기 스레드가 끝나길 기다린다. 연결이 끝나지 않으면 실패한다. */
    void awaitFinished() {
        awaitThread(reader);
        if (reader.isAlive() || readerFailure != null) {
            throw new AssertionError(
                    "연결이 " + TIMEOUT_MS + "ms 안에 정상적으로 끝나지 않았다. 받은 이벤트: " + describeEvents(), readerFailure);
        }
    }

    /** 클라이언트가 {@code AUTH(ACK)}를 받아 AUTH 관문을 통과할 때까지 기다린다. 통과하지 못하면 실패한다. */
    Event awaitAuthenticated() throws InterruptedException {
        try {
            return authenticated.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new AssertionError("AUTH 관문 통과를 " + TIMEOUT_MS + "ms 안에 보지 못했다", e);
        }
    }

    /** 지금까지 받은 모든 이벤트를 받은 순서대로 돌려준다. */
    synchronized List<Event> events() {
        return new ArrayList<>(history);
    }

    /** 실패 메시지용. {@link Event}에는 {@code toString}이 없어 종류와 스트림 번호를 직접 적는다. */
    private String describeEvents() {
        StringBuilder text = new StringBuilder("[");
        for (Event event : events()) {
            text.append(text.length() > 1 ? ", " : "")
                    .append(event.kind())
                    .append("#")
                    .append(event.streamId());
        }
        return text.append("]").toString();
    }

    private void readLoop() {
        byte[] buffer = new byte[readChunkBytes];
        try {
            InputStream in = socket.getInputStream();
            boolean eof = false;
            while (!eof) {
                int n = in.read(buffer);
                if (n < 0) {
                    withEndpoint(Endpoint::disconnect);
                    eof = true;
                } else {
                    byte[] chunk = new byte[n];
                    System.arraycopy(buffer, 0, chunk, 0, n);
                    receiveChunk(chunk);
                }
                if (hasClosed()) {
                    shutdownOutput();
                }
            }
        } catch (IOException e) {
            recordingFailure(() -> withEndpoint(Endpoint::disconnect));
        } catch (RuntimeException e) {
            readerFailure = e;
        } finally {
            closeSocket();
        }
    }

    /** 읽기 스레드의 마무리 동작이 던진 예외도 {@link #readerFailure}에 남겨, 조용히 죽은 스레드가 정상 종료로 보이지 않게 한다. */
    private void recordingFailure(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            readerFailure = e;
        }
    }

    private synchronized boolean hasClosed() {
        return history.stream().anyMatch(e -> e.kind() == Event.Kind.CLOSED);
    }

    /** 쌓인 메시지를 쓰고 이벤트를 모은다. 핸들러가 낸 응답도 같은 호출에서 쓴다. */
    private void flush() {
        writeMessages();
        for (Event event : endpoint.drainEvents()) {
            if (event.kind() == Event.Kind.AUTHENTICATED) {
                // 수립 알림은 애플리케이션 이벤트 흐름과 따로 둔다. 요청·응답·종료의 순서를 보는 테스트가 수립 시점에 영향받지 않게 한다.
                authenticated.complete(event);
                continue;
            }
            history.add(event);
            queue.add(event);
            onEvent.accept(endpoint, event);
        }
        writeMessages();
    }

    private void writeMessages() {
        try {
            OutputStream out = socket.getOutputStream();
            for (OutboundMessage message : endpoint.drainMessages()) {
                if (message.kind() == OutboundMessage.Kind.BINARY) {
                    out.write(message.bytes());
                    synchronized (bytesLock) {
                        writtenBytes += message.bytes().length;
                        writtenFrameTypes.add(frameType(message.bytes()));
                    }
                }
            }
            out.flush();
        } catch (IOException e) {
            // 상대가 먼저 끊었거나 우리가 이미 닫은 연결에 쓴 것이다. 정상 종료 경로에서도 일어나므로 실패로 보지 않는다.
            // 정말 깨졌다면 읽기 쪽 끊김이나 기다리는 쪽의 타임아웃으로 드러난다.
        }
    }

    private void shutdownOutput() {
        try {
            if (!socket.isOutputShutdown()) {
                socket.shutdownOutput();
            }
        } catch (IOException e) {
            // 이미 끊긴 연결이다.
        }
    }

    private void closeSocket() {
        try {
            socket.close();
        } catch (IOException e) {
            // 이미 닫힌 소켓이다.
        }
    }

    /**
     * 두 연결의 잠금을 함께 잡은 채 각자의 동작을 실행하고 메시지를 쓴다. 두 동작이 모두 끝나기 전에는 어느 쪽 읽기 스레드도 상대가 보낸 바이트를 처리하지 못하므로,
     * 양쪽이 서로 보낸 것을 보기 전에 각자 보내는 상황을 매번 만든다.
     */
    static void withBoth(Connection first, Consumer<Endpoint> onFirst, Connection second, Consumer<Endpoint> onSecond) {
        synchronized (first) {
            synchronized (second) {
                first.withEndpoint(onFirst);
                second.withEndpoint(onSecond);
            }
        }
    }

    /** 받은 요청마다 응답하는 이벤트 핸들러. 응답 {@code status}는 요청 {@code type}에서 {@code statusForType}으로 정한다. */
    static BiConsumer<Endpoint, Event> responding(IntUnaryOperator statusForType) {
        return (endpoint, event) -> {
            if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                endpoint.respond(event.streamId(), statusForType.applyAsInt(event.type()), Fixtures.payload("ok"));
            }
        };
    }

    /** 이 연결 하나를 끊는다. 다른 연결에는 영향이 없다. */
    @Override
    public void close() {
        closeSocket();
        awaitThread(reader);
    }

    /** 스레드가 끝나길 상한 시간까지 기다린다. 인터럽트되면 플래그를 되돌려 둔다. */
    static void awaitThread(Thread thread) {
        try {
            thread.join(TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
