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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 소켓 하나에 {@link Endpoint} 하나를 붙여 읽기 스레드로 돌린다. core가 쌓은 메시지는 매번 소켓에 쓰고, 이벤트는 모아 둔다. {@code Endpoint}는
 * 스레드 안전하지 않으므로 모든 접근을 이 객체의 잠금 안에서 한다.
 */
final class Peer implements AutoCloseable {

    /** 이벤트나 종료를 기다리는 상한. 넘기면 실패시켜 테스트가 멈추지 않게 한다. */
    static final long TIMEOUT_MS = 5000L;

    private static final int READ_BUFFER_BYTES = 4096;

    private final Socket socket;
    private final Endpoint endpoint;
    private final BiConsumer<Endpoint, Event> onEvent;
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private final List<Event> history = new ArrayList<>();
    private final Thread reader;

    Peer(Socket socket, Endpoint endpoint, String name, BiConsumer<Endpoint, Event> onEvent) {
        this.socket = socket;
        this.endpoint = endpoint;
        this.onEvent = onEvent;
        this.reader = new Thread(this::readLoop, name);
        this.reader.setDaemon(true);
    }

    /** 읽기 스레드를 시작한다. 시작 전에 낸 요청은 core가 미뤘다가 보낸다. */
    void start() {
        reader.start();
    }

    /** {@link Endpoint}에 대한 동작을 잠금 안에서 실행하고, 쌓인 메시지를 소켓에 쓴다. */
    synchronized void act(Consumer<Endpoint> action) {
        action.accept(endpoint);
        flush();
    }

    /** 지정한 종류의 다음 이벤트를 기다려 돌려준다. 다른 종류의 이벤트는 건너뛰되 {@link #events()}에는 남는다. */
    Event awaitEvent(Event.Kind kind) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        while (true) {
            long remaining = deadline - System.nanoTime();
            Event event = remaining <= 0 ? null : queue.poll(remaining, TimeUnit.NANOSECONDS);
            if (event == null) {
                throw new AssertionError(kind + " 이벤트를 " + TIMEOUT_MS + "ms 안에 받지 못했다. 받은 이벤트: " + describeEvents());
            }
            if (event.kind() == kind) {
                return event;
            }
        }
    }

    /** 읽기 스레드가 끝나길 기다린다. 연결이 끝나지 않으면 실패한다. */
    void awaitFinished() {
        join(reader);
        if (reader.isAlive()) {
            throw new AssertionError("연결이 " + TIMEOUT_MS + "ms 안에 끝나지 않았다. 받은 이벤트: " + describeEvents());
        }
    }

    /** core는 전송 계층을 닫으라는 메시지를 따로 내지 않는다. {@code closed} 이벤트가 나오면 연결이 끝난 것이다(규격 3.5절). */
    private synchronized boolean isClosed() {
        return history.stream().anyMatch(e -> e.kind() == Event.Kind.CLOSED);
    }

    boolean isSocketClosed() {
        return socket.isClosed();
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

    /** 지금까지 받은 모든 이벤트를 받은 순서대로 돌려준다. */
    synchronized List<Event> events() {
        return new ArrayList<>(history);
    }

    private void readLoop() {
        byte[] buffer = new byte[READ_BUFFER_BYTES];
        try {
            InputStream in = socket.getInputStream();
            boolean finished = false;
            while (!finished) {
                int n = in.read(buffer);
                if (n < 0) {
                    act(Endpoint::disconnect);
                    finished = true;
                } else {
                    byte[] chunk = new byte[n];
                    System.arraycopy(buffer, 0, chunk, 0, n);
                    act(e -> e.receive(chunk));
                    finished = isClosed();
                }
            }
        } catch (IOException e) {
            act(Endpoint::disconnect);
        } finally {
            closeSocket();
        }
    }

    /** 쌓인 메시지를 쓰고 이벤트를 모은다. 핸들러가 낸 응답도 같은 호출에서 쓴다. */
    private void flush() {
        writeMessages();
        for (Event event : endpoint.drainEvents()) {
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
                }
            }
            out.flush();
        } catch (IOException e) {
            // 쓰기 실패는 곧 읽기 쪽에서 끊김으로 드러난다.
        }
    }

    private void closeSocket() {
        try {
            socket.close();
        } catch (IOException e) {
            // 이미 닫힌 소켓이다.
        }
    }

    @Override
    public void close() {
        closeSocket();
        join(reader);
    }

    /** 스레드가 끝나길 상한 시간까지 기다린다. 인터럽트되면 플래그를 되돌려 둔다. */
    static void join(Thread thread) {
        try {
            thread.join(TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
