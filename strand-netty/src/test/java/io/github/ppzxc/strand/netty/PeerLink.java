package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 실제 소켓 하나의 맞은편에서 core {@link Endpoint}를 직접 굴리는 시험 상대. 읽은 바이트를 Endpoint에 먹이고 Endpoint가 내려는 출력을 소켓에 쓰며, 낸 사건을
 * {@link #events}에 쌓는다. 시험은 {@link #run}으로 Endpoint를 부르고, {@link #runDetached}로 출력을 소켓에 쓰지 않고 바이트로 받아 엔진 입력에 직접 넣을 수 있다.
 */
final class PeerLink implements AutoCloseable {
    private final Endpoint peer;
    private final Socket socket;
    private final List<Event> events = new ArrayList<>();
    private volatile boolean ended;

    PeerLink(Endpoint peer, Socket socket) {
        this.peer = peer;
        this.socket = socket;
        Thread thread = new Thread(this::readLoop, "peer-link");
        thread.setDaemon(true);
        thread.start();
    }

    private void readLoop() {
        byte[] buffer = new byte[16 * 1024];
        try {
            InputStream in = socket.getInputStream();
            for (; ; ) {
                int count = in.read(buffer);
                if (count < 0) {
                    break;
                }
                byte[] chunk = new byte[count];
                System.arraycopy(buffer, 0, chunk, 0, count);
                synchronized (this) {
                    events.addAll(EndpointFeed.receive(peer, chunk));
                    flush();
                }
            }
        } catch (IOException e) {
            // 어느 쪽이 끊겼다.
        }
        ended = true;
    }

    /** Endpoint를 부르고 그 결과의 출력을 소켓에 쓴다. */
    synchronized void run(Consumer<Endpoint> action) {
        action.accept(peer);
        flush();
    }

    /** Endpoint를 부르고 나온 출력을 소켓에 쓰지 않고 바이트로 돌려준다(엔진 입력에 직접 넣으려는 시험용). */
    synchronized byte[] runDetached(Consumer<Endpoint> action) {
        action.accept(peer);
        java.io.ByteArrayOutputStream collected = new java.io.ByteArrayOutputStream();
        for (EndpointOutput output = peer.nextOutput(); output != null; output = peer.nextOutput()) {
            collected.write(output.bytes(), 0, output.length());
            peer.outputProgress(output.id(), output.length());
        }
        events.addAll(EndpointFeed.drain(peer));
        return collected.toByteArray();
    }

    /** 소켓에 바이트를 그대로 쓴다(Endpoint를 거치지 않는 프레임). */
    synchronized void writeRaw(byte[] bytes) throws IOException {
        OutputStream out = socket.getOutputStream();
        out.write(bytes);
        out.flush();
    }

    private void flush() {
        try {
            OutputStream out = socket.getOutputStream();
            for (EndpointOutput output = peer.nextOutput(); output != null; output = peer.nextOutput()) {
                out.write(output.bytes(), 0, output.length());
                peer.outputProgress(output.id(), output.length());
            }
            out.flush();
        } catch (IOException e) {
            ended = true;
        }
        events.addAll(EndpointFeed.drain(peer));
    }

    synchronized List<Event> events() {
        return new ArrayList<>(events);
    }

    /** 상대가 받은 요청의 스트림 번호(받은 순서). */
    synchronized List<Long> requestStreams() {
        List<Long> streams = new ArrayList<>();
        events.stream()
                .filter(event -> event.kind() == Event.Kind.REQUEST_RECEIVED)
                .forEach(event -> streams.add(event.streamId()));
        return streams;
    }

    /** 상대가 받은 RESET 사건의 스트림 번호와 오류 코드(받은 순서). */
    synchronized List<long[]> resets() {
        List<long[]> resets = new ArrayList<>();
        events.stream()
                .filter(event -> event.kind() == Event.Kind.REQUEST_RESET)
                .forEach(event -> resets.add(
                        new long[] {event.streamId(), event.resetErrorCode().getAsLong()}));
        return resets;
    }

    boolean ended() {
        return ended;
    }

    /** 소켓을 닫아 엔진이 끊김을 보게 한다. */
    void disconnect() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }

    @Override
    public void close() {
        disconnect();
    }
}
