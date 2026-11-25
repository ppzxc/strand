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
        }
        ended = true;
    }

    synchronized void run(Consumer<Endpoint> action) {
        action.accept(peer);
        flush();
    }

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

    synchronized List<Long> requestStreams() {
        List<Long> streams = new ArrayList<>();
        events.stream()
                .filter(event -> event.kind() == Event.Kind.REQUEST_RECEIVED)
                .forEach(event -> streams.add(event.streamId()));
        return streams;
    }

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

    void disconnect() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() {
        disconnect();
    }
}
