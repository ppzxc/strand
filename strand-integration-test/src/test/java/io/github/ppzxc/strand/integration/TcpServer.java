package io.github.ppzxc.strand.integration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;
import org.jspecify.annotations.Nullable;

final class TcpServer implements AutoCloseable {

    private static final ConcurrentHashMap<Integer, TcpServer> BY_PORT = new ConcurrentHashMap<>();

    private final Wire.Server server;
    private final int port;
    private final List<Peer> peers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Peer> accepted = new LinkedBlockingQueue<>();

    private TcpServer(Transport transport, @Nullable IntUnaryOperator statusForType) throws Exception {
        Wire wire = transport.wire();
        this.server = wire.startServer(connection -> {
            Peer.Inbox inbox = new Peer.Inbox();
            Peer peer = new Peer(connection, null, wire.claimPassage(true), inbox);
            peers.add(peer);
            accepted.add(peer);
            return inbox.handler(statusForType);
        });
        this.port = server.port();
        BY_PORT.put(port, this);
    }

    static TcpServer start(Transport transport) throws Exception {
        return start(transport, null);
    }

    static TcpServer start(Transport transport, @Nullable IntUnaryOperator statusForType) throws Exception {
        return new TcpServer(transport, statusForType);
    }

    static @Nullable TcpServer byPort(int port) {
        return BY_PORT.get(port);
    }

    int port() throws IOException {
        return port;
    }

    Peer awaitPeer() throws InterruptedException {
        Peer peer = accepted.poll(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (peer == null) {
            throw new AssertionError("서버에 연결이 " + Fixtures.TIMEOUT_MS + "ms 안에 들어오지 않았다");
        }
        return peer;
    }

    @Nullable
    Peer pollPeer() {
        return accepted.poll();
    }

    @Override
    public void close() throws IOException {
        BY_PORT.remove(port);
        server.closeListener();
        for (Peer peer : peers) {
            peer.close();
        }
        try {
            server.shutdown().get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new AssertionError("서버가 " + Fixtures.TIMEOUT_MS + "ms 안에 종료되지 않았다", e);
        }
    }
}
