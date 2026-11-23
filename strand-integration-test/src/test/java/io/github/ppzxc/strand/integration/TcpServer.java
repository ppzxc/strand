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

/**
 * 포트 0에 바인딩한 제품 서버(nio 또는 netty). 연결 인증을 통과한 연결마다 서버 쪽 {@link Peer}를 만든다. 연결을 받고 돌리는 일은 모두 제품 엔진이 한다.
 */
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

    /** 요청을 받아도 응답하지 않는 서버. */
    static TcpServer start(Transport transport) throws Exception {
        return start(transport, null);
    }

    /** 받은 요청마다 응답하는 서버. 응답 {@code status}는 요청 {@code type}에서 {@code statusForType}으로 정한다({@code null}이면 응답하지 않는다). */
    static TcpServer start(Transport transport, @Nullable IntUnaryOperator statusForType) throws Exception {
        return new TcpServer(transport, statusForType);
    }

    /** 이 포트에서 도는 서버. 날것의 상대가 연 포트면 null이다. */
    static @Nullable TcpServer byPort(int port) {
        return BY_PORT.get(port);
    }

    int port() throws IOException {
        return port;
    }

    /** 인증을 통과해 핸들러가 준비된 다음 연결의 서버 쪽을 들어온 순서대로 기다려 돌려준다. */
    Peer awaitPeer() throws InterruptedException {
        Peer peer = accepted.poll(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (peer == null) {
            throw new AssertionError("서버에 연결이 " + Fixtures.TIMEOUT_MS + "ms 안에 들어오지 않았다");
        }
        return peer;
    }

    /** 기다리지 않고, 아직 넘겨주지 않은 다음 서버 쪽 연결을 돌려준다. 없으면 null이다. */
    @Nullable
    Peer pollPeer() {
        return accepted.poll();
    }

    /** 수락을 멈추고 모든 연결을 끊은 뒤 서버를 종료한다. */
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
