package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.nio.Listener;
import io.github.ppzxc.strand.nio.NioStrandServer;
import io.github.ppzxc.strand.nio.PreparedHandler;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import org.jspecify.annotations.Nullable;

/**
 * 포트 0에 바인딩한 제품 서버({@link NioStrandServer}). 연결 인증을 통과한 연결마다 서버 쪽 {@link Peer}를 만든다. 연결을 받고 돌리는 일은 모두 제품 엔진이 한다.
 */
final class TcpServer implements AutoCloseable {

    private static final ConcurrentHashMap<Integer, TcpServer> BY_PORT = new ConcurrentHashMap<>();

    private final NioStrandServer server;
    private final Listener listener;
    private final int port;
    private final List<Peer> peers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Peer> accepted = new LinkedBlockingQueue<>();
    private final AtomicInteger connectsCompleted = new AtomicInteger();
    private final AtomicInteger prepared = new AtomicInteger();

    private TcpServer(@Nullable IntUnaryOperator statusForType) throws Exception {
        this.server = io.github.ppzxc.strand.nio.internal.TestNio.serverBuilder(Peer.HANDLERS)
                .handlerFactory(connection -> {
                    Peer.Inbox inbox = new Peer.Inbox();
                    Peer peer =
                            new Peer(connection, null, PassageObservation.claim(PassageWrapping.Side.SERVER), inbox);
                    peers.add(peer);
                    accepted.add(peer);
                    prepared.incrementAndGet();
                    return PreparedHandler.of(inbox.handler(statusForType));
                })
                .build();
        this.listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .toCompletableFuture()
                .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        this.port = ((InetSocketAddress) listener.localAddress()).getPort();
        BY_PORT.put(port, this);
    }

    /** 요청을 받아도 응답하지 않는 서버. */
    static TcpServer start() throws Exception {
        return start(null);
    }

    /** 받은 요청마다 응답하는 서버. 응답 {@code status}는 요청 {@code type}에서 {@code statusForType}으로 정한다({@code null}이면 응답하지 않는다). */
    static TcpServer start(@Nullable IntUnaryOperator statusForType) throws Exception {
        return new TcpServer(statusForType);
    }

    /** 이 포트에서 도는 서버. 날것의 상대가 연 포트면 null이다. */
    static @Nullable TcpServer byPort(int port) {
        return BY_PORT.get(port);
    }

    int port() throws IOException {
        return port;
    }

    /**
     * 맺어진 클라이언트 연결의 서버 쪽 핸들러 준비가 끝나길 기다린다. 서버는 핸들러 준비 전에 온 요청을 거절한다(규격 4.5절). 연결 인증 응답은 팩터리가 끝나기 전에 나가므로 클라이언트가
     * 연결을 받은 직후에 낸 첫 요청이 거절될 수 있다. 팩터리가 낸 결과를 엔진이 받아들이면 팩터리 작업 슬롯이 돌아오므로(소유 객체의 {@code FACTORY_TASKS} 사용량이 0), 그것을 기다린 뒤 각
     * 서버 쪽 연결의 전송 스레드가 한 바퀴 돌았는지까지 확인한다.
     */
    void awaitPrepared() {
        int expected = connectsCompleted.incrementAndGet();
        Peer.awaitUntil(() -> prepared.get() >= expected, "서버가 연결 " + expected + "의 핸들러를 준비하지 못했다");
        Peer.awaitUntil(
                () -> server.resourceUsage(Resource.FACTORY_TASKS).current() == 0,
                "서버의 핸들러 팩터리 작업이 " + Fixtures.TIMEOUT_MS + "ms 안에 끝나지 않았다");
        for (Peer peer : peers) {
            peer.awaitEngineTurn();
        }
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
        listener.close();
        for (Peer peer : peers) {
            peer.close();
        }
        try {
            server.shutdown().toCompletableFuture().get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new AssertionError("서버가 " + Fixtures.TIMEOUT_MS + "ms 안에 종료되지 않았다", e);
        }
    }
}
