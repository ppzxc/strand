package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

/**
 * 포트 0에 바인딩한 TCP 서버. 들어온 연결마다 서버 쪽 {@link Peer}를 만든다. 수락 스레드는 연결을 받아 {@link Peer}에 넘길 뿐이고, 연결을 돌리는 일은
 * {@link io.github.ppzxc.strand.nio.internal.ConnectionDriver}가 한다.
 */
final class TcpServer implements AutoCloseable {

    private final ServerSocketChannel serverChannel;
    private final IntUnaryOperator statusForType;
    private final Function<SocketChannel, Passage> passageFactory;
    private final List<Peer> peers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Peer> accepted = new LinkedBlockingQueue<>();
    private final Thread acceptor;

    private TcpServer(
            ServerSocketChannel serverChannel,
            IntUnaryOperator statusForType,
            Function<SocketChannel, Passage> passageFactory) {
        this.serverChannel = serverChannel;
        this.statusForType = statusForType;
        this.passageFactory = passageFactory;
        this.acceptor = new Thread(this::acceptLoop, "integration-accept");
        this.acceptor.setDaemon(true);
    }

    /** 요청을 받아도 응답하지 않는 서버. */
    static TcpServer start() throws IOException {
        return start(null);
    }

    /** 받은 요청마다 응답하는 서버. 응답 {@code status}는 요청 {@code type}에서 {@code statusForType}으로 정한다({@code null}이면 응답하지 않는다). */
    static TcpServer start(IntUnaryOperator statusForType) throws IOException {
        return start(statusForType, channel -> Peer.tcp(channel, TcpPassage.DEFAULT_READ_BUFFER_BYTES));
    }

    /** {@link #start(IntUnaryOperator)}와 같되, 채널에서 통로를 만드는 방법을 정한다. */
    static TcpServer start(IntUnaryOperator statusForType, Function<SocketChannel, Passage> passageFactory)
            throws IOException {
        ServerSocketChannel channel = ServerSocketChannel.open();
        channel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50);
        TcpServer server = new TcpServer(channel, statusForType, passageFactory);
        server.acceptor.start();
        return server;
    }

    int port() throws IOException {
        return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
    }

    /** 아직 넘겨주지 않은 다음 연결의 서버 쪽을 들어온 순서대로 기다려 돌려준다. */
    Peer awaitPeer() throws InterruptedException {
        Peer peer = accepted.poll(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (peer == null) {
            throw new AssertionError("서버에 연결이 " + Fixtures.TIMEOUT_MS + "ms 안에 들어오지 않았다");
        }
        return peer;
    }

    private void acceptLoop() {
        try {
            while (serverChannel.isOpen()) {
                SocketChannel channel = serverChannel.accept();
                Peer peer = Peer.server(passageFactory.apply(channel), statusForType);
                peers.add(peer);
                accepted.add(peer);
            }
        } catch (IOException e) {
            // 서버 채널을 닫으면 accept가 끝난다.
        }
    }

    /** 수락을 멈추고 수락 스레드가 끝난 뒤에 모든 연결을 닫는다. 그래야 마지막으로 받은 연결이 새지 않는다. */
    @Override
    public void close() throws IOException {
        serverChannel.close();
        try {
            acceptor.join(Fixtures.TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (Peer peer : peers) {
            peer.close();
        }
    }
}
