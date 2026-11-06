package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/** 포트 0에 바인딩한 TCP 서버. 연결마다 서버 {@link Endpoint}와 읽기 스레드를 하나씩 둔다. */
final class TestServer implements AutoCloseable {

    private final ServerSocket serverSocket;
    private final BiConsumer<Endpoint, Event> handler;
    private final List<Peer> peers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Peer> accepted = new LinkedBlockingQueue<>();
    private final Thread acceptor;

    private TestServer(ServerSocket serverSocket, BiConsumer<Endpoint, Event> handler) {
        this.serverSocket = serverSocket;
        this.handler = handler;
        this.acceptor = new Thread(this::acceptLoop, "test-server-accept");
        this.acceptor.setDaemon(true);
    }

    /** 요청을 받아도 응답하지 않는 서버. */
    static TestServer start() throws IOException {
        return start((endpoint, event) -> {});
    }

    /** 받은 요청마다 지정한 {@code status}와 빈 본문으로 응답하는 서버. */
    static TestServer startResponding(int status) throws IOException {
        return start((endpoint, event) -> {
            if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                endpoint.respond(event.streamId(), status, "ok".getBytes(StandardCharsets.UTF_8));
            }
        });
    }

    private static TestServer start(BiConsumer<Endpoint, Event> handler) throws IOException {
        TestServer server = new TestServer(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()), handler);
        server.acceptor.start();
        return server;
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    /** 아직 넘겨주지 않은 다음 연결을 기다려 돌려준다. */
    Peer awaitConnection() throws InterruptedException {
        Peer peer = accepted.poll(Peer.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (peer == null) {
            throw new AssertionError("서버에 연결이 " + Peer.TIMEOUT_MS + "ms 안에 들어오지 않았다");
        }
        return peer;
    }

    private void acceptLoop() {
        try {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                Peer peer = new Peer(
                        socket, Endpoint.server(EndpointConfig.defaults()), "test-server-" + socket.getPort(), handler);
                peers.add(peer);
                accepted.add(peer);
                peer.start();
            }
        } catch (IOException e) {
            // 서버 소켓을 닫으면 accept가 끝난다.
        }
    }

    @Override
    public void close() throws IOException {
        serverSocket.close();
        for (Peer peer : peers) {
            peer.close();
        }
        Peer.join(acceptor);
    }
}
