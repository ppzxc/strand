package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntUnaryOperator;

/** 포트 0에 바인딩한 TCP 서버. 연결마다 서버 {@link Endpoint}와 읽기 스레드를 하나씩 둔다. */
final class TestServer implements AutoCloseable {

    private final ServerSocket serverSocket;
    private final BiConsumer<Endpoint, Event> handler;
    private final int readChunkBytes;
    private final List<Connection> connections = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Connection> accepted = new LinkedBlockingQueue<>();
    private final Thread acceptor;

    private TestServer(ServerSocket serverSocket, int readChunkBytes, BiConsumer<Endpoint, Event> handler) {
        this.serverSocket = serverSocket;
        this.readChunkBytes = readChunkBytes;
        this.handler = handler;
        this.acceptor = new Thread(this::acceptLoop, "test-server-accept");
        this.acceptor.setDaemon(true);
    }

    /** 요청을 받아도 응답하지 않는 서버. */
    static TestServer start() throws IOException {
        return start(Connection.DEFAULT_READ_CHUNK_BYTES, (endpoint, event) -> {});
    }

    /** 받은 요청마다 응답하는 서버. 응답 {@code status}는 요청 {@code type}에서 {@code statusForType}으로 정한다. */
    static TestServer startResponding(IntUnaryOperator statusForType) throws IOException {
        return startResponding(statusForType, Connection.DEFAULT_READ_CHUNK_BYTES);
    }

    /** {@link #startResponding(IntUnaryOperator)}와 같되, 연결마다 한 번에 {@code readChunkBytes}바이트까지만 읽는다. */
    static TestServer startResponding(IntUnaryOperator statusForType, int readChunkBytes) throws IOException {
        return start(readChunkBytes, (endpoint, event) -> {
            if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                endpoint.respond(event.streamId(), statusForType.applyAsInt(event.type()), Fixtures.payload("ok"));
            }
        });
    }

    private static TestServer start(int readChunkBytes, BiConsumer<Endpoint, Event> handler) throws IOException {
        TestServer server =
                new TestServer(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()), readChunkBytes, handler);
        server.acceptor.start();
        return server;
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    /** 아직 넘겨주지 않은 다음 연결을 들어온 순서대로 기다려 돌려준다. 돌려받은 {@link Connection#close()}는 그 연결만 끊는다. */
    Connection awaitConnection() throws InterruptedException {
        Connection connection = accepted.poll(Connection.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (connection == null) {
            throw new AssertionError("서버에 연결이 " + Connection.TIMEOUT_MS + "ms 안에 들어오지 않았다");
        }
        return connection;
    }

    private void acceptLoop() {
        try {
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                Connection connection = new Connection(
                        socket,
                        Endpoint.server(EndpointConfig.defaults()),
                        "test-server-" + socket.getPort(),
                        readChunkBytes,
                        handler);
                connections.add(connection);
                accepted.add(connection);
                connection.start();
            }
        } catch (IOException e) {
            // 서버 소켓을 닫으면 accept가 끝난다.
        }
    }

    /** 수락을 멈추고 수락 스레드가 끝난 뒤에 모든 연결을 닫는다. 그래야 마지막으로 받은 연결이 새지 않는다. */
    @Override
    public void close() throws IOException {
        serverSocket.close();
        Connection.awaitThread(acceptor);
        for (Connection connection : connections) {
            connection.close();
        }
    }
}
