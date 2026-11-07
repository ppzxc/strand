package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.IntUnaryOperator;

/** 서버에 TCP로 연결하는 클라이언트 {@link Endpoint}. */
final class TestClient implements AutoCloseable {

    private final Connection connection;

    private TestClient(Connection connection) {
        this.connection = connection;
    }

    static TestClient connect(int port) throws IOException {
        return connect(port, Connection.DEFAULT_READ_CHUNK_BYTES);
    }

    /** 서버가 낸 요청마다 응답하는 클라이언트. 응답 {@code status}는 요청 {@code type}에서 {@code statusForType}으로 정한다. */
    static TestClient connectResponding(int port, IntUnaryOperator statusForType) throws IOException {
        return connect(port, Connection.DEFAULT_READ_CHUNK_BYTES, Connection.responding(statusForType));
    }

    /** 한 번에 {@code readChunkBytes}바이트까지만 읽어 {@code Endpoint}에 넘기는 클라이언트. 프레임이 쪼개져 오는 경우를 만든다. */
    static TestClient connect(int port, int readChunkBytes) throws IOException {
        return connect(port, readChunkBytes, (endpoint, event) -> {});
    }

    private static TestClient connect(int port, int readChunkBytes, BiConsumer<Endpoint, Event> handler)
            throws IOException {
        Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
        Connection connection = new Connection(
                socket, Endpoint.client(EndpointConfig.defaults()), "test-client", readChunkBytes, handler);
        connection.start();
        return new TestClient(connection);
    }

    void request(int type, byte[] payload) {
        connection.withEndpoint(endpoint -> endpoint.request(type, payload));
    }

    /** 애플리케이션이 연결을 닫는다(규격 3.5절). */
    void closeConnection(int errorCode) {
        connection.withEndpoint(endpoint -> endpoint.close(errorCode));
    }

    Event awaitEvent(Event.Kind kind) throws InterruptedException {
        return connection.awaitEvent(kind);
    }

    void awaitFinished() {
        connection.awaitFinished();
    }

    List<Event> events() {
        return connection.events();
    }

    @Override
    public void close() {
        connection.close();
    }
}
