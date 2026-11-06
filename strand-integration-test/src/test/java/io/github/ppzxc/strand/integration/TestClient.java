package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;

/** 서버에 TCP로 연결하는 클라이언트 {@link Endpoint}. */
final class TestClient implements AutoCloseable {

    private final Connection connection;

    private TestClient(Connection connection) {
        this.connection = connection;
    }

    static TestClient connect(int port) throws IOException {
        Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
        Connection connection = new Connection(
                socket, Endpoint.client(EndpointConfig.defaults()), "test-client", (endpoint, event) -> {});
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
