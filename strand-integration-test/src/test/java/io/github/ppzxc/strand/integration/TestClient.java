package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/** 서버에 TCP로 연결하는 클라이언트 {@link Endpoint}. */
final class TestClient implements AutoCloseable {

    private final Peer peer;

    private TestClient(Peer peer) {
        this.peer = peer;
    }

    static TestClient connect(int port) throws IOException {
        Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
        Peer peer =
                new Peer(socket, Endpoint.client(EndpointConfig.defaults()), "test-client", (endpoint, event) -> {});
        peer.start();
        return new TestClient(peer);
    }

    void request(int type, byte[] payload) {
        peer.act(endpoint -> endpoint.request(type, payload));
    }

    /** 애플리케이션이 연결을 닫는다(규격 3.5절). */
    void closeConnection(int errorCode) {
        peer.act(endpoint -> endpoint.close(errorCode));
    }

    Peer peer() {
        return peer;
    }

    Event awaitEvent(Event.Kind kind) throws InterruptedException {
        return peer.awaitEvent(kind);
    }

    @Override
    public void close() {
        peer.close();
    }
}
