package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/** 루프백으로 맺은 TCP 연결 한 쌍. 소켓 버퍼를 작게 줄여 부분 쓰기와 백프레셔를 만들 수 있다. */
final class TcpLoopback {

    final SocketChannel client;
    final SocketChannel accepted;

    private TcpLoopback(SocketChannel client, SocketChannel accepted) {
        this.client = client;
        this.accepted = accepted;
    }

    static TcpLoopback open() throws IOException {
        return open(0);
    }

    /**
     * 연결 한 쌍을 맺는다.
     *
     * @param bufferBytes 양쪽 소켓의 송신·수신 버퍼 크기 요청(0이면 기본값). 맺기 전에 정해야 수신 창에 반영된다
     */
    static TcpLoopback open(int bufferBytes) throws IOException {
        try (ServerSocketChannel server = ServerSocketChannel.open()) {
            if (bufferBytes > 0) {
                server.setOption(StandardSocketOptions.SO_RCVBUF, bufferBytes);
            }
            server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            SocketChannel client = SocketChannel.open();
            if (bufferBytes > 0) {
                client.setOption(StandardSocketOptions.SO_RCVBUF, bufferBytes);
                client.setOption(StandardSocketOptions.SO_SNDBUF, bufferBytes);
            }
            client.connect(server.getLocalAddress());
            SocketChannel accepted = server.accept();
            if (bufferBytes > 0) {
                accepted.setOption(StandardSocketOptions.SO_SNDBUF, bufferBytes);
            }
            return new TcpLoopback(client, accepted);
        }
    }
}
