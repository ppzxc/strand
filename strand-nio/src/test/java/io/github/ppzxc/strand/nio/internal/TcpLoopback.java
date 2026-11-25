package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

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
