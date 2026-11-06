package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/** NIO {@link SocketChannel}과 {@link ByteBuffer}로 바이트를 보내고 한 번 읽는다. 사용법: {@code NioTcpExample host port} */
public final class NioTcpExample {

    private NioTcpExample() {}

    public static void main(String[] args) throws IOException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9000;
        System.out.println("strand core " + StrandVersion.get());
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port))) {
            // 여기서 core가 인코딩한 바이트를 보낸다. 규격 구현 전이라 자리표시 바이트를 보낸다.
            ByteBuffer out = ByteBuffer.wrap(new byte[] {0x01});
            while (out.hasRemaining()) {
                channel.write(out);
            }
            ByteBuffer in = ByteBuffer.allocate(1024);
            int n = channel.read(in);
            System.out.println("수신 " + n + " 바이트");
        }
    }
}
