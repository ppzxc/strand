package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/**
 * NIO {@link SocketChannel}과 {@link ByteBuffer} 위에서 클라이언트 {@link Endpoint}를 돌린다. 논블로킹 {@code Selector} 대신 단순한
 * 블로킹 채널을 쓰지만, 부분 쓰기와 임의 위치에서 잘린 읽기를 core가 받아낸다는 점은 같다. 사용법: {@code NioTcpExample [host]
 * [port]} (상대는 {@link ServerExample})
 */
public final class NioTcpExample {

    private NioTcpExample() {}

    public static void main(String[] args) throws IOException {
        String host = ExampleSupport.arg(args, 0, "localhost");
        int port = Integer.parseInt(ExampleSupport.arg(args, 1, "9000"));
        System.out.println("strand core " + StrandVersion.get());
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port))) {
            Endpoint endpoint = Endpoint.client(EndpointConfig.defaults());
            endpoint.request(ExampleSupport.DEMO_REQUEST_TYPE, "hello".getBytes(StandardCharsets.UTF_8));
            endpoint.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
            flush(endpoint, channel);

            ByteBuffer in = ByteBuffer.allocate(4096);
            boolean resultReceived = false;
            boolean closed = false;
            while (!closed) {
                in.clear();
                int n = channel.read(in);
                if (n < 0) {
                    endpoint.disconnect();
                } else {
                    in.flip();
                    byte[] chunk = new byte[in.remaining()];
                    in.get(chunk);
                    endpoint.receive(chunk);
                }
                for (Event event : endpoint.drainEvents()) {
                    System.out.println("[클라이언트] " + ExampleSupport.describe(event));
                    resultReceived |= event.kind() == Event.Kind.RESULT;
                    closed |= event.kind() == Event.Kind.CLOSED;
                }
                if (resultReceived && !closed) {
                    endpoint.close(ExampleSupport.NO_ERROR);
                }
                flush(endpoint, channel);
            }
        }
    }

    /** 채널은 한 번에 일부만 쓸 수 있으므로 남은 바이트가 없을 때까지 쓴다. */
    private static void flush(Endpoint endpoint, SocketChannel channel) throws IOException {
        for (OutboundMessage message : endpoint.drainMessages()) {
            if (message.kind() != OutboundMessage.Kind.BINARY) {
                continue;
            }
            ByteBuffer out = ByteBuffer.wrap(message.bytes());
            while (out.hasRemaining()) {
                channel.write(out);
            }
        }
    }
}
