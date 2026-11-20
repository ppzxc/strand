package io.github.ppzxc.strand.integration.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/** 실제 Netty NIO 채널 하나와 loopback 상대 소켓, 열린 core 엔드포인트, 구동 객체. 송신 버퍼를 작게 잡아 쓰기 막힘을 만든다. */
final class TcpFixture implements AutoCloseable {
    final NioEventLoopGroup group = new NioEventLoopGroup(1);
    final ServerSocket listener;
    final BoundaryChannel channel;
    final Socket peer;
    final Endpoint endpoint = OpenEndpoint.server();
    final TcpOutputDriver driver;

    TcpFixture() throws Exception {
        listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        listener.setReceiveBufferSize(4096);
        channel = (BoundaryChannel) new Bootstrap()
                .group(group)
                .channel(BoundaryChannel.class)
                .option(ChannelOption.SO_SNDBUF, 4096)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {}
                })
                .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort()))
                .sync()
                .channel();
        peer = listener.accept();
        peer.setSoTimeout(10_000);
        driver = new TcpOutputDriver(channel, endpoint);
    }

    <T> T onLoop(Callable<T> task) throws Exception {
        return channel.eventLoop().submit(task).sync().getNow();
    }

    /** 소켓이 더 받지 못할 때까지 0바이트를 직접 써서 채운다. 쓴 바이트 수를 돌려준다. */
    int saturate() throws Exception {
        return onLoop(() -> {
            ByteBuffer chunk = ByteBuffer.allocate(65536);
            int total = 0;
            for (int i = 0; i < 4096; i++) {
                chunk.clear();
                int n = channel.writeAttempt(chunk);
                if (n == 0) return total;
                total += n;
            }
            throw new IllegalStateException("socket did not saturate");
        });
    }

    byte[] read(int count) throws IOException {
        byte[] bytes = new byte[count];
        new DataInputStream(peer.getInputStream()).readFully(bytes);
        return bytes;
    }

    /** 상대가 EOF를 볼 때까지 읽는다. */
    byte[] readToEnd() throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int n;
        while ((n = peer.getInputStream().read(chunk)) >= 0) out.write(chunk, 0, n);
        return out.toByteArray();
    }

    /** 이벤트 루프가 닫힘 정리를 모두 처리할 때까지 기다린다. */
    void closeAndSettle() throws Exception {
        channel.close().sync();
        channel.closeFuture().sync();
        onLoop(() -> null);
    }

    @Override
    public void close() throws Exception {
        try {
            channel.close().sync();
        } finally {
            try {
                peer.close();
                listener.close();
            } finally {
                group.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS)
                        .sync();
            }
        }
    }
}
