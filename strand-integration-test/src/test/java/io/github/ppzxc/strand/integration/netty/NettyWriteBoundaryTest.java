package io.github.ppzxc.strand.integration.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class NettyWriteBoundaryTest {
    @Test
    void actualSocketWriteReportsBytesBeforeFrameIsComplete() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            BoundaryChannel channel = (BoundaryChannel) new Bootstrap()
                    .group(group)
                    .channel(BoundaryChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {}
                    })
                    .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort()))
                    .sync()
                    .channel();
            try (Socket peer = listener.accept()) {
                byte[] payload = new byte[] {1, 2, 3, 4};
                int written = channel.eventLoop()
                        .submit(() -> channel.writeAttempt(ByteBuffer.wrap(payload)))
                        .sync()
                        .getNow();
                assertThat(written).isEqualTo(4);
                byte[] received = new byte[4];
                new java.io.DataInputStream(peer.getInputStream()).readFully(received);
                assertThat(received).containsExactly(payload);
            } finally {
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void nonReadingPeerEventuallyProducesActualZeroAndPartialWrites() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup(1);
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            listener.setReceiveBufferSize(4096);
            BoundaryChannel channel = (BoundaryChannel) new Bootstrap()
                    .group(group)
                    .channel(BoundaryChannel.class)
                    .option(io.netty.channel.ChannelOption.SO_SNDBUF, 4096)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {}
                    })
                    .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort()))
                    .sync()
                    .channel();
            try (Socket peer = listener.accept()) {
                assertThat(peer.isConnected()).isTrue();
                int[] progress = channel.eventLoop()
                        .submit(() -> {
                            ByteBuffer frame = ByteBuffer.allocate(1024 * 1024);
                            int partial = 0;
                            int zero = 0;
                            for (int i = 0; i < 256; i++) {
                                frame.clear();
                                int written = channel.writeAttempt(frame);
                                if (written == 0) {
                                    zero++;
                                    break;
                                }
                                if (written < frame.capacity()) {
                                    partial++;
                                }
                            }
                            return new int[] {partial, zero};
                        })
                        .sync()
                        .getNow();
                assertThat(progress[0]).isPositive();
                assertThat(progress[1]).isEqualTo(1);
            } finally {
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }
}
