package io.github.ppzxc.strand.integration.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class NettyOutboundBoundaryTest {
    @Test
    void channelReportsSocketProgressBeforeReleasingOutboundBuffer() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            ProgressChannel channel = (ProgressChannel) new Bootstrap()
                    .group(group)
                    .channel(ProgressChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {}
                    })
                    .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort()))
                    .sync()
                    .channel();
            try (Socket peer = listener.accept()) {
                ByteBuf frame = Unpooled.wrappedBuffer(new byte[] {1, 2, 3});
                channel.writeAndFlush(frame).sync();
                byte[] received = new byte[3];
                new java.io.DataInputStream(peer.getInputStream()).readFully(received);
                assertThat(received).containsExactly(1, 2, 3);
                assertThat(channel.progress()).isEqualTo(Arrays.asList(3));
                assertThat(channel.bufferReferenceCountsAtReport()).isEqualTo(Arrays.asList(1));
                assertThat(frame.refCnt()).isZero();
            } finally {
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void withdrawsFrameBeforeFirstWriteAfterSocketBackpressure() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            listener.setReceiveBufferSize(4096);
            ProgressChannel channel = (ProgressChannel) new Bootstrap()
                    .group(group)
                    .channel(ProgressChannel.class)
                    .option(io.netty.channel.ChannelOption.SO_SNDBUF, 4096)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {}
                    })
                    .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort()))
                    .sync()
                    .channel();
            try (Socket peer = listener.accept()) {
                peer.setSoTimeout(10000);
                byte[] filler = new byte[1024 * 1024];
                ByteBuf prefix = Unpooled.directBuffer(filler.length).writeBytes(filler);
                ByteBuf withdrawn = Unpooled.directBuffer(1).writeByte(42);
                ByteBuf sentinel = Unpooled.directBuffer(1).writeByte(99);
                channel.writeAndFlush(prefix);
                channel.writeAndFlush(withdrawn);
                assertThat(channel.eventLoop()
                                .submit(channel::zeroWrites)
                                .sync()
                                .getNow())
                        .isPositive();
                channel.eventLoop().submit(() -> channel.withdraw(withdrawn)).sync();
                channel.writeAndFlush(sentinel);
                byte[] actual = new byte[filler.length];
                new java.io.DataInputStream(peer.getInputStream()).readFully(actual);
                assertThat(actual).containsExactly(filler);
                assertThat(peer.getInputStream().read()).isEqualTo(99);
                assertThat(withdrawn.refCnt()).isZero();
            } finally {
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void partiallyWrittenFrameFinishesBeforeFollowingFrame() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            listener.setReceiveBufferSize(4096);
            ProgressChannel channel = (ProgressChannel) new Bootstrap()
                    .group(group)
                    .channel(ProgressChannel.class)
                    .option(io.netty.channel.ChannelOption.SO_SNDBUF, 4096)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {}
                    })
                    .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort()))
                    .sync()
                    .channel();
            try (Socket peer = listener.accept()) {
                peer.setSoTimeout(10000);
                byte[] payload = new byte[1024 * 1024];
                Arrays.fill(payload, (byte) 7);
                ByteBuf frame = Unpooled.directBuffer(payload.length).writeBytes(payload);
                channel.writeAndFlush(frame);
                assertThat(channel.eventLoop()
                                .submit(channel::zeroWrites)
                                .sync()
                                .getNow())
                        .isPositive();
                assertThat(channel.eventLoop().submit(channel::progress).sync().getNow())
                        .isNotEmpty();
                channel.eventLoop().submit(() -> channel.withdraw(frame)).sync();
                channel.writeAndFlush(Unpooled.directBuffer(1).writeByte(99));
                byte[] actual = new byte[payload.length];
                new java.io.DataInputStream(peer.getInputStream()).readFully(actual);
                assertThat(actual).containsExactly(payload);
                assertThat(peer.getInputStream().read()).isEqualTo(99);
                assertThat(frame.refCnt()).isZero();
            } finally {
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    @Test
    void closingBackpressuredChannelReleasesPendingBuffers() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            listener.setReceiveBufferSize(4096);
            ProgressChannel channel = (ProgressChannel) new Bootstrap()
                    .group(group)
                    .channel(ProgressChannel.class)
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
                ByteBuf pending = Unpooled.directBuffer(1024 * 1024).writeZero(1024 * 1024);
                channel.writeAndFlush(pending);
                assertThat(channel.eventLoop()
                                .submit(channel::zeroWrites)
                                .sync()
                                .getNow())
                        .isPositive();
                channel.close().sync();
                channel.eventLoop().submit(() -> {}).sync();
                assertThat(pending.refCnt()).isZero();
                assertThat(channel.eventLoop()
                                .submit(channel::retainedCount)
                                .sync()
                                .getNow())
                        .isZero();
            } finally {
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }
}
