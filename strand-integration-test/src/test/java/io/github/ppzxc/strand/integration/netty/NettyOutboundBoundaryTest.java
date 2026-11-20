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
                ByteBuf frame = Unpooled.directBuffer(3).writeBytes(new byte[] {1, 2, 3});
                channel.send(frame).sync();
                byte[] received = new byte[3];
                new java.io.DataInputStream(peer.getInputStream()).readFully(received);
                assertThat(received).containsExactly(1, 2, 3);
                assertThat(channel.progress()).isEqualTo(Arrays.asList(3));
                assertThat(channel.bufferReferenceCountsAtReport()).isEqualTo(Arrays.asList(1));
                assertThat(frame.refCnt()).isZero();
                channel.eventLoop().submit(() -> channel.withdraw(frame)).sync();
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
                ByteBuf withdrawn = Unpooled.directBuffer(1).writeByte(42);
                ByteBuf sentinel = Unpooled.directBuffer(1).writeByte(99);
                int fillerSize = channel.eventLoop()
                        .submit(() -> {
                            java.nio.ByteBuffer bytes = java.nio.ByteBuffer.allocate(65536);
                            int total = 0;
                            for (int i = 0; i < 1024; i++) {
                                bytes.clear();
                                int n = channel.writeAttempt(bytes);
                                if (n == 0) {
                                    channel.send(withdrawn);
                                    if (channel.zeroWrites() == 0
                                            || !channel.progress().isEmpty()) {
                                        throw new IllegalStateException("target frame did not encounter a zero write");
                                    }
                                    channel.withdraw(withdrawn);
                                    return total;
                                }
                                total += n;
                            }
                            throw new IllegalStateException("socket did not saturate");
                        })
                        .sync()
                        .getNow();
                assertThat(fillerSize).isPositive();
                channel.send(sentinel);
                byte[] actual = new byte[fillerSize];
                new java.io.DataInputStream(peer.getInputStream()).readFully(actual);
                assertThat(actual).containsOnly((byte) 0);
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
    void partiallyWrittenRequestFinishesBeforeFollowingReset() throws Exception {
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
                Arrays.fill(payload, 20, payload.length, (byte) 7);
                java.nio.ByteBuffer.wrap(payload)
                        .putInt(payload.length - 4)
                        .putShort((short) 0x0101)
                        .put((byte) 0)
                        .put((byte) 0)
                        .putLong(1)
                        .putInt(0);
                byte[] reset = java.nio.ByteBuffer.allocate(24)
                        .putInt(20)
                        .putShort((short) 4)
                        .put((byte) 0)
                        .put((byte) 0)
                        .putLong(1)
                        .putInt(0)
                        .putInt(4)
                        .array();
                ByteBuf frame = Unpooled.directBuffer(payload.length).writeBytes(payload);
                channel.send(frame);
                assertThat(channel.eventLoop()
                                .submit(channel::zeroWrites)
                                .sync()
                                .getNow())
                        .isPositive();
                assertThat(channel.eventLoop().submit(channel::progress).sync().getNow())
                        .isNotEmpty();
                channel.eventLoop().submit(() -> channel.withdraw(frame)).sync();
                channel.send(Unpooled.directBuffer(reset.length).writeBytes(reset));
                byte[] actual = new byte[payload.length];
                new java.io.DataInputStream(peer.getInputStream()).readFully(actual);
                assertThat(actual).containsExactly(payload);
                byte[] actualReset = new byte[reset.length];
                new java.io.DataInputStream(peer.getInputStream()).readFully(actualReset);
                assertThat(actualReset).containsExactly(reset);
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
                channel.send(pending);
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

    @Test
    void rejectedEventLoopSubmissionReleasesFrame() throws Exception {
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
                assertThat(peer.isConnected()).isTrue();
                channel.close().sync();
                group.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.SECONDS)
                        .sync();
                ByteBuf frame = Unpooled.directBuffer(1).writeByte(1);
                io.netty.channel.ChannelFuture result = channel.send(frame);
                assertThat(result.awaitUninterruptibly().isSuccess()).isFalse();
                assertThat(frame.refCnt()).isZero();
            }
        } finally {
            group.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.SECONDS)
                    .sync();
        }
    }
}
