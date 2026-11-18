package io.github.ppzxc.strand.integration.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.DataInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/** Experiment only: drives the JDK engine and a Netty NIO channel over a loopback socket. */
final class TlsProbe {
    static final class Result {
        final int plaintextConsumed;
        final int ciphertextWritten;
        final byte[] received;

        Result(int plaintextConsumed, int ciphertextWritten, byte[] received) {
            this.plaintextConsumed = plaintextConsumed;
            this.ciphertextWritten = ciphertextWritten;
            this.received = received;
        }
    }

    static Result exchange(byte[] message) throws Exception {
        return exchange(message, false, null);
    }

    static Result exchangeOutbound(byte[] message) throws Exception {
        return exchange(message, true, null);
    }

    static Result exchangePartial(byte[] message, byte[] reset) throws Exception {
        return exchange(message, true, reset);
    }

    private static Result exchange(byte[] message, boolean outbound, byte[] reset) throws Exception {
        SSLContext context = context();
        ExecutorService serverThread = Executors.newSingleThreadExecutor();
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (SSLServerSocket server = (SSLServerSocket)
                context.getServerSocketFactory().createServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            server.setEnabledProtocols(new String[] {"TLSv1.2"});
            Future<byte[]> received = serverThread.submit(() -> {
                try (SSLSocket peer = (SSLSocket) server.accept()) {
                    peer.setSoTimeout(10000);
                    byte[] bytes = new byte[message.length + (reset == null ? 0 : reset.length)];
                    new DataInputStream(peer.getInputStream()).readFully(bytes);
                    return bytes;
                }
            });
            BoundaryChannel channel = (BoundaryChannel) new Bootstrap()
                    .group(group)
                    .channel(outbound ? ProgressChannel.class : BoundaryChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {}
                    })
                    .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort()))
                    .sync()
                    .channel();
            try {
                java.util.concurrent.atomic.AtomicReference<io.netty.channel.ChannelFuture> output =
                        outbound ? new java.util.concurrent.atomic.AtomicReference<>() : null;
                int[] counts = channel.eventLoop()
                        .submit(() -> drive(context, channel, message, output, reset))
                        .sync()
                        .getNow();
                if (outbound) {
                    output.get().sync();
                    counts[1] = channel.eventLoop()
                            .submit(() -> ((ProgressChannel) channel)
                                    .progress().stream()
                                            .mapToInt(Integer::intValue)
                                            .sum())
                            .sync()
                            .getNow();
                }
                return new Result(counts[0], counts[1], received.get(10, TimeUnit.SECONDS));
            } finally {
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
            serverThread.shutdownNow();
        }
    }

    private static SSLContext context() throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        char[] password = "strandtest".toCharArray();
        try (InputStream in = TlsProbe.class.getResourceAsStream("/netty/localhost.p12")) {
            keys.load(in, password);
        }
        KeyManagerFactory km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        km.init(keys, password);
        TrustManagerFactory tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tm.init(keys);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(km.getKeyManagers(), tm.getTrustManagers(), null);
        return context;
    }

    static int[] pressure() throws Exception {
        return pressure(false);
    }

    static int[] pressureOutbound() throws Exception {
        return pressure(true);
    }

    private static int[] pressure(boolean outbound) throws Exception {
        SSLContext context = context();
        ExecutorService serverThread = Executors.newSingleThreadExecutor();
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        try (SSLServerSocket server = (SSLServerSocket)
                context.getServerSocketFactory().createServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            server.setReceiveBufferSize(4096);
            server.setEnabledProtocols(new String[] {"TLSv1.2"});
            Future<?> received = serverThread.submit(() -> {
                try (SSLSocket peer = (SSLSocket) server.accept()) {
                    peer.setSoTimeout(10000);
                    peer.startHandshake();
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("release timed out");
                }
                return null;
            });
            BoundaryChannel channel = (BoundaryChannel) new Bootstrap()
                    .group(group)
                    .channel(outbound ? ProgressChannel.class : BoundaryChannel.class)
                    .option(io.netty.channel.ChannelOption.SO_SNDBUF, 4096)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {}
                    })
                    .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort()))
                    .sync()
                    .channel();
            try {
                int[] result = channel.eventLoop()
                        .submit(() -> drive(
                                context,
                                channel,
                                null,
                                outbound ? new java.util.concurrent.atomic.AtomicReference<>() : null,
                                null))
                        .sync()
                        .getNow();
                release.countDown();
                received.get(10, TimeUnit.SECONDS);
                return result;
            } finally {
                release.countDown();
                channel.close().sync();
            }
        } finally {
            group.shutdownGracefully().sync();
            serverThread.shutdownNow();
        }
    }

    private static int[] drive(
            SSLContext context,
            BoundaryChannel channel,
            byte[] message,
            java.util.concurrent.atomic.AtomicReference<io.netty.channel.ChannelFuture> output,
            byte[] reset)
            throws Exception {
        SSLEngine engine = context.createSSLEngine("localhost", 0);
        engine.setUseClientMode(true);
        engine.setEnabledProtocols(new String[] {"TLSv1.2"});
        engine.beginHandshake();
        ByteBuffer incoming = ByteBuffer.allocate(engine.getSession().getPacketBufferSize() * 2);
        ByteBuffer outgoing = ByteBuffer.allocate(engine.getSession().getPacketBufferSize() * 2);
        ByteBuffer application = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize() * 2);
        ByteBuffer empty = ByteBuffer.allocate(0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (engine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            if (System.nanoTime() > deadline) throw new IllegalStateException("TLS handshake timed out");
            switch (engine.getHandshakeStatus()) {
                case NEED_TASK:
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) task.run();
                    break;
                case NEED_WRAP:
                    outgoing.clear();
                    SSLEngineResult wrapped = engine.wrap(empty, outgoing);
                    if (wrapped.getStatus() != SSLEngineResult.Status.OK)
                        throw new IllegalStateException(wrapped.toString());
                    outgoing.flip();
                    writeFully(channel, outgoing, deadline);
                    break;
                case NEED_UNWRAP:
                    int n = channel.readAttempt(incoming);
                    if (n < 0) throw new IllegalStateException("peer closed during handshake");
                    incoming.flip();
                    SSLEngineResult unwrapped = engine.unwrap(incoming, application);
                    incoming.compact();
                    if (unwrapped.getStatus() != SSLEngineResult.Status.OK
                            && unwrapped.getStatus() != SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        throw new IllegalStateException(unwrapped.toString());
                    }
                    if (n == 0) Thread.yield();
                    break;
                default:
                    throw new IllegalStateException(engine.getHandshakeStatus().toString());
            }
        }
        if (message == null) {
            int consumed = 0;
            int partial = 0;
            ByteBuffer plaintext = ByteBuffer.allocate(16384);
            for (int frame = 0; frame < 1024; frame++) {
                plaintext.clear();
                outgoing.clear();
                SSLEngineResult result = engine.wrap(plaintext, outgoing);
                if (result.getStatus() != SSLEngineResult.Status.OK) throw new IllegalStateException(result.toString());
                consumed += result.bytesConsumed();
                outgoing.flip();
                if (output != null) {
                    ProgressChannel progressChannel = (ProgressChannel) channel;
                    int before = progressChannel.progress().stream()
                            .mapToInt(Integer::intValue)
                            .sum();
                    int length = outgoing.remaining();
                    io.netty.buffer.ByteBuf ciphertext = io.netty.buffer.Unpooled.directBuffer(length);
                    ciphertext.writeBytes(outgoing);
                    io.netty.channel.ChannelFuture write = progressChannel.send(ciphertext);
                    int after = progressChannel.progress().stream()
                            .mapToInt(Integer::intValue)
                            .sum();
                    if (after - before > 0 && after - before < length) partial++;
                    if (progressChannel.zeroWrites() > 0) return new int[] {consumed, partial, 1};
                    if (!write.isDone() || !write.isSuccess())
                        throw new IllegalStateException("unexpected pending write");
                    continue;
                }
                while (outgoing.hasRemaining()) {
                    int remaining = outgoing.remaining();
                    int n = channel.writeAttempt(outgoing);
                    if (n == 0) return new int[] {consumed, partial, 1};
                    if (n < remaining) partial++;
                }
            }
            return new int[] {consumed, partial, 0};
        }
        if (reset != null) {
            ByteBuffer request = ByteBuffer.wrap(message);
            int firstConsumed = 0;
            while (request.hasRemaining()) {
                outgoing.clear();
                SSLEngineResult result = engine.wrap(request, outgoing);
                if (result.getStatus() != SSLEngineResult.Status.OK || result.bytesConsumed() == 0)
                    throw new IllegalStateException(result.toString());
                if (firstConsumed == 0) firstConsumed = result.bytesConsumed();
                outgoing.flip();
                io.netty.buffer.ByteBuf ciphertext = io.netty.buffer.Unpooled.directBuffer(outgoing.remaining());
                ciphertext.writeBytes(outgoing);
                output.set(((ProgressChannel) channel).send(ciphertext));
            }
            outgoing.clear();
            SSLEngineResult result = engine.wrap(ByteBuffer.wrap(reset), outgoing);
            if (result.getStatus() != SSLEngineResult.Status.OK || result.bytesConsumed() != reset.length)
                throw new IllegalStateException(result.toString());
            outgoing.flip();
            io.netty.buffer.ByteBuf ciphertext = io.netty.buffer.Unpooled.directBuffer(outgoing.remaining());
            ciphertext.writeBytes(outgoing);
            output.set(((ProgressChannel) channel).send(ciphertext));
            return new int[] {firstConsumed, 0};
        }
        outgoing.clear();
        SSLEngineResult result = engine.wrap(ByteBuffer.wrap(message), outgoing);
        if (result.getStatus() != SSLEngineResult.Status.OK) throw new IllegalStateException(result.toString());
        outgoing.flip();
        if (output != null) {
            io.netty.buffer.ByteBuf ciphertext = io.netty.buffer.Unpooled.directBuffer(outgoing.remaining());
            ciphertext.writeBytes(outgoing);
            output.set(((ProgressChannel) channel).send(ciphertext));
            return new int[] {result.bytesConsumed(), 0};
        }
        int written = writeFully(channel, outgoing, deadline);
        return new int[] {result.bytesConsumed(), written};
    }

    private static int writeFully(BoundaryChannel channel, ByteBuffer buffer, long deadline) throws Exception {
        int count = 0;
        while (buffer.hasRemaining()) {
            if (System.nanoTime() > deadline) throw new IllegalStateException("socket write timed out");
            int n = channel.writeAttempt(buffer);
            count += n;
            if (n == 0) Thread.yield();
        }
        return count;
    }
}
