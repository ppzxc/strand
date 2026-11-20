package io.github.ppzxc.strand.integration.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

/** 실제 Netty NIO 채널, JDK SSLEngine 클라이언트, 실제 JDK TLS 서버 소켓. 암호문 송신 버퍼를 작게 잡아 쓰기 막힘을 만든다. */
final class TlsFixture implements AutoCloseable {
    final NioEventLoopGroup group = new NioEventLoopGroup(1);
    final ExecutorService serverThread = Executors.newSingleThreadExecutor();
    final SSLServerSocket server;
    final BoundaryChannel channel;
    final SSLSocket peer;
    final Endpoint endpoint = OpenEndpoint.server();
    final TlsOutputDriver driver;

    TlsFixture(String protocol, int reservedPackets) throws Exception {
        SSLContext context = TlsProbe.context();
        server = (SSLServerSocket)
                context.getServerSocketFactory().createServerSocket(0, 1, InetAddress.getLoopbackAddress());
        server.setReceiveBufferSize(4096);
        server.setEnabledProtocols(new String[] {protocol});
        Future<SSLSocket> accepted = serverThread.submit(() -> {
            SSLSocket socket = (SSLSocket) server.accept();
            socket.setSoTimeout(10_000);
            socket.startHandshake();
            return socket;
        });
        channel = (BoundaryChannel) new Bootstrap()
                .group(group)
                .channel(BoundaryChannel.class)
                .option(ChannelOption.SO_SNDBUF, 4096)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {}
                })
                .connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort()))
                .sync()
                .channel();
        SSLEngine engine = onLoop(() -> TlsOutputDriver.handshakeClient(context, channel, protocol));
        peer = accepted.get(10, TimeUnit.SECONDS);
        driver = onLoop(() -> new TlsOutputDriver(channel, endpoint, engine, reservedPackets));
    }

    <T> T onLoop(Callable<T> task) throws Exception {
        return channel.eventLoop().submit(task).sync().getNow();
    }

    byte[] read(int count) throws IOException {
        byte[] bytes = new byte[count];
        new DataInputStream(peer.getInputStream()).readFully(bytes);
        return bytes;
    }

    /** 상대가 TLS close_notify나 EOF를 볼 때까지 읽은 평문. */
    byte[] readToEnd() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int n;
        while ((n = peer.getInputStream().read(chunk)) >= 0) out.write(chunk, 0, n);
        return out.toByteArray();
    }

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
                server.close();
            } finally {
                serverThread.shutdownNow();
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
            }
        }
    }
}
