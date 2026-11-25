package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.TestTls;
import io.github.ppzxc.strand.netty.internal.TlsSizes;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

final class NettyRealRig implements AutoCloseable {
    static final class RecordingChannel extends StrandChannel {
        final AtomicLong consumed = new AtomicLong();
        final AtomicLong stalls = new AtomicLong();

        @Override
        public int writeAttempt(ByteBuffer bytes) throws IOException {
            int wanted = bytes.remaining();
            int count = super.writeAttempt(bytes);
            consumed.addAndGet(count);
            if (wanted > 0 && count < wanted) {
                stalls.incrementAndGet();
            }
            return count;
        }
    }

    static final class PausableRelay implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket();
        private final int targetPort;
        private final List<Socket> sockets = new ArrayList<>();
        private volatile boolean paused;
        private volatile boolean closed;

        PausableRelay(int targetPort) throws IOException {
            this.targetPort = targetPort;
            listener.setReceiveBufferSize(2048);
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            Thread accept = new Thread(this::acceptLoop, "relay-accept");
            accept.setDaemon(true);
            accept.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        void pause() {
            paused = true;
        }

        void resume() {
            paused = false;
        }

        private void acceptLoop() {
            try {
                while (!closed) {
                    Socket client = listener.accept();
                    Socket server = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                    client.setTcpNoDelay(true);
                    server.setTcpNoDelay(true);
                    synchronized (sockets) {
                        sockets.add(client);
                        sockets.add(server);
                    }
                    pump(client, server, true);
                    pump(server, client, false);
                }
            } catch (IOException e) {
            }
        }

        private void pump(Socket from, Socket to, boolean upstream) {
            Thread thread = new Thread(
                    () -> {
                        byte[] buffer = new byte[4096];
                        try {
                            InputStream in = from.getInputStream();
                            OutputStream out = to.getOutputStream();
                            for (; ; ) {
                                while (upstream && paused && !closed) {
                                    Thread.sleep(2);
                                }
                                int count = in.read(buffer);
                                if (count < 0) {
                                    break;
                                }
                                out.write(buffer, 0, count);
                            }
                        } catch (IOException | InterruptedException e) {
                        } finally {
                            closeQuietly(from);
                            closeQuietly(to);
                        }
                    },
                    upstream ? "relay-up" : "relay-down");
            thread.setDaemon(true);
            thread.start();
        }

        void killConnections() {
            synchronized (sockets) {
                sockets.forEach(PausableRelay::closeQuietly);
            }
        }

        void resetConnections() {
            synchronized (sockets) {
                for (Socket socket : sockets) {
                    try {
                        socket.setSoLinger(true, 0);
                    } catch (IOException ignored) {
                    }
                    closeQuietly(socket);
                }
            }
        }

        private static void closeQuietly(Socket socket) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }

        @Override
        public void close() {
            closed = true;
            killConnections();
            try {
                listener.close();
            } catch (IOException ignored) {
            }
        }
    }

    AppFixture fixture;
    final BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
    NettyStrandServer server;
    Listener listener;
    PausableRelay relay;
    final RecordingChannel recording = new RecordingChannel();
    NettyStrandConnection connection;

    NettyRealRig(boolean tls) throws Exception {
        try {
            start(tls);
        } catch (Exception | Error e) {
            close();
            throw e;
        }
    }

    private void start(boolean tls) throws Exception {
        AppFixture.Spec spec = new AppFixture.Spec().maxFrameSize(1L << 20).bodyBytes(1 << 20);
        fixture = tls ? spec.tls() : spec.plain();
        server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(received::add)
                .build();
        listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        relay = new PausableRelay(((InetSocketAddress) listener.localAddress()).getPort());
        InetSocketAddress relayAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), relay.port());
        OwnerBudgets owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        TlsSizes sizes = tls ? TlsSizes.of(TestTls.client()) : null;
        ProgressSpace space = ProgressSpace.derive(8, 8, fixture.maxFrameSize, sizes);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        ConnectionParams.Tls security = tls
                ? new ConnectionParams.Tls(TestTls.client(), sizes, relayAddress.getHostString(), relay.port())
                : null;
        CompletableFuture<NettyStrandConnection> ready = new CompletableFuture<>();
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                EndpointConfig.defaults().maxFrameSize(fixture.maxFrameSize).maxConcurrentStreams(8),
                relayAddress,
                fixture.group.next(),
                new SerialExecutor(fixture.executor),
                fixture.executor,
                budgets,
                space,
                fixture.maxFrameSize,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(10),
                TimeUnit.SECONDS.toNanos(5),
                64L << 20,
                System::nanoTime,
                new ConnectionParams.PreparationObserver() {
                    @Override
                    public void ready(NettyStrandConnection value) {
                        ready.complete(value);
                    }

                    @Override
                    public void failed(PreparationFailure failure) {
                        ready.completeExceptionally(failure.cause());
                    }
                },
                null,
                null,
                null,
                security,
                ByteBufAllocator.DEFAULT);
        recording.config().setOption(ChannelOption.SO_SNDBUF, 4096);
        recording.config().setTcpNoDelay(true);
        NettyStrandConnection created = new NettyStrandConnection(params, recording);
        created.startClient(relayAddress);
        connection = ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        awaitServerHandler();
    }

    private void awaitServerHandler() throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            RequestHandle probe = connection.request(Request.of(AppFixture.TYPE, new byte[0]));
            IncomingRequest request = null;
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
            while (request == null && !probe.result().isDone() && System.nanoTime() < limit) {
                request = received.poll(5, TimeUnit.MILLISECONDS);
            }
            if (request != null) {
                request.respond(Status.OK, new byte[0]);
            }
            awaitTrue(() -> probe.result().isDone());
            if (request != null) {
                return;
            }
        }
        throw new AssertionError("서버가 핸들러를 활성화하지 않았다");
    }

    void awaitStalls(long count) throws Exception {
        awaitTrue(() -> recording.stalls.get() >= count);
    }

    static void awaitTrue(BooleanSupplier condition) throws Exception {
        NettyRig.awaitTrue(condition);
    }

    long used(Resource resource) {
        return connection.resourceUsage(resource).current();
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.abort();
        }
        if (relay != null) {
            relay.close();
        }
        try {
            if (listener != null) {
                listener.close();
            }
        } catch (Exception ignored) {
        }
        if (fixture != null) {
            fixture.close();
        }
    }
}
