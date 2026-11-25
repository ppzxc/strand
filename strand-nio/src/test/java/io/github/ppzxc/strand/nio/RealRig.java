package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import io.github.ppzxc.strand.nio.internal.TestTls;
import io.github.ppzxc.strand.nio.internal.TlsSizes;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class RealRig implements AutoCloseable {

    static final class RecordingPassage implements Passage {
        private final Passage delegate;
        final AtomicLong consumed = new AtomicLong();
        final AtomicLong stalls = new AtomicLong();

        RecordingPassage(Passage delegate) {
            this.delegate = delegate;
        }

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
            return delegate.await(timeoutMs, waitForRead, waitForWrite);
        }

        @Override
        public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
            int count = delegate.writeBytes(bytes, offset, length);
            consumed.addAndGet(count);
            if (length > 0 && count < length) {
                stalls.incrementAndGet();
            }
            return count;
        }

        @Override
        public boolean pendingOutput() {
            return delegate.pendingOutput();
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void shutdownOutput() {
            delegate.shutdownOutput();
        }

        @Override
        public void close() {
            delegate.close();
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
                }
            }
            killConnections();
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

    final AppFixture fixture;
    final BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
    final NioStrandServer server;
    final Listener listener;
    final PausableRelay relay;
    final RecordingPassage recording;
    final NioStrandConnection connection;

    RealRig(boolean tls) throws Exception {
        AppFixture.Spec spec = new AppFixture.Spec().maxFrameSize(1L << 20).bodyBytes(1 << 20);
        this.fixture = tls ? spec.tls() : spec.plain();
        this.server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(received::add)
                .build();
        this.listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        this.relay = new PausableRelay(((InetSocketAddress) listener.localAddress()).getPort());
        InetSocketAddress relayAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), relay.port());
        OwnerBudgets owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        ProgressSpace space =
                ProgressSpace.derive(8, 8, fixture.maxFrameSize, tls ? TlsSizes.of(TestTls.client()) : null);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        javax.net.ssl.SSLContext clientContext = tls ? TestTls.client() : null;
        RecordingPassage[] opened = new RecordingPassage[1];
        CompletableFuture<NioStrandConnection> ready = new CompletableFuture<>();
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                EndpointConfig.defaults().maxFrameSize(fixture.maxFrameSize).maxConcurrentStreams(8),
                remaining -> {
                    SocketChannel channel = SocketChannel.open();
                    channel.setOption(StandardSocketOptions.SO_SNDBUF, 4096);
                    channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    channel.socket().connect(relayAddress);
                    opened[0] = new RecordingPassage(
                            PassageFactory.open(channel, clientContext, relayAddress.getHostString(), relay.port()));
                    return opened[0];
                },
                relayAddress,
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
                    public void ready(NioStrandConnection value) {
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
                tls);
        NioStrandConnection created = new NioStrandConnection(params);
        created.start();
        this.connection = ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        this.recording = opened[0];
        awaitServerHandler();
    }

    private void awaitServerHandler() throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            io.github.ppzxc.strand.api.RequestHandle probe =
                    connection.request(io.github.ppzxc.strand.api.Request.of(AppFixture.TYPE, new byte[0]));

            IncomingRequest request = null;
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
            while (request == null && !probe.result().isDone() && System.nanoTime() < limit) {
                request = received.poll(5, TimeUnit.MILLISECONDS);
            }
            if (request != null) {
                request.respond(io.github.ppzxc.strand.api.Status.OK, new byte[0]);
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

    static void awaitTrue(java.util.function.BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("기다린 조건이 되지 않았다");
            }
            Thread.sleep(5);
        }
    }

    long used(Resource resource) {
        return connection.resourceUsage(resource).current();
    }

    @Override
    public void close() {
        connection.abort();
        relay.close();
        try {
            listener.close();
        } catch (Exception ignored) {

        }
        fixture.close();
    }
}
