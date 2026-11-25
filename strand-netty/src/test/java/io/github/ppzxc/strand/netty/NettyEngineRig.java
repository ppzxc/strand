package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;

final class NettyEngineRig implements AutoCloseable {
    static final int TYPE = AppFixture.TYPE;
    private static final int SMALL_BUFFER = 4096;
    private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);

    static final class TestChannel extends StrandChannel {
        private final AtomicLong capacity = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong written = new AtomicLong();
        private final AtomicLong stalls = new AtomicLong();
        private final ByteArrayOutputStream swallowed = new ByteArrayOutputStream();
        private volatile boolean failing;
        private volatile boolean muted;
        private volatile RuntimeException unexpected;

        TestChannel() {
            super();
        }

        TestChannel(SocketChannel accepted) {
            super(null, accepted);
        }

        void capacity(long bytes) {
            capacity.set(bytes);
        }

        void addCapacity(long bytes) {
            capacity.updateAndGet(current -> current > Long.MAX_VALUE - bytes ? Long.MAX_VALUE : current + bytes);
        }

        long written() {
            return written.get();
        }

        long stalls() {
            return stalls.get();
        }

        void failWrites() {
            failing = true;
        }

        void mute() {
            muted = true;
        }

        void failWritesUnexpectedly(RuntimeException error) {
            unexpected = error;
        }

        synchronized byte[] swallowedBytes() {
            byte[] bytes = swallowed.toByteArray();
            swallowed.reset();
            return bytes;
        }

        @Override
        public int writeAttempt(ByteBuffer bytes) throws IOException {
            RuntimeException error = unexpected;
            if (error != null) {
                throw error;
            }
            if (failing) {
                throw new IOException("시험이 쓰기를 실패시킨다");
            }
            int wanted = bytes.remaining();
            if (muted) {
                synchronized (this) {
                    byte[] copy = new byte[wanted];
                    bytes.get(copy);
                    swallowed.write(copy, 0, copy.length);
                }
                written.addAndGet(wanted);
                return wanted;
            }
            int allowed = (int) Math.min(capacity.get(), wanted);
            int count = 0;
            if (allowed > 0) {
                ByteBuffer limited = bytes.duplicate();
                limited.limit(bytes.position() + allowed);
                count = super.writeAttempt(limited);
                bytes.position(bytes.position() + count);
                capacity.addAndGet(-count);
                written.addAndGet(count);
            }
            if (wanted > 0 && count < wanted) {
                stalls.incrementAndGet();
            }
            return count;
        }
    }

    static final class Tuning {
        UnaryOperator<EndpointConfig> config = c -> c;
        UnaryOperator<EndpointConfig> peerConfig = c -> c;
        long closeFinishNanos = TimeUnit.SECONDS.toNanos(5);
        long writeQueueBytes = 64L << 20;
        long perConnection = 8;

        Tuning perConnection(long value) {
            this.perConnection = value;
            return this;
        }

        Tuning config(UnaryOperator<EndpointConfig> value) {
            this.config = value;
            return this;
        }

        Tuning peerConfig(UnaryOperator<EndpointConfig> value) {
            this.peerConfig = value;
            return this;
        }

        Tuning closeFinish(Duration value) {
            this.closeFinishNanos = value.toNanos();
            return this;
        }

        Tuning writeQueueBytes(long value) {
            this.writeQueueBytes = value;
            return this;
        }
    }

    final AppFixture fixture;
    final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1_000));
    final TestChannel channel;
    final EventLoop loop;
    final NettyStrandConnection connection;
    final OwnerBudgets owner;
    PeerLink peer;
    private Socket peerSocket;
    private final AtomicInteger flooded = new AtomicInteger();
    private final ConnectionParams.Role role;
    private final EndpointConfig peerConfig;
    private final Set<Long> ignoredStreams = Collections.synchronizedSet(new HashSet<>());
    private ServerSocket clientListener;
    private ServerSocketChannel serverListener;
    private Socket serverSideSocket;
    private final CompletableFuture<NettyStrandConnection> ready = new CompletableFuture<>();

    private NettyEngineRig(ConnectionParams.Role role, RequestHandler handler, Tuning tuning) throws Exception {
        this.role = role;
        this.fixture = new AppFixture.Spec().perConnection(tuning.perConnection).plain();
        this.loop = fixture.group.next();
        EndpointConfig config = tuning.config.apply(EndpointConfig.defaults()
                .knownTypes(Collections.singleton(TYPE))
                .maxConcurrentStreams(8)
                .ackTimeoutMs(HOUR_MS)
                .idleIntervalMs(HOUR_MS * 24));
        this.peerConfig = tuning.peerConfig.apply(config);
        this.owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        InetSocketAddress target;
        if (role == ConnectionParams.Role.CLIENT) {
            clientListener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            target = new InetSocketAddress(InetAddress.getLoopbackAddress(), clientListener.getLocalPort());
            channel = new TestChannel();
            channel.config().setOption(io.netty.channel.ChannelOption.SO_RCVBUF, SMALL_BUFFER);
        } else {
            serverListener = ServerSocketChannel.open();
            serverListener.setOption(java.net.StandardSocketOptions.SO_RCVBUF, SMALL_BUFFER);
            serverListener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            int port = ((InetSocketAddress) serverListener.getLocalAddress()).getPort();
            serverSideSocket = new Socket();
            serverSideSocket.setSendBufferSize(SMALL_BUFFER);
            serverSideSocket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            channel = new TestChannel(serverListener.accept());
            target = new InetSocketAddress(InetAddress.getLoopbackAddress(), serverSideSocket.getLocalPort());
        }
        channel.config().setTcpNoDelay(true);
        ConnectionParams params = new ConnectionParams(
                role,
                config,
                target,
                loop,
                new SerialExecutor(fixture.executor),
                fixture.executor,
                budgets,
                space,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                clock.get() + TimeUnit.HOURS.toNanos(1),
                tuning.closeFinishNanos,
                tuning.writeQueueBytes,
                clock::get,
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
                handler,
                null,
                role == ConnectionParams.Role.SERVER ? credentials -> AuthenticationResult.accepted("peer") : null,
                null,
                ByteBufAllocator.DEFAULT);
        this.connection = new NettyStrandConnection(params, channel);
    }

    private void start() throws Exception {
        if (role == ConnectionParams.Role.CLIENT) {
            connection.startClient(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), clientListener.getLocalPort()));
            clientListener.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
            Socket accepted = clientListener.accept();
            peerSocket = accepted;
            accepted.setSendBufferSize(SMALL_BUFFER);
            peer = new PeerLink(Endpoint.server(peerConfig), accepted);
            ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        } else {
            connection.startServer();
            peerSocket = serverSideSocket;
            peer = new PeerLink(Endpoint.client(peerConfig), serverSideSocket);
            peer.run(endpoint -> {});
            NettyRig.awaitTrue(() -> peer.events().stream().anyMatch(e -> e.kind() == Event.Kind.AUTHENTICATED));
        }
    }

    static NettyEngineRig client(Tuning tuning) throws Exception {
        NettyEngineRig rig = new NettyEngineRig(ConnectionParams.Role.CLIENT, null, tuning);
        rig.startOrClose();
        return rig;
    }

    static NettyEngineRig client() throws Exception {
        return client(new Tuning());
    }

    static NettyEngineRig server(RequestHandler handler, Tuning tuning) throws Exception {
        NettyEngineRig rig = new NettyEngineRig(ConnectionParams.Role.SERVER, handler, tuning);
        rig.startOrClose();
        return rig;
    }

    private void startOrClose() throws Exception {
        try {
            start();
        } catch (Exception | Error e) {
            close();
            throw e;
        }
    }

    void tick(long ms) throws Exception {
        clock.addAndGet(Duration.ofMillis(ms).toNanos());
        step();
    }

    void step() throws Exception {
        inject(new byte[0]);
    }

    void inject(byte[] bytes) throws Exception {
        onLoop(() -> channel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(bytes)));
    }

    void inSameStep(Runnable before, byte[] input) throws Exception {
        onLoop(() -> {
            before.run();
            channel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(input));
        });
    }

    void onLoop(Runnable task) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        loop.execute(() -> {
            try {
                task.run();
                done.complete(null);
            } catch (RuntimeException | Error e) {
                done.completeExceptionally(e);
            }
        });
        done.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
    }

    void settleLoop() throws Exception {
        onLoop(() -> {});
    }

    void awaitStalls(long count) throws Exception {
        NettyRig.awaitTrue(() -> channel.stalls() >= count);
    }

    void peerHangsUp() {
        peer.disconnect();
    }

    CompletableFuture<Void> flood(int count, IntFunction<byte[]> frameOf) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        java.io.OutputStream out = peerSocket.getOutputStream();
                        for (int index = 0; index < count; index += 100) {
                            ByteArrayOutputStream batch = new ByteArrayOutputStream();
                            int end = Math.min(count, index + 100);
                            for (int i = index; i < end; i++) {
                                batch.write(frameOf.apply(i));
                            }
                            out.write(batch.toByteArray());
                            flooded.addAndGet(end - index);
                        }
                        out.flush();
                        done.complete(null);
                    } catch (IOException e) {
                        done.completeExceptionally(e);
                    }
                },
                "engine-rig-flood");
        thread.setDaemon(true);
        thread.start();
        return done;
    }

    int flooded() {
        return flooded.get();
    }

    boolean closed() {
        return connection.closed().isDone();
    }

    List<Long> peerRequestStreams() {
        List<Long> streams = new ArrayList<>(peer.requestStreams());
        streams.removeAll(ignoredStreams);
        return streams;
    }

    List<Long> awaitPeerRequests(int count) throws Exception {
        NettyRig.awaitTrue(() -> peerRequestStreams().size() >= count);
        return peerRequestStreams();
    }

    void peerRespond(long streamId, int status, byte[] body) {
        peer.run(endpoint -> endpoint.respond(streamId, status, body));
    }

    long used(Resource resource) {
        return connection.resourceUsage(resource).current();
    }

    @Override
    public void close() {
        connection.abort();
        if (peer != null) {
            peer.close();
        }
        closeQuietly(clientListener);
        try {
            if (serverListener != null) {
                serverListener.close();
            }
        } catch (IOException ignored) {
        }
        fixture.close();
    }

    private static void closeQuietly(ServerSocket socket) {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }
}
