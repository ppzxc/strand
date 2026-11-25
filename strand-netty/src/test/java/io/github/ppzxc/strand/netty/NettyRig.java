package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

final class NettyRig implements AutoCloseable {
    static final int TYPE = AppFixture.TYPE;
    private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);

    static final class LimitedChannel extends StrandChannel {
        private final AtomicLong capacity = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong written = new AtomicLong();
        private final AtomicLong stalls = new AtomicLong();
        private volatile boolean failing;

        LimitedChannel() {
            super();
        }

        void capacity(long bytes) {
            capacity.set(bytes);
        }

        long stallCount() {
            return stalls.get();
        }

        LimitedChannel(java.nio.channels.SocketChannel accepted) {
            super(null, accepted);
        }

        @Override
        public int writeAttempt(ByteBuffer bytes) throws IOException {
            if (failing) {
                throw new IOException("시험이 쓰기를 실패시킨다");
            }
            int wanted = bytes.remaining();
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

    final AppFixture fixture = AppFixture.plain();
    final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1_000));
    final LimitedChannel channel = new LimitedChannel();
    final EventLoop loop = fixture.group.next();
    PeerLink peer;
    NettyStrandConnection connection;
    private ServerSocket listener;
    private final Set<Long> sentinelStreams = Collections.synchronizedSet(new HashSet<>());
    private CountDownLatch gateEntered;
    private CountDownLatch gateRelease;

    private NettyRig() {}

    OwnerBudgets owner;

    InetSocketAddress target;
    final CompletableFuture<NettyStrandConnection> ready = new CompletableFuture<>();
    private EndpointConfig config;
    ConnectionParams params;

    private void start() throws Exception {
        prepare();
        connection.startClient(target);
        listener.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
        Socket accepted = listener.accept();
        peer = new PeerLink(Endpoint.server(config), accepted);
        ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private void prepare() throws Exception {
        config = EndpointConfig.defaults()
                .knownTypes(Collections.singleton(TYPE))
                .maxConcurrentStreams(8)
                .ackTimeoutMs(HOUR_MS)
                .idleIntervalMs(HOUR_MS * 24);
        owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        target = new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort());
        params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                config,
                target,
                loop,
                new SerialExecutor(fixture.executor),
                fixture.executor,
                budgets,
                space,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                clock.get() + TimeUnit.HOURS.toNanos(1),
                TimeUnit.SECONDS.toNanos(5),
                64L << 20,
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
                null,
                null,
                null,
                null,
                ByteBufAllocator.DEFAULT);
        channel.config().setTcpNoDelay(true);
        connection = new NettyStrandConnection(params, channel);
    }

    static NettyRig idle() throws Exception {
        NettyRig rig = new NettyRig();
        try {
            rig.prepare();
        } catch (Exception | Error e) {
            rig.close();
            throw e;
        }
        return rig;
    }

    static NettyRig client() throws Exception {
        NettyRig rig = new NettyRig();
        try {
            rig.start();
        } catch (Exception | Error e) {
            rig.close();
            throw e;
        }
        return rig;
    }

    void advance(Duration duration) {
        clock.addAndGet(duration.toNanos());
    }

    void closeGate() throws InterruptedException {
        gateEntered = new CountDownLatch(1);
        gateRelease = new CountDownLatch(1);
        CountDownLatch entered = gateEntered;
        CountDownLatch release = gateRelease;
        loop.execute(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        if (!entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)) {
            throw new AssertionError("이벤트 루프가 문 앞에 서지 않았다");
        }
    }

    void openGate() {
        CountDownLatch release = gateRelease;
        gateRelease = null;
        if (release != null) {
            release.countDown();
        }
    }

    void inSameStep(Runnable before, byte[] input) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        loop.execute(() -> {
            try {
                before.run();
                channel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(input));
                done.complete(null);
            } catch (RuntimeException | Error e) {
                done.completeExceptionally(e);
            }
        });
        done.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
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

    long written() {
        return channel.written.get();
    }

    long stalls() {
        return channel.stalls.get();
    }

    void writeCapacity(long bytes) {
        channel.capacity.set(bytes);
    }

    void failWrites() {
        channel.failing = true;
    }

    List<Long> peerRequestStreams() {
        List<Long> streams = new ArrayList<>(peer.requestStreams());
        streams.removeAll(sentinelStreams);
        return streams;
    }

    List<long[]> peerResets() {
        List<long[]> resets = new ArrayList<>();
        for (long[] reset : peer.resets()) {
            if (!sentinelStreams.contains(reset[0])) {
                resets.add(reset);
            }
        }
        return resets;
    }

    List<Event.Kind> peerEventKinds() {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : peer.events()) {
            if (!sentinelStreams.contains(event.streamId())) {
                kinds.add(event.kind());
            }
        }
        return kinds;
    }

    void peerRespond(long streamId, int status, byte[] body) {
        peer.run(endpoint -> endpoint.respond(streamId, status, body));
    }

    byte[] detachedResponse(long streamId, int status, byte[] body) {
        return peer.runDetached(endpoint -> endpoint.respond(streamId, status, body));
    }

    void settle() throws Exception {
        int before = peer.requestStreams().size();
        RequestHandle sentinel = connection.request(Request.of(TYPE, new byte[0]));
        awaitTrue(() -> peer.requestStreams().size() > before);
        long stream = peer.requestStreams().get(before);
        sentinelStreams.add(stream);
        peerRespond(stream, Status.OK, new byte[0]);
        awaitTrue(() -> sentinel.result().isDone());
    }

    long used(Resource resource) {
        return connection.resourceUsage(resource).current();
    }

    static void awaitTrue(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("기다린 조건이 되지 않았다");
            }
            Thread.sleep(2);
        }
    }

    @Override
    public void close() {
        openGate();
        if (connection != null) {
            connection.abort();
        }
        if (peer != null) {
            peer.close();
        }
        try {
            if (listener != null) {
                listener.close();
            }
        } catch (IOException ignored) {
        }
        fixture.close();
    }
}
