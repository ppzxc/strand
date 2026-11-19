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

/**
 * 연결 엔진 하나(클라이언트)를 수동 시계와 쓰기량을 제한하는 실제 채널에 꽂고, 맞은편 실제 소켓에서 core {@link Endpoint}를 {@link PeerLink}로 굴린다. 채널의 쓰기
 * 한도로 쓰기 0·부분·전체를 결정적으로 만들고, 이벤트 루프를 막는 문(gate)으로 앱 호출이 core 등록 전 대기에 머무는 틈을 만든다. 엔진의 한 걸음은 입력 처리 → 시계 →
 * 명령 처리 순서이므로 {@link #inSameStep}으로 입력과 명령을 같은 걸음에 겹친다.
 */
final class NettyRig implements AutoCloseable {
    static final int TYPE = AppFixture.TYPE;
    private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);

    /** {@code writeAttempt}가 받아 주는 바이트를 한도로 제한하고, 받아 준 합과 한 바이트도 받아 주지 못한(멈춤) 횟수를 센다. */
    static final class LimitedChannel extends StrandChannel {
        private final AtomicLong capacity = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong written = new AtomicLong();
        private final AtomicLong stalls = new AtomicLong();
        private volatile boolean failing;

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
    final PeerLink peer;
    final NettyStrandConnection connection;
    private final ServerSocket listener;
    private final Set<Long> sentinelStreams = Collections.synchronizedSet(new HashSet<>());
    private CountDownLatch gateEntered;
    private CountDownLatch gateRelease;

    private NettyRig() throws Exception {
        EndpointConfig config = EndpointConfig.defaults()
                .knownTypes(Collections.singleton(TYPE))
                .maxConcurrentStreams(8)
                .ackTimeoutMs(HOUR_MS)
                .idleIntervalMs(HOUR_MS * 24);
        OwnerBudgets owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        this.listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        InetSocketAddress target = new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort());
        CompletableFuture<NettyStrandConnection> ready = new CompletableFuture<>();
        ConnectionParams params = new ConnectionParams(
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
        this.connection = new NettyStrandConnection(params, channel);
        connection.startClient(target);
        listener.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
        Socket accepted = listener.accept();
        this.peer = new PeerLink(Endpoint.server(config), accepted);
        ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /** 엔진이 클라이언트이고 상대(코어 서버)가 요청에 응답하는 연결. 준비가 끝난 상태로 돌려준다. */
    static NettyRig client() throws Exception {
        return new NettyRig();
    }

    // ---------------------------------------------------------------- 시계, 문, 같은 걸음

    /** 수동 시계를 올린다. 엔진은 다음 걸음(깨어남 타이머나 호출)에서 이를 본다. */
    void advance(Duration duration) {
        clock.addAndGet(duration.toNanos());
    }

    /** 이벤트 루프를 막아, 그동안의 앱 호출이 core 등록 전 대기에 머물게 한다. */
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

    /**
     * 이벤트 루프의 한 작업 안에서 앞 동작을 하고 받은 바이트를 엔진 입력에 넣는다. 엔진의 걸음은 입력 → 시계 → 명령 순서이므로, 앞 동작이 낸 명령과 시계는 이
     * 입력과 같은 걸음에서 입력 뒤에 처리된다.
     */
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

    // ---------------------------------------------------------------- 쓰기 통로

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

    // ---------------------------------------------------------------- 상대 관찰

    /** 상대가 받은 요청의 스트림 번호(받은 순서). 동기화용 표지 요청은 뺀다. */
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

    /** 상대가 요청에 응답한다(소켓으로). */
    void peerRespond(long streamId, int status, byte[] body) {
        peer.run(endpoint -> endpoint.respond(streamId, status, body));
    }

    /** 상대의 응답을 소켓에 쓰지 않고 바이트로 받는다. */
    byte[] detachedResponse(long streamId, int status, byte[] body) {
        return peer.runDetached(endpoint -> endpoint.respond(streamId, status, body));
    }

    /**
     * 엔진이 지금까지 받은 것을 다 처리했고 쓸 것을 다 썼음을 알려 주는 동기화 지점. 표지 요청을 하나 내고 상대가 받아 응답하게 한 뒤, 그 결과가 끝나기를 기다린다.
     * 엔진은 명령과 출력을 차례로 처리하므로, 이 표지에 앞서 낸 취소·RESET이 있었다면 상대가 이미 받았다. 표지 스트림은 관찰 목록에서 뺀다.
     */
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

    static void awaitTrue(java.util.function.BooleanSupplier condition) throws Exception {
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
        connection.abort();
        peer.close();
        try {
            listener.close();
        } catch (IOException ignored) {
            // 정리 중의 오류는 할 수 있는 일이 없다.
        }
        fixture.close();
    }
}
