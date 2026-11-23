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
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;

/**
 * 연결 엔진 하나(클라이언트 또는 서버)를 수동 시계와 시험이 쓰기를 쥐는 실제 채널에 꽂고, 맞은편 실제 소켓에서 core {@link Endpoint}를 {@link PeerLink}로 굴린다.
 * nio 쪽 {@code EngineRig}에 대응하는 netty 시험대다. 채널의 쓰기 용량으로 쓰기 막힘(0)·부분·전체를, 삼킴({@link TestChannel#mute})으로 상대에 닿지 않는 쓰기를,
 * 입력 주입({@link #inject})으로 상대 없이 엔진 입력을 만든다. 엔진의 한 걸음은 입력 처리 → 시계 → 명령 처리 순서이고 시계는 수동이라, 시계를 올린 뒤 {@link #tick}이 빈
 * 입력으로 걸음을 한 번 돌려 엔진이 올라간 시계를 보게 한다.
 */
final class NettyEngineRig implements AutoCloseable {
    static final int TYPE = AppFixture.TYPE;
    private static final int SMALL_BUFFER = 4096;
    private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);

    /** 쓰기 용량·삼킴·실패를 시험이 정하고, 받아 준 합과 한 바이트도 못 받아 준(멈춤) 횟수를 센다. */
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

        /** 쓰기가 성공한 것처럼 돌아오되 상대에게 닿지 않는다. 삼킨 바이트는 {@link #swallowedBytes}로 꺼낸다. */
        void mute() {
            muted = true;
        }

        /** 다음 쓰기부터 예상 못한 런타임 예외를 낸다(입출력 오류가 아니다). */
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

    /** 시험이 정하는 조정. 기본은 확인 대기 1시간, 유휴 PING 끔에 가까운 느슨한 값이다. */
    static final class Tuning {
        UnaryOperator<EndpointConfig> config = c -> c;
        UnaryOperator<EndpointConfig> peerConfig = c -> c;
        long closeFinishNanos = TimeUnit.SECONDS.toNanos(5);
        long writeQueueBytes = 64L << 20;
        long perConnection = 8;

        /** 연결별 한도 규모. 키우면 제어 출력 공간이 커져 쓰기 대기열 상한이 읽기를 먼저 멈춘다. */
        Tuning perConnection(long value) {
            this.perConnection = value;
            return this;
        }

        /** 두 끝의 엔드포인트 설정을 고친다. */
        Tuning config(UnaryOperator<EndpointConfig> value) {
            this.config = value;
            return this;
        }

        /** 상대(코어 엔드포인트)의 설정만 더 고친다. */
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
            // 엔진이 읽기를 멈추면 상대가 밀어 넣는 것이 소켓 버퍼에 다 들어가 버리지 않게 받는 버퍼를 작게 한다.
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
            awaitTrue(() -> peer.events().stream().anyMatch(e -> e.kind() == Event.Kind.AUTHENTICATED));
        }
    }

    /** 엔진이 클라이언트이고 상대(코어 서버)가 요청에 응답하는 연결. 준비가 끝난 상태로 돌려준다. */
    static NettyEngineRig client(Tuning tuning) throws Exception {
        NettyEngineRig rig = new NettyEngineRig(ConnectionParams.Role.CLIENT, null, tuning);
        rig.startOrClose();
        return rig;
    }

    static NettyEngineRig client() throws Exception {
        return client(new Tuning());
    }

    /** 엔진이 서버이고 상대(코어 클라이언트)가 요청을 보내는 연결. 준비가 끝난 상태로 돌려준다. */
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

    // ---------------------------------------------------------------- 시계와 입력

    /** 수동 시계를 올리고 엔진이 올라간 시계를 보도록 걸음을 한 번 돌린다. */
    void tick(long ms) throws Exception {
        clock.addAndGet(Duration.ofMillis(ms).toNanos());
        step();
    }

    /** 빈 입력으로 엔진의 걸음을 한 번 돌리고 끝나기를 기다린다. */
    void step() throws Exception {
        inject(new byte[0]);
    }

    /** 받은 바이트를 엔진 입력에 직접 넣는다(상대 소켓을 거치지 않는다). 이벤트 루프에서 끝나기를 기다린다. */
    void inject(byte[] bytes) throws Exception {
        onLoop(() -> channel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(bytes)));
    }

    /** 이벤트 루프의 한 작업 안에서 앞 동작을 하고 바이트를 엔진 입력에 넣는다. 앞 동작이 낸 시계·명령은 같은 걸음에서 입력 뒤에 처리된다. */
    void inSameStep(Runnable before, byte[] input) throws Exception {
        onLoop(() -> {
            before.run();
            channel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(input));
        });
    }

    /** 이벤트 루프에서 작업을 실행하고 끝나기를 기다린다. */
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

    /** 이벤트 루프가 지금까지 맡은 작업을 다 돌 때까지 기다린다. */
    void settleLoop() throws Exception {
        onLoop(() -> {});
    }

    /** 엔진이 쓰기를 {@code count}번째 막힐 때까지 기다린다. */
    void awaitStalls(long count) throws Exception {
        awaitTrue(() -> channel.stalls() >= count);
    }

    /** 상대 소켓을 닫아 엔진이 끊김(끝)을 읽게 한다. */
    void peerHangsUp() {
        peer.disconnect();
    }

    /**
     * 상대 소켓으로 프레임 {@code count}개를 끝없이 밀어 넣는다(상대 엔드포인트를 거치지 않는다). 엔진이 읽기를 멈추면 소켓 버퍼가 차서 쓰는 스레드가 막힌다. 다 넘기면 완료되는
     * 미래를 돌려주고, 지금까지 소켓에 넘긴 프레임 수는 {@link #flooded}다.
     */
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

    /** {@link #flood}가 지금까지 소켓에 넘긴 프레임 수. */
    int flooded() {
        return flooded.get();
    }

    boolean closed() {
        return connection.closed().isDone();
    }

    // ---------------------------------------------------------------- 상대 관찰

    /** 상대가 받은 요청의 스트림 번호(받은 순서). 무시하기로 한 스트림은 뺀다. */
    List<Long> peerRequestStreams() {
        List<Long> streams = new ArrayList<>(peer.requestStreams());
        streams.removeAll(ignoredStreams);
        return streams;
    }

    /** 상대가 요청을 {@code count}개 받을 때까지 기다리고 그 스트림 번호들을 돌려준다. */
    List<Long> awaitPeerRequests(int count) throws Exception {
        awaitTrue(() -> peerRequestStreams().size() >= count);
        return peerRequestStreams();
    }

    /** 상대가 요청에 응답한다(소켓으로). */
    void peerRespond(long streamId, int status, byte[] body) {
        peer.run(endpoint -> endpoint.respond(streamId, status, body));
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
            // 정리 중의 오류는 할 수 있는 일이 없다.
        }
        fixture.close();
    }

    private static void closeQuietly(ServerSocket socket) {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // 정리 중의 오류는 할 수 있는 일이 없다.
        }
    }
}
