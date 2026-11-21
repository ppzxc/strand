package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Extensions;
import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.TlsEngines;
import io.github.ppzxc.strand.netty.internal.TlsPipe;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.jspecify.annotations.Nullable;

/**
 * 연결 하나를 돌리는 Netty 이벤트 루프와 앱 호출의 경계. 채널이 고정된 EventLoop 하나만 {@link Endpoint}를 부르고, 다른 스레드의 호출은 예약을 먼저
 * 확보한 뒤 명령으로 큐에 넣고 그 EventLoop를 깨운다. 명령 큐는 예약 한도(송신 요청, 받은 요청의 응답)와 연결·인증·팩터리 결과 하나씩이 세므로 유한하다.
 *
 * <p>core는 유한 단위로만 구동한다. 입력은 {@code receiveBounded}로 한 번에 최대 {@value #MAX_UNITS_PER_STEP}단위씩 처리하고, 사건은 제어 출력·결과
 * 사건 공간({@link ProgressSpace}에서 정한 {@link CollectionSpace})만큼만 수거한다. 수거 공간이 모자라 멈추면 쓰기를 진행해 출력을 비우고 소비 위치부터
 * 다시 입력한다. 출력은 {@code nextOutput}으로 고르고 채널의 실제 쓰기량(TCP) 또는 평문 소비량(TLS)을 {@code outputProgress}로 보고한다.
 * 확인·시도·보고는 이벤트 루프 작업 하나 안에서 이어지므로 같은 연결의 다른 core 명령이 끼지 않는다. 쓰기 0은 보고하지 않는다.
 *
 * <p>읽기는 {@code autoRead}로 조절한다. 처리하지 못한 입력이 남았거나 core 출력이 쓰기 대기열 상한을 넘으면 읽기를 멈추고, 풀리면 다시 켠다. 쓰기가
 * 막히면 쓰기 가능 알림 대신 짧은 주기로 다시 시도한다(#315의 검증 범위).
 *
 * <p>사용자 코드(인증·팩터리·핸들러·알림·기본 관찰)는 연결의 {@code SerialExecutor}에서만 돌려 이벤트 루프와 분리하고 같은 연결의 호출을
 * 직렬화한다. 결과 보존은 그 실행을 기다리지 않는다.
 *
 * <p><b>정리와 closed.</b> 통로 정리는 {@code close()} Future가 아니라 채널의 {@code closeFuture}가 끝난 뒤 이 연결이 가진 내부 버퍼와 남은 결과를
 * 모두 정리하고 이벤트 루프의 후속 작업까지 지난 시점이다. 그때 예약을 돌려주고 {@code closed}를 완료한다.
 */
final class ConnectionEngine {
    /** 규격 2.5.2절의 error_code. */
    private static final int NO_ERROR = 0;
    private static final int EXCESSIVE_USE = 5;

    private static final int INTERNAL_ERROR = 2;
    /** 규격 2.5.2절 UNAUTHENTICATED. 준비 중 CLOSE가 이 코드면 실패 단계는 연결 인증이다. */
    private static final long UNAUTHENTICATED = 7;

    private static final int UNIMPLEMENTED = 12;
    /** 한 번에 처리하는 입력 단위의 상한. 넘으면 시계와 명령 처리를 먼저 한다. */
    static final int MAX_UNITS_PER_STEP = 64;
    /** TCP에서 한 번에 읽는 조각의 크기. TLS는 엔진이 알린 암호문 패킷 크기를 쓴다. */
    static final int TCP_READ_CHUNK_BYTES = 16 * 1024;
    /** 쓰기가 막혔을 때 다시 시도하는 주기(#315 검증에서 쓴 값). */
    private static final long WRITE_RETRY_NANOS = 1_000_000L;

    /** 연결 생성 때 별도로 확보하는 기본 종료 알림의 자리 수(결정 #307: 한 건). */
    private static final long DEFAULT_CLOSE_NOTICE_SLOTS = 1;

    /** 핸들러 예외 같은 로컬 진단의 기록처. 상대에게 보내는 것이 아니다. */
    private static final Logger LOGGER = Logger.getLogger("io.github.ppzxc.strand.netty");

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final byte[] EMPTY = new byte[0];
    private static final long NO_DEADLINE = Long.MIN_VALUE;

    private final NettyStrandConnection facade;
    private final ConnectionParams p;
    private volatile StrandChannel channel;
    private final EventLoop loop;
    private final CollectionSpace collectionSpace;
    private final Queue<Command> commands = new ConcurrentLinkedQueue<>();
    private final ResultCompletion<CloseNotice> closed;
    private final AtomicBoolean closeSubmitted = new AtomicBoolean();
    private final AtomicBoolean wakeQueued = new AtomicBoolean();
    private final AtomicBoolean tornDown = new AtomicBoolean();
    private volatile boolean abortRequested;
    private volatile boolean stopped;
    private volatile @Nullable RequestHandler activeHandler;

    // 이벤트 루프에서만 읽고 쓰는 상태.
    private @Nullable Loop active;
    private PreparationStage stage;
    private boolean preparing = true;
    private boolean preparingSlotHeld = true;
    private boolean releasedNotified;
    private long resolvedBytes;
    private final ArrayDeque<InetSocketAddress> pendingTargets = new ArrayDeque<>();
    private final Map<Long, OutgoingEntry> pending = new HashMap<>();
    private final Map<Long, IncomingRequest> incoming = new HashMap<>();
    private CloseNotice finalNotice = CloseNotice.disconnected(null);
    private @Nullable Throwable connectFailure;
    /**
     * 결과나 응답을 접수한 뒤에도 본문을 가진 출력이 아직 남아 있어 돌려주지 못한 바이트 예약. 스트림 번호별로 그 애플리케이션 프레임 출력이 다 나가거나(또는
     * 연결이 끊겨 출력이 사라지면) 돌려준다. 이벤트 루프에서만 읽고 쓴다.
     */
    private final Map<Long, HeldBytes> heldBytes = new HashMap<>();

    ConnectionEngine(NettyStrandConnection facade, ConnectionParams params, StrandChannel channel) {
        this.facade = facade;
        this.p = params;
        this.channel = channel;
        this.loop = params.loop;
        this.stage =
                params.role == ConnectionParams.Role.CLIENT ? PreparationStage.CONNECTION : PreparationStage.SECURITY;
        this.collectionSpace = CollectionSpace.of(
                ProgressSpace.eventSlots(usageLimit(Resource.RESULT_EVENT_BYTES), params.maxFrameSize),
                ProgressSpace.outputSlots(usageLimit(Resource.CONTROL_OUTPUT_BYTES)));
        // 기본 종료 알림 한 건의 자리를 연결을 만들 때 일반 관찰 한도와 별도로 확보한다. 일반 관찰이 포화여도 종료 알림은 등록된다.
        this.closed = new ResultCompletion<>(
                params.executor,
                params.budgets.observationBudget(),
                params.budgets.owner().observationBudget(),
                new ObservationBudget(DEFAULT_CLOSE_NOTICE_SLOTS));
        installChannel(channel);
    }

    /** 채널에 읽기 조각 설정과 연결 핸들러를 달고, 채널이 정리되면 연결 정리를 시작하게 한다. */
    private void installChannel(StrandChannel installed) {
        // 읽기 조각은 TLS 암호문 패킷 하나(받는 암호문 공간이 그 두 배) 또는 TCP 기본 조각이다. 한 번의 읽기 루프에서 한 조각만 받는다.
        int chunk = p.tls == null ? TCP_READ_CHUNK_BYTES : p.tls.sizes.packetBufferSize();
        installed.config().setAllocator(p.allocator);
        installed.config().setRecvByteBufAllocator(new FixedRecvByteBufAllocator(chunk));
        installed.pipeline().addLast(new Handler());
        installed.closeFuture().addListener(future -> {
            if (installed.equals(channel)) {
                onChannelClosed();
            }
        });
    }

    private long usageLimit(Resource resource) {
        return p.budgets.usage(resource).limit();
    }

    // ---------------------------------------------------------------- 시작

    /**
     * 클라이언트: 준비 기한 타이머를 시작하고, 필요하면 주소를 앱 실행기에서 해석한 뒤 주소를 하나씩 순서대로 연결한다. 모든 시도가 처음 정한 준비 기한을
     * 공유한다.
     */
    void startClient(InetSocketAddress target) {
        if (p.attempt != null) {
            p.attempt.bind(this);
        }
        armPreparationTimer();
        if (!target.isUnresolved()) {
            runOnLoop(() -> connectTo(java.util.Collections.singletonList(target), 0));
            return;
        }
        if (!p.budgets.owner().tryReserve(Resource.RESOLVE_TASKS, 1)) {
            setupFailed(new IOException(new ResourceExhaustedException(Resource.RESOLVE_TASKS)));
            return;
        }
        try {
            p.appExecutor.execute(() -> resolveThenConnect(target));
        } catch (RuntimeException | Error e) {
            p.budgets.owner().release(Resource.RESOLVE_TASKS, 1);
            setupFailed(e);
        }
    }

    /** 앱용 실행기에서 이름을 해석한다. 슬롯은 해석이 실제로 반환할 때까지 유지하고, 늦게 반환해도 이미 끝난 시도로는 연결하지 않는다. */
    private void resolveThenConnect(InetSocketAddress target) {
        InetAddress[] addresses = null;
        Throwable failure = null;
        try {
            addresses = p.resolver.resolve(target.getHostString());
        } catch (Throwable t) {
            failure = t;
        } finally {
            p.budgets.owner().release(Resource.RESOLVE_TASKS, 1);
        }
        if (failure != null) {
            setupFailed(failure);
            return;
        }
        List<InetSocketAddress> targets = new ArrayList<>();
        for (InetAddress each : addresses == null ? new InetAddress[0] : addresses) {
            targets.add(new InetSocketAddress(each, target.getPort()));
        }
        if (targets.isEmpty()) {
            setupFailed(new UnknownHostException(target.getHostString()));
            return;
        }
        long bytes = 0;
        for (InetSocketAddress each : targets) {
            bytes += each.getAddress().getAddress().length;
        }
        long reserve = bytes;
        runOnLoop(() -> connectTo(targets, reserve));
    }

    /** 시도가 이미 취소·기한·종료로 끝났는가. 끝난 시도는 새 소켓을 열지 않는다. */
    private boolean attemptEnded() {
        return connectFailure != null || tornDown.get();
    }

    /** 이벤트 루프: 해석한 주소의 바이트를 보관하기 전에 예약하고, 첫 주소부터 연결한다. */
    private void connectTo(List<InetSocketAddress> targets, long bytes) {
        if (attemptEnded()) {
            return;
        }
        PreparationBytes preparationBytes = p.preparationBytes;
        if (preparationBytes != null) {
            if (!preparationBytes.tryReserve(bytes)) {
                setupFailed(new ResourceExhaustedException(Resource.PREPARATION_BYTES));
                return;
            }
            resolvedBytes = bytes;
        }
        pendingTargets.addAll(targets);
        connectNext();
    }

    /** 이벤트 루프: 다음 주소 하나에 연결한다. 남은 준비 기한이 없으면 기한 초과로 끝낸다. */
    private void connectNext() {
        InetSocketAddress target = pendingTargets.remove();
        long remaining = p.preparationDeadlineAtNanos - p.nanoClock.getAsLong();
        if (remaining <= 0) {
            setupFailed(new PreparationDeadlineExceededException());
            return;
        }
        StrandChannel current = channel;
        current.config().setConnectTimeoutMillis((int) Math.min(remaining / NANOS_PER_MILLI + 1, Integer.MAX_VALUE));
        ChannelFuture registration;
        try {
            registration = loop.register(current);
        } catch (RuntimeException e) {
            setupFailed(e);
            return;
        }
        registration.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                setupFailed(future.cause());
            } else {
                current.connect(target).addListener((ChannelFutureListener) connected -> {
                    if (!connected.isSuccess()) {
                        connectFailed(current, connected.cause());
                    }
                });
            }
        });
    }

    /** 이벤트 루프: 한 주소의 연결이 실패했다. 남은 주소가 있고 시도가 끝나지 않았으면 새 채널로 다음 주소를 연결하고, 아니면 이 실패로 끝낸다. */
    @SuppressWarnings("FutureReturnValueIgnored") // 실패한 채널의 정리 완료는 관찰할 것이 없다.
    private void connectFailed(StrandChannel failed, Throwable cause) {
        if (attemptEnded()) {
            // 취소·기한이 먼저 끝냈다.
            return;
        }
        if (pendingTargets.isEmpty()) {
            setupFailed(cause);
            return;
        }
        StrandChannel next = new StrandChannel();
        next.config().setTcpNoDelay(true);
        // 교체를 먼저 한다. 실패한 채널의 정리가 연결 정리로 번지지 않는다.
        channel = next;
        installChannel(next);
        failed.close();
        connectNext();
    }

    /** 서버: 수락한 채널을 등록한다. 준비 기한은 수락부터 센다. */
    void startServer() {
        armPreparationTimer();
        ChannelFuture registration;
        try {
            registration = loop.register(channel);
        } catch (RuntimeException e) {
            setupFailed(e);
            return;
        }
        registration.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                setupFailed(future.cause());
            }
        });
    }

    /** 채널이 활성화되기 전의 실패(주소 해석·등록·연결). 원인을 남기고 채널을 정리한다. */
    private void setupFailed(Throwable cause) {
        Runnable fail = () -> {
            if (connectFailure == null) {
                connectFailure = cause;
                finalNotice = CloseNotice.disconnected(cause);
            }
            closeChannel();
        };
        if (loop.inEventLoop()) {
            // 채널 정리(closeFuture)보다 원인을 먼저 남겨야 준비 실패가 실제 원인을 담는다.
            fail.run();
        } else {
            runOnLoop(fail);
        }
    }

    /** 이벤트 루프에서 실행한다. 루프가 이미 끝났으면 호출 스레드에서 바로 실행한다(정리만 하는 작업이다). */
    private void runOnLoop(Runnable task) {
        try {
            loop.execute(task);
        } catch (RuntimeException e) {
            task.run();
        }
    }

    /** 채널이 연결되기 전에도 준비 기한이 지나면 연결 시도를 끝낸다. 활성화 뒤에는 연결 루프가 기한을 본다. */
    @SuppressWarnings("FutureReturnValueIgnored") // 타이머는 취소하지 않는다. 시각이 지난 뒤 상태를 다시 확인한다.
    private void armPreparationTimer() {
        long remaining = Math.max(0, p.preparationDeadlineAtNanos - p.nanoClock.getAsLong());
        try {
            loop.schedule(
                    () -> {
                        if (active == null && preparing) {
                            PreparationDeadlineExceededException deadline = new PreparationDeadlineExceededException();
                            setupFailed(deadline);
                        }
                    },
                    remaining / NANOS_PER_MILLI + 1,
                    TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            // EventLoop가 이미 끝났다. 채널 정리가 결과를 확정한다.
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored") // 정리 완료는 closeFuture 리스너가 관찰한다.
    private void closeChannel() {
        if (channel.isRegistered()) {
            channel.close();
        } else {
            // 등록한 적 없는 채널은 closeFuture가 끝나지 않는다. 같은 정리 경로를 직접 지난다.
            channel.unsafe().closeForcibly();
            onChannelClosed();
        }
    }

    // ---------------------------------------------------------------- 채널 이벤트 (이벤트 루프)

    private final class Handler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            onActive();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            try {
                if (message instanceof ByteBuf) {
                    onRead((ByteBuf) message);
                }
            } finally {
                ReferenceCountUtil.release(message);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 주소를 하나씩 시도하며 교체한 채널의 오류는 이 연결의 것이 아니다.
            if (ctx.channel().equals(channel)) {
                onException(cause);
            }
        }
    }

    private void onActive() {
        if (stopped || active != null) {
            return;
        }
        // 연결이 맺어졌으므로 해석한 주소 목록은 더 쓰지 않는다.
        pendingTargets.clear();
        releaseResolvedBytes();
        try {
            stage = p.tls != null ? PreparationStage.SECURITY : PreparationStage.HELLO;
            Loop created = new Loop();
            active = created;
            created.drive();
        } catch (IOException | RuntimeException e) {
            connectFailure = e;
            finalNotice = CloseNotice.disconnected(e);
            closeChannel();
        }
    }

    private void onRead(ByteBuf buffer) {
        Loop current = active;
        if (current == null || stopped) {
            return;
        }
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        current.accept(bytes);
    }

    private void onException(Throwable cause) {
        Loop current = active;
        if (current == null) {
            setupFailed(cause);
            return;
        }
        current.transportFailed(cause);
        current.drive();
        closeChannel();
    }

    /** 채널이 정리됐다(어떤 경로로든). 남은 결과와 예약을 정리하고, 이벤트 루프의 후속 작업을 거쳐 closed를 완료한다. */
    private void onChannelClosed() {
        if (!tornDown.compareAndSet(false, true)) {
            return;
        }
        Throwable fatal = null;
        try {
            Loop current = active;
            if (current != null) {
                current.channelClosed();
            }
        } catch (RuntimeException e) {
            fatal = e;
        } finally {
            stopped = true;
            try {
                abandonRemaining();
                failLeftovers();
                Throwable cause = fatal != null ? fatal : connectFailure;
                failPreparation(stage, cause != null ? cause : new IllegalStateException("연결이 준비 중에 끝났다"));
            } finally {
                runOnLoop(this::complete);
            }
        }
    }

    private void complete() {
        // 채널 정리(closeFuture)는 소켓이 실제로 풀리기 전에 끝날 수 있다. closed 완료는 실제 해제 뒤여야 하므로 JDK가 소켓을 놓을 때까지
        // 타이머로 선택 주기를 지나간다(선택기를 직접 부르지 않는다).
        if (channel.socketHeld() && !loop.isShuttingDown()) {
            try {
                // 이벤트 루프가 끝나면 Netty가 예약 작업을 취소한다. 그때는 기다릴 선택 주기가 없으니 바로 끝낸다.
                loop.schedule(this::complete, WRITE_RETRY_NANOS, TimeUnit.NANOSECONDS)
                        .addListener(timer -> {
                            if (timer.isCancelled()) {
                                finishClosed();
                            }
                        });
                return;
            } catch (RejectedExecutionException e) {
                // 이벤트 루프가 끝났다. 선택기가 닫히면 등록된 채널은 모두 풀린다.
            }
        }
        finishClosed();
    }

    private void finishClosed() {
        releaseSlots();
        completeReleased();
        closed.complete(finalNotice);
        p.terminated.run();
    }

    private void failLeftovers() {
        pending.values().forEach(entry -> entry.finish(Status.UNAVAILABLE, Certainty.UNKNOWN, EMPTY));
        pending.clear();
        new ArrayList<>(incoming.values())
                .forEach(request -> finishIncoming(request.state(), RequestEnd.connectionEnd()));
    }

    // ---------------------------------------------------------------- 연결 슬롯 예약

    /**
     * 연결을 받기 전에 준비·전체 연결 슬롯과 필수 진행 공간을 예약한다. 모자라면 확보한 것을 모두 돌려주고 모자란 종류를 알린다. 기다리지 않는다.
     *
     * @return 모두 예약했으면 null, 아니면 모자란 종류
     */
    static @Nullable Resource admit(ConnectionBudgets budgets, ProgressSpace space) {
        OwnerBudgets owner = budgets.owner();
        if (!owner.tryReserve(Resource.PREPARING_CONNECTIONS, 1)) {
            return Resource.PREPARING_CONNECTIONS;
        }
        if (!owner.tryReserve(Resource.TOTAL_CONNECTIONS, 1)) {
            owner.release(Resource.PREPARING_CONNECTIONS, 1);
            return Resource.TOTAL_CONNECTIONS;
        }
        Resource failed = reserveProgress(budgets, space);
        if (failed != null) {
            owner.release(Resource.TOTAL_CONNECTIONS, 1);
            owner.release(Resource.PREPARING_CONNECTIONS, 1);
        }
        return failed;
    }

    private static Resource[] progressKinds() {
        return new Resource[] {
            Resource.FRAME_ASSEMBLY_BYTES,
            Resource.CONTROL_OUTPUT_BYTES,
            Resource.RESULT_EVENT_BYTES,
            Resource.TLS_PLAINTEXT_BYTES,
            Resource.TLS_CIPHERTEXT_BYTES,
            Resource.TLS_CONTROL_BYTES
        };
    }

    private static long[] progressAmounts(ProgressSpace space) {
        return new long[] {
            space.frameAssembly(),
            space.controlOutput(),
            space.resultEvent(),
            space.tlsPlaintext(),
            space.tlsCiphertext(),
            space.tlsControl()
        };
    }

    private static @Nullable Resource reserveProgress(ConnectionBudgets budgets, ProgressSpace space) {
        Resource[] kinds = progressKinds();
        long[] amounts = progressAmounts(space);
        for (int i = 0; i < kinds.length; i++) {
            if (!budgets.tryReserve(kinds[i], amounts[i])) {
                releaseProgress(budgets, space, i);
                return kinds[i];
            }
        }
        return null;
    }

    /** {@link #admit}로 예약했지만 연결을 시작하지 못했을 때 예약을 모두 돌려준다. */
    static void cancelAdmission(ConnectionBudgets budgets, ProgressSpace space) {
        releaseProgress(budgets, space, progressKinds().length);
        budgets.owner().release(Resource.TOTAL_CONNECTIONS, 1);
        budgets.owner().release(Resource.PREPARING_CONNECTIONS, 1);
    }

    /** 진행 공간 예약 가운데 앞의 {@code count}종류를 돌려준다. 예약 도중 모자라 되돌릴 때와 연결이 끝날 때 쓴다. */
    private static void releaseProgress(ConnectionBudgets budgets, ProgressSpace space, int count) {
        Resource[] kinds = progressKinds();
        long[] amounts = progressAmounts(space);
        for (int i = 0; i < count; i++) {
            budgets.release(kinds[i], amounts[i]);
        }
    }

    private void releaseResolvedBytes() {
        PreparationBytes preparationBytes = p.preparationBytes;
        if (preparationBytes != null && resolvedBytes > 0) {
            preparationBytes.release(resolvedBytes);
            resolvedBytes = 0;
        }
    }

    private static final class HeldBytes {
        final Resource resource;
        final long amount;

        HeldBytes(Resource resource, long amount) {
            this.resource = resource;
            this.amount = amount;
        }
    }

    /** 스트림의 애플리케이션 프레임 출력이 사라질 때까지 바이트 예약을 유지한다. 0바이트는 유지할 것이 없다. */
    private void holdBytes(long streamId, Resource resource, long amount) {
        if (amount > 0) {
            heldBytes.put(streamId, new HeldBytes(resource, amount));
        }
    }

    /** 스트림의 출력이 모두 사라졌다. 그 출력 때문에 유지하던 바이트 예약을 돌려준다. */
    private void returnHeldBytes(long streamId) {
        HeldBytes held = heldBytes.remove(streamId);
        if (held != null) {
            p.budgets.release(held.resource, held.amount);
        }
    }

    /** 출력이 모두 사라졌다(연결이 끊겼거나 마무리됐다). 남은 출력 때문에 유지하던 바이트 예약을 모두 돌려준다. */
    private void returnAllHeldBytes() {
        for (HeldBytes held : heldBytes.values()) {
            p.budgets.release(held.resource, held.amount);
        }
        heldBytes.clear();
    }

    private void releaseSlots() {
        returnAllHeldBytes();
        if (p.preparationBytes != null) {
            p.preparationBytes.releaseAll();
        }
        releaseProgress(p.budgets, p.space, progressKinds().length);
        p.budgets.owner().release(Resource.TOTAL_CONNECTIONS, 1);
        releasePreparingSlot();
    }

    private void releasePreparingSlot() {
        if (preparingSlotHeld) {
            preparingSlotHeld = false;
            p.budgets.owner().release(Resource.PREPARING_CONNECTIONS, 1);
        }
    }

    // ---------------------------------------------------------------- 앱 스레드에서 부르는 경계

    ResourceUsage usage(Resource resource) {
        return p.budgets.usage(resource);
    }

    Result<CloseNotice> closed() {
        return closed;
    }

    void close() {
        if (closeSubmitted.compareAndSet(false, true)) {
            submit(new EndpointCall(endpoint -> endpoint.close(NO_ERROR)));
        }
    }

    void expireCredential() {
        requireServer("expireCredential");
        submit(new EndpointCall(Endpoint::credentialExpired));
    }

    void revokeCredential() {
        requireServer("revokeCredential");
        submit(new EndpointCall(Endpoint::credentialRevoked));
    }

    void closeExcessiveUse() {
        submit(new EndpointCall(endpoint -> endpoint.close(EXCESSIVE_USE)));
    }

    private void requireServer(String operation) {
        if (p.role != ConnectionParams.Role.SERVER) {
            throw new IllegalStateException(operation + "은 서버 연결에서만 부를 수 있다");
        }
    }

    void abort() {
        abortRequested = true;
        wake();
    }

    /**
     * 전체 종료가 이 연결에 CLOSE를 시작시킨다. 아직 확정되지 않은 연결 시도는 먼저 취소한다. 채널이 활성화되기 전에 낸 명령은 연결 루프가 만들어질 때 처리되고, 활성화되지 못하는
     * 시도는 취소가 끝낸다.
     */
    void shutdownClose() {
        if (p.attempt != null) {
            p.attempt.cancel();
        }
        close();
    }

    /** 전체 종료의 grace가 끝나 이 연결을 즉시 끊는다. 아직 확정되지 않은 연결 시도는 먼저 취소한다. */
    void shutdownAbort() {
        if (p.attempt != null) {
            p.attempt.cancel();
        }
        abort();
    }

    /**
     * 연결 시도 취소를 EventLoop에 넘긴다. 성공과 취소 가운데 이기는 쪽은 EventLoop가 처리하는 순서가 정한다. 채널이 활성화되기 전(주소 해석·연결 중)이면 곧바로 채널을
     * 정리하고, 활성화된 뒤에는 연결 루프의 명령으로 준비를 취소로 확정한다.
     */
    void cancelAttempt() {
        runOnLoop(() -> {
            if (!preparing) {
                return;
            }
            if (active == null) {
                setupFailed(new ConnectionAttemptCancelledException());
            } else {
                submit(new CancelAttemptCommand());
            }
        });
    }

    RequestHandle request(Request request, RequestOptions options) {
        byte[] extensions = request.extensions().toWire();
        long bytes = (long) request.payloadLength() + extensions.length;
        long deadlineAt = options.deadline().isPresent()
                ? deadlineAfter(options.deadline().get())
                : NO_DEADLINE;
        ResultCompletion<RequestResult> result = new ResultCompletion<>(
                p.executor, p.budgets.observationBudget(), p.budgets.owner().observationBudget());
        if (stopped) {
            OutgoingEntry rejected = new OutgoingEntry(result, request.type(), EMPTY, EMPTY, bytes, deadlineAt);
            rejected.finishLocally(Status.UNAVAILABLE, Certainty.NOT_PROCESSED, null);
            return new Handle(rejected);
        }
        // 본문을 복사하기 전에 연결·소유 객체 범위의 예약을 확보한다. 모자라면 복사하지 않고 처리 안 됨으로 끝낸다.
        Resource exhausted = reserveOutgoing(bytes);
        if (exhausted != null) {
            OutgoingEntry rejected = new OutgoingEntry(result, request.type(), EMPTY, EMPTY, bytes, deadlineAt);
            rejected.finishLocally(0, null, exhausted);
            return new Handle(rejected);
        }
        OutgoingEntry entry =
                new OutgoingEntry(result, request.type(), extensions, request.payload(), bytes, deadlineAt);
        entry.reserved = true;
        submit(new RequestCommand(entry));
        return new Handle(entry);
    }

    private long deadlineAfter(java.time.Duration duration) {
        long nanos;
        try {
            nanos = duration.toNanos();
        } catch (ArithmeticException e) {
            nanos = Long.MAX_VALUE;
        }
        return Saturating.add(p.nanoClock.getAsLong(), nanos);
    }

    /** 송신 요청 수와 바이트를 연결·소유 객체 범위에 함께 예약한다. 하나라도 모자라면 확보한 것을 돌려준다. */
    private @Nullable Resource reserveOutgoing(long bytes) {
        if (!p.budgets.tryReserve(Resource.OUTGOING_REQUESTS, 1)) {
            return Resource.OUTGOING_REQUESTS;
        }
        if (!p.budgets.tryReserve(Resource.OUTGOING_BYTES, bytes)) {
            p.budgets.release(Resource.OUTGOING_REQUESTS, 1);
            return Resource.OUTGOING_BYTES;
        }
        return null;
    }

    private void releaseOutgoing(long bytes) {
        p.budgets.release(Resource.OUTGOING_BYTES, bytes);
        p.budgets.release(Resource.OUTGOING_REQUESTS, 1);
    }

    Result<ResponseAcceptance> respond(IncomingRequest request, int status, byte[] body) {
        IncomingState state = request.state();
        ResultCompletion<ResponseAcceptance> result = new ResultCompletion<>(
                p.executor, p.budgets.observationBudget(), p.budgets.owner().observationBudget());
        // 재시도마다 끝났는지·이미 응답했는지를 자원 부족보다 먼저 판정한다.
        ResponseAcceptance ended = endedAcceptance(state);
        if (ended != null) {
            result.complete(ended);
            return result;
        }
        // 응답 접수 명령은 요청마다 하나만 기다린다. 본문이 비어 예약이 0이어도 명령 대기가 예약 없이 늘지 않게 한다.
        if (!state.responding.compareAndSet(false, true)) {
            result.complete(ResponseAcceptance.resourceExhausted(Resource.RESPONSE_BYTES));
            return result;
        }
        if (!p.budgets.tryReserve(Resource.RESPONSE_BYTES, body.length)) {
            state.responding.set(false);
            // 부족을 알리는 사이 요청이 끝났다면 끝남이 우선이다.
            ResponseAcceptance nowEnded = endedAcceptance(state);
            result.complete(
                    nowEnded != null ? nowEnded : ResponseAcceptance.resourceExhausted(Resource.RESPONSE_BYTES));
            return result;
        }
        submit(new RespondCommand(state, status, body.clone(), result));
        return result;
    }

    /** 요청이 이미 끝났으면 그에 맞는 접수 결과를, 아직 살아 있으면 null을 돌려준다. */
    private static @Nullable ResponseAcceptance endedAcceptance(IncomingState state) {
        if (!state.ended.isDone()) {
            return null;
        }
        return state.ended.resultNow().isResponseAccepted()
                ? ResponseAcceptance.alreadyResponded()
                : ResponseAcceptance.requestEnded();
    }

    private final class Handle implements RequestHandle {
        private final OutgoingEntry entry;
        private final AtomicBoolean cancelSubmitted = new AtomicBoolean();
        private final LoopBoundResult result;

        Handle(OutgoingEntry entry) {
            this.entry = entry;
            this.result = new LoopBoundResult(entry.result, loop);
        }

        @Override
        public Result<RequestResult> result() {
            return result;
        }

        @Override
        public void cancel() {
            if (!entry.result.isDone() && cancelSubmitted.compareAndSet(false, true)) {
                submit(new CancelCommand(entry));
            }
        }
    }

    // ---------------------------------------------------------------- 명령

    private interface Command {
        void execute(Loop loop);

        /** 전송 스레드가 끝난 뒤 들어왔거나 끝나기 전에 처리되지 못한 명령. */
        void abandon();
    }

    private static final class EndpointCall implements Command {
        private final Consumer<Endpoint> call;

        EndpointCall(Consumer<Endpoint> call) {
            this.call = call;
        }

        @Override
        public void execute(Loop loop) {
            call.accept(loop.endpoint);
        }

        @Override
        public void abandon() {}
    }

    private final class RequestCommand implements Command {
        private final OutgoingEntry entry;

        RequestCommand(OutgoingEntry entry) {
            this.entry = entry;
        }

        @Override
        public void execute(Loop loop) {
            loop.advanceClock();
            long remainingMs = 0;
            if (entry.deadlineAtNanos != NO_DEADLINE) {
                long remaining = entry.deadlineAtNanos - p.nanoClock.getAsLong();
                if (remaining <= 0) {
                    // 호출 시점부터 센 기한이 core 등록 전 대기에서 이미 지났다. 요청을 만들지 않는다(처리 안 됨).
                    entry.finish(Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED, EMPTY);
                    return;
                }
                // 올림이 아니라 항상 1밀리초를 더해 기한이 앞서 지나지 않게 한다.
                remainingMs = remaining / NANOS_PER_MILLI + 1;
            }
            try {
                long ticket = loop.endpoint.request(entry.type, entry.extensions, entry.payload, remainingMs);
                entry.releaseBody();
                entry.ticket = ticket;
                pending.put(ticket, entry);
            } catch (IllegalArgumentException e) {
                // 협상한 프레임 크기를 넘는 요청. 아무것도 보내지 않았다.
                entry.finishLocally(0, null, Resource.PEER_FRAME_SIZE);
            }
        }

        @Override
        public void abandon() {
            entry.finishLocally(Status.UNAVAILABLE, Certainty.NOT_PROCESSED, null);
        }
    }

    private static final class CancelCommand implements Command {
        private final ConnectionEngine.OutgoingEntry entry;

        CancelCommand(ConnectionEngine.OutgoingEntry entry) {
            this.entry = entry;
        }

        @Override
        public void execute(Loop loop) {
            // 끝난 요청이나 번호표를 받지 못한 요청(로컬에서 이미 끝남)의 취소는 core가 무시한다.
            loop.endpoint.cancelRequest(entry.ticket);
        }

        @Override
        public void abandon() {}
    }

    private final class RespondCommand implements Command {
        private final IncomingState state;
        private final int status;
        private final byte[] body;
        private final ResultCompletion<ResponseAcceptance> result;

        RespondCommand(IncomingState state, int status, byte[] body, ResultCompletion<ResponseAcceptance> result) {
            this.state = state;
            this.status = status;
            this.body = body;
            this.result = result;
        }

        @Override
        public void execute(Loop loop) {
            ResponseAcceptance acceptance;
            boolean accepted = false;
            try {
                if (state.responded) {
                    // 응답을 접수한 요청은 그 응답 때문에 끝난 것이다. 뒤따르는 응답은 이미 응답한 것으로 알린다.
                    acceptance = ResponseAcceptance.alreadyResponded();
                } else if (loop.endpoint.respond(state.streamId, status, body)) {
                    state.responded = true;
                    accepted = true;
                    acceptance = ResponseAcceptance.accepted();
                } else {
                    // 상대가 RESET했거나 연결이 끝난 스트림이다. 응답은 접수하지 않았다.
                    acceptance = ResponseAcceptance.requestEnded();
                    returnIncoming(state);
                }
            } catch (IllegalArgumentException e) {
                // 협상한 프레임 크기를 넘는 응답. 본문을 보관하지 않고 응답 권한도 소비하지 않는다.
                acceptance = ResponseAcceptance.resourceExhausted(Resource.PEER_FRAME_SIZE);
            }
            if (accepted) {
                // 응답 프레임이 core 출력으로 쌓였다. 본문 참조는 그 프레임이 다 나가거나 연결이 끝날 때까지 남으므로 예약도 그때 돌려준다.
                holdBytes(state.streamId, Resource.RESPONSE_BYTES, body.length);
            } else {
                p.budgets.release(Resource.RESPONSE_BYTES, body.length);
            }
            // 요청 종료와 대기 표시 해제를 결과보다 먼저 보존해, 결과를 본 앱이 곧바로 다시 응답하면 이미 응답한 것으로 판정된다.
            state.responding.set(false);
            if (accepted) {
                finishIncoming(state, RequestEnd.responseAccepted());
            }
            result.complete(acceptance);
        }

        @Override
        public void abandon() {
            p.budgets.release(Resource.RESPONSE_BYTES, body.length);
            state.responding.set(false);
            result.complete(ResponseAcceptance.requestEnded());
        }
    }

    private final class CancelAttemptCommand implements Command {
        @Override
        public void execute(Loop loop) {
            if (preparing) {
                ConnectionAttemptCancelledException cancelled = new ConnectionAttemptCancelledException();
                loop.endCause = cancelled;
                failPreparation(stage, cancelled);
                loop.endpoint.disconnect();
            }
        }

        @Override
        public void abandon() {}
    }

    private final class AuthenticationCompleteCommand implements Command {
        private final long id;
        private final AuthenticationResult result;
        private final @Nullable Throwable failure;

        AuthenticationCompleteCommand(long id, @Nullable AuthenticationResult result, @Nullable Throwable failure) {
            this.id = id;
            this.result = result == null ? AuthenticationResult.unavailable() : result;
            this.failure = failure;
        }

        @Override
        public void execute(Loop loop) {
            if (failure != null) {
                loop.endpoint.completeAuthentication(id, AuthDecision.TRANSIENT_FAILURE);
                failPreparation(PreparationStage.AUTHENTICATION, failure);
                return;
            }
            switch (result.kind()) {
                case ACCEPTED:
                    loop.endpoint.completeAuthentication(id, AuthDecision.PASS, result.lifetime());
                    facade.authenticated(
                            new AuthenticatedPeer(Objects.requireNonNull(result.principal()), p.remoteAddress));
                    startFactory(loop);
                    break;
                case REJECTED:
                    loop.endpoint.completeAuthentication(id, AuthDecision.REJECT);
                    failPreparation(PreparationStage.AUTHENTICATION, new AuthenticationRejectedException());
                    break;
                default:
                    loop.endpoint.completeAuthentication(id, AuthDecision.TRANSIENT_FAILURE);
                    failPreparation(PreparationStage.AUTHENTICATION, new IllegalStateException("인증 서비스를 쓸 수 없다"));
                    break;
            }
        }

        @Override
        public void abandon() {}
    }

    private final class FactoryCompleteCommand implements Command {
        private final @Nullable PreparedHandler prepared;
        private final @Nullable Throwable failure;

        FactoryCompleteCommand(@Nullable PreparedHandler prepared, @Nullable Throwable failure) {
            this.prepared = prepared;
            this.failure = failure;
        }

        @Override
        public void execute(Loop loop) {
            PreparedHandler handler = prepared;
            if (handler == null) {
                p.budgets.owner().release(Resource.FACTORY_TASKS, 1);
                Throwable cause = failure == null ? new IllegalStateException("팩터리가 핸들러를 돌려주지 않았다") : failure;
                failPreparation(PreparationStage.HANDLER_FACTORY, cause);
                loop.endpoint.close(INTERNAL_ERROR);
                return;
            }
            if (!preparing || loop.closedSeen) {
                // 활성화되지 못한 결과다. 폐기가 기존 팩터리 예약을 이어받아 끝날 때 돌려준다.
                dispose(handler);
                return;
            }
            p.budgets.owner().release(Resource.FACTORY_TASKS, 1);
            activate(loop, handler.handler());
        }

        @Override
        public void abandon() {
            PreparedHandler handler = prepared;
            if (handler == null) {
                p.budgets.owner().release(Resource.FACTORY_TASKS, 1);
            } else {
                dispose(handler);
            }
        }
    }

    /**
     * 활성화되지 못한 결과의 폐기를 앱용 실행기에 넘긴다. 폐기는 이 연결의 직렬 실행을 거치지 않는다: 같은 연결의 반환하지 않는 사용자 작업 뒤에서 폐기가 막히지
     * 않고, 연결이 이미 끝났어도 실행된다.
     */
    private void dispose(PreparedHandler handler) {
        try {
            p.appExecutor.execute(() -> {
                try {
                    handler.dispose();
                } finally {
                    p.budgets.owner().release(Resource.FACTORY_TASKS, 1);
                }
            });
        } catch (RuntimeException | Error e) {
            // 폐기 실행이 거절됐다. 폐기 동작을 EventLoop에서 대신 실행하지 않고 앱이 조회·회수하도록 보존한다. 예약은 앱이 인수할 때까지 유지한다.
            UnreleasedHandlers registry = Objects.requireNonNull(p.unreleased);
            registry.add(new UnreleasedHandler(
                    handler, registry, () -> p.budgets.owner().release(Resource.FACTORY_TASKS, 1)));
        }
    }

    private void submit(Command command) {
        commands.add(command);
        wake();
        if (stopped) {
            abandonRemaining();
        }
    }

    private void abandonRemaining() {
        Command command = commands.poll();
        while (command != null) {
            command.abandon();
            command = commands.poll();
        }
    }

    /** 이벤트 루프를 깨워 명령을 처리하게 한다. 이미 깨우는 작업이 대기 중이면 합친다(이벤트 루프 작업 큐도 유한하게 유지한다). */
    private void wake() {
        if (!wakeQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            loop.execute(() -> {
                wakeQueued.set(false);
                Loop current = active;
                if (current != null) {
                    current.drive();
                }
            });
        } catch (RuntimeException e) {
            // EventLoop가 끝났다. 처리할 수 없는 명령은 내려놓는다.
            wakeQueued.set(false);
            abandonRemaining();
        }
    }

    // ---------------------------------------------------------------- 준비

    /** 준비 결과를 실패로 확정한다. 처음 한 번만 알리고 준비 슬롯을 돌려준다. 이미 확정됐으면 아무 일도 하지 않는다. */
    private void failPreparation(PreparationStage at, Throwable cause) {
        if (preparing) {
            preparing = false;
            confirmAttempt();
            releasePreparingSlot();
            PreparationFailure failure = new PreparationFailure(p.remoteAddress, at, cause);
            notifyFailed(failure);
        }
    }

    private void becomeReady(Loop loop) {
        if (preparing) {
            preparing = false;
            confirmAttempt();
            releasePreparingSlot();
            // 성공한 시도는 채널을 연결에 넘겼으므로 시도가 정리할 것이 없다.
            completeReleased();
            notifyReady(loop);
        }
    }

    private void confirmAttempt() {
        if (p.attempt != null) {
            p.attempt.confirm();
        }
    }

    /**
     * 시도가 소유한 채널 정리 완료를 알린다. 알림은 앱용 실행기를 거치며, 거절되면 실행기 없이 확정한다(Netty Future의 리스너는 어차피 EventLoop에서 실행된다).
     */
    private void completeReleased() {
        io.netty.util.concurrent.Promise<Void> released = p.released;
        if (released == null || releasedNotified) {
            return;
        }
        releasedNotified = true;
        try {
            p.appExecutor.execute(() -> released.trySuccess(null));
        } catch (RuntimeException | Error e) {
            released.trySuccess(null);
        }
    }

    /**
     * 준비 성공 알림은 앱용 실행기에서 한다. 같은 연결의 인증·팩터리가 반환하지 않아도 알릴 수 있도록 연결별 직렬 실행을 거치지 않는다. 실행기가 거절하면
     * 알림을 대신할 스레드를 만들지 않고, 앱이 받을 수 없는 연결이므로 끊고 시도를 거절 원인으로 실패시킨다. 서버는 성공을 알릴 앱 관찰자가 없어 거절돼도 연결을 유지한다.
     */
    private void notifyReady(Loop loop) {
        try {
            p.appExecutor.execute(() -> p.observer.ready(facade));
        } catch (RuntimeException | Error e) {
            if (p.role == ConnectionParams.Role.CLIENT) {
                // 같은 처리 단계 안에서 즉시 끊는다. 남은 수집 사건과 입력은 처리하지 않아 실패로 확정된 연결에서 핸들러가 실행되지 않는다.
                loop.abandon();
                p.observer.rejected(new PreparationFailure(p.remoteAddress, stage, e));
            }
        }
    }

    /** 준비 실패 알림도 앱용 실행기에서 한다. 거절되면 원래 실패에 거절을 덧붙여 확정한다(Netty Future의 리스너는 EventLoop에서 실행된다). */
    private void notifyFailed(PreparationFailure failure) {
        try {
            p.appExecutor.execute(() -> p.observer.failed(failure));
        } catch (RuntimeException | Error e) {
            try {
                failure.cause().addSuppressed(e);
            } catch (IllegalArgumentException sameAsCause) {
                // 거절 예외가 원인과 같은 객체면 덧붙일 것이 없다.
            }
            p.observer.rejected(failure);
        }
    }

    private void activate(Loop loop, @Nullable RequestHandler handler) {
        activeHandler = handler;
        loop.endpoint.handlerReady();
        becomeReady(loop);
    }

    private void startFactory(Loop loop) {
        ConnectionHandlerFactory factory = p.factory;
        if (factory == null) {
            activate(loop, p.handler);
            return;
        }
        stage = PreparationStage.HANDLER_FACTORY;
        if (!p.budgets.owner().tryReserve(Resource.FACTORY_TASKS, 1)) {
            failPreparation(PreparationStage.HANDLER_FACTORY, new ResourceExhaustedException(Resource.FACTORY_TASKS));
            loop.endpoint.close(INTERNAL_ERROR);
            return;
        }
        try {
            p.executor.execute(() -> runFactory(factory));
        } catch (RuntimeException | Error e) {
            p.budgets.owner().release(Resource.FACTORY_TASKS, 1);
            failPreparation(PreparationStage.HANDLER_FACTORY, e);
            loop.endpoint.close(INTERNAL_ERROR);
        }
    }

    private void runFactory(ConnectionHandlerFactory factory) {
        PreparedHandler prepared = null;
        Throwable failure = null;
        try {
            prepared = factory.create(facade);
        } catch (Throwable t) {
            failure = t;
        }
        submit(new FactoryCompleteCommand(prepared, failure));
    }

    private void runAuthentication(long id, Credentials credentials, CredentialVerifier verifier, long retained) {
        AuthenticationResult result = null;
        Throwable failure = null;
        try {
            result = verifier.verify(credentials);
        } catch (Throwable t) {
            failure = t;
        } finally {
            p.budgets.owner().release(Resource.AUTHENTICATION_TASKS, 1);
            p.budgets.release(Resource.PREPARATION_BYTES, retained);
        }
        submit(new AuthenticationCompleteCommand(id, result, failure));
    }

    // ---------------------------------------------------------------- 받은 요청과 알림 (전송 스레드)

    private void onRequestReceived(Loop loop, Event event) {
        long streamId = event.streamId();
        byte[] body = event.body();
        byte[] wireExtensions = event.extensions();
        RequestHandler handler = activeHandler;
        if (handler == null) {
            // 핸들러를 등록하지 않은 쪽이 받은 요청은 구현하지 않은 것으로 답한다.
            loop.endpoint.respond(streamId, UNIMPLEMENTED, EMPTY);
            return;
        }
        long bytes = (long) body.length + wireExtensions.length;
        if (!reserveIncoming(bytes)) {
            // 수신 부족은 핸들러 시작 전 REFUSED다.
            loop.endpoint.refuseRequest(streamId);
            return;
        }
        ResultCompletion<RequestEnd> ended = new ResultCompletion<>(
                p.executor, p.budgets.observationBudget(), p.budgets.owner().observationBudget());
        IncomingState state = new IncomingState(streamId, bytes, ended);
        IncomingRequest request = new IncomingRequest(
                this, facade, state, event.type(), Extensions.fromWire(wireExtensions), body, ended);
        incoming.put(streamId, request);
        try {
            p.executor.execute(() -> runHandler(request, handler));
        } catch (RuntimeException | Error e) {
            // 실행 거절은 핸들러 시작 전 REFUSED다. 작업이 본문을 쥐지 않았으므로 바이트도 함께 돌아온다.
            incoming.remove(streamId);
            state.handlerSettled();
            returnIncoming(state);
            p.budgets.release(Resource.HANDLER_TASKS, 1);
            loop.endpoint.refuseRequest(streamId);
        }
    }

    /**
     * 상대가 받은 요청을 RESET했다. 요청이 끝났음을 오류 코드와 함께 보존해 핸들러가 처리를 멈출 수 있게 하고(규격 4.4절), 실행 대기 중이던 핸들러는
     * 시작하지 않는다. 이미 응답을 접수했거나 끝난 요청이면 core가 이 사건을 내지 않는다.
     */
    private void onRequestReset(Event event) {
        IncomingRequest request = incoming.get(event.streamId());
        if (request != null) {
            finishIncoming(
                    request.state(), RequestEnd.peerReset(event.resetErrorCode().getAsLong()));
        }
    }

    /** 받는 요청 수·바이트·핸들러 작업 슬롯을 예약한다. 하나라도 모자라면 확보한 것을 돌려주고 false. */
    private boolean reserveIncoming(long bytes) {
        if (!p.budgets.tryReserve(Resource.INCOMING_REQUESTS, 1)) {
            return false;
        }
        if (!p.budgets.tryReserve(Resource.INCOMING_BYTES, bytes)) {
            p.budgets.release(Resource.INCOMING_REQUESTS, 1);
            return false;
        }
        if (!p.budgets.tryReserve(Resource.HANDLER_TASKS, 1)) {
            p.budgets.release(Resource.INCOMING_BYTES, bytes);
            p.budgets.release(Resource.INCOMING_REQUESTS, 1);
            return false;
        }
        return true;
    }

    /** 요청이 끝났다: 요청 수 예약을 돌려주고, 핸들러 작업도 참조를 놓았으면 바이트 예약도 돌려준다. */
    private void returnIncoming(IncomingState state) {
        if (state.claimCountReturn()) {
            p.budgets.release(Resource.INCOMING_REQUESTS, 1);
        }
        returnIncomingBytes(state);
    }

    private void returnIncomingBytes(IncomingState state) {
        if (state.claimBytesReturn()) {
            p.budgets.release(Resource.INCOMING_BYTES, state.reservedBytes);
        }
    }

    private void finishIncoming(IncomingState state, RequestEnd end) {
        incoming.remove(state.streamId);
        returnIncoming(state);
        state.ended.complete(end);
    }

    /** 앱용 실행기에서 도는 핸들러 호출. 실행 대기 중 끝난 요청은 시작하지 않는다. */
    private void runHandler(IncomingRequest request, RequestHandler handler) {
        try {
            if (request.ended().isDone() || stopped) {
                return;
            }
            handler.handle(request);
        } catch (Throwable t) {
            // 아직 살아 있고 응답 전이면 빈 INTERNAL 응답을 시도한다. 예외 메시지는 상대에게 보내지 않고 연결은 유지한다. 원인은 로컬 진단에만 남긴다.
            diagnoseHandlerFailure(request, t);
            request.respond(Status.INTERNAL, EMPTY);
        } finally {
            request.state().handlerSettled();
            returnIncomingBytes(request.state());
            p.budgets.release(Resource.HANDLER_TASKS, 1);
        }
    }

    /** 핸들러 예외를 로컬 진단으로 남긴다. 상대에게는 보내지 않는다. 진단 실패가 요청 처리를 바꾸지 않는다. */
    private void diagnoseHandlerFailure(IncomingRequest request, Throwable failure) {
        try {
            LOGGER.log(
                    Level.WARNING,
                    "받은 요청 핸들러가 예외를 던졌다(type=" + request.type() + ", 상대=" + p.remoteAddress + ")",
                    failure);
        } catch (RuntimeException ignored) {
            // 로그 처리기 오류가 빈 INTERNAL 응답과 슬롯 반환을 막지 않는다.
        }
    }

    // ---------------------------------------------------------------- 연결 루프 (이벤트 루프)

    /** 이벤트 루프 안에서만 쓰는 상태와 구동 순서. */
    private final class Loop {
        final Endpoint endpoint;
        final @Nullable TlsPipe tls;
        final long origin;
        long lastElapsed;
        boolean finished;
        private boolean abandoned;
        boolean closedSeen;
        private boolean transportGone;
        private boolean outputShutdown;
        private boolean closeNotifyStarted;
        private boolean blocked;
        private boolean reading = true;
        private long finishDeadline;
        private @Nullable Throwable ioFailure;
        private @Nullable Throwable endCause;
        private final ArrayDeque<byte[]> inputQueue = new ArrayDeque<>();
        private byte[] input = EMPTY;
        private int inputPos;
        private @Nullable EndpointOutput writing;
        private byte[] writingBytes = EMPTY;
        private int writingOffset;
        private @Nullable ByteBuf writingBuffer;
        private @Nullable ScheduledFuture<?> timer;
        private long timerAtNanos;

        Loop() throws SSLException {
            this.origin = p.nanoClock.getAsLong();
            this.endpoint = p.role == ConnectionParams.Role.CLIENT
                    ? Endpoint.client(p.config)
                    : Endpoint.serverWithDeferredAuthentication(p.config);
            ConnectionParams.Tls security = p.tls;
            if (security == null) {
                this.tls = null;
            } else {
                SSLEngine engine = p.role == ConnectionParams.Role.CLIENT
                        ? TlsEngines.client(
                                security.context, Objects.requireNonNull(security.clientHost), security.clientPort)
                        : TlsEngines.server(security.context);
                this.tls = new TlsPipe(engine, security.sizes, p.allocator, channel::writeAttempt);
            }
        }

        /** 받은 조각을 입력 대기열에 올리고 한 번 구동한다. 대기열은 한 번의 읽기 조각(TLS는 그 복호 결과)만큼만 쌓인다. */
        void accept(byte[] bytes) {
            try {
                if (tls != null) {
                    tls.feed(bytes);
                    moveDecrypted();
                } else {
                    inputQueue.add(bytes);
                }
            } catch (IOException e) {
                transportFailed(e);
            }
            drive();
        }

        private void moveDecrypted() {
            byte[] plain = Objects.requireNonNull(tls).pollPlain();
            while (plain != null) {
                inputQueue.add(plain);
                plain = tls.pollPlain();
            }
        }

        /** 한 번의 구동: 입력 처리, 시계 진행, 명령, 사건, 쓰기, 마무리. 이벤트 루프 작업 하나 안에서 끝난다. */
        void drive() {
            if (stopped) {
                return;
            }
            try {
                step();
            } catch (IOException e) {
                transportFailed(e);
                collectEvents();
            } catch (RuntimeException e) {
                abort(e);
            }
            if (finished) {
                cancelTimer();
                closeChannel();
            } else {
                updateRead();
                scheduleTimer();
            }
        }

        private void step() throws IOException {
            if (tls != null) {
                tls.pump();
                moveDecrypted();
                if (tls.peerEnded() && inputQueue.isEmpty() && inputPos >= input.length) {
                    transportFailed(new EOFException("상대가 TLS를 끝냈다"));
                }
            }
            processInput();
            advanceClock();
            runCommands();
            if (abortRequested) {
                endpoint.disconnect();
                finished = true;
            }
            checkPreparation();
            collectEvents();
            writeOutputs();
            collectEvents();
            blocked = writing != null || endpoint.nextOutput() != null || (tls != null && tls.pendingOutput());
            finishClosing();
        }

        void advanceClock() {
            long elapsed = Math.max(lastElapsed, p.nanoClock.getAsLong() - origin);
            lastElapsed = elapsed;
            endpoint.advanceTo(elapsed);
        }

        private void runCommands() {
            Command command = commands.poll();
            while (command != null) {
                command.execute(this);
                command = commands.poll();
            }
        }

        /** 읽기를 켜고 끈다: 처리하지 못한 입력이 남았거나 core 출력이 상한을 넘으면 멈춘다. */
        private void updateRead() {
            boolean want = !transportGone
                    && inputQueue.isEmpty()
                    && inputPos >= input.length
                    && (tls == null || tls.canAcceptRead())
                    && !backlogged();
            if (want != reading) {
                reading = want;
                channel.config().setAutoRead(want);
            }
        }

        /** 남은 출력이 상한을 넘으면 읽기를 멈춘다(상대가 읽지 않으면서 계속 보내는 경우 core 출력이 끝없이 늘지 않게 한다). */
        private boolean backlogged() {
            return endpoint.pendingOutputBytes()
                    > writeQueueLimit(
                            endpoint.peerMaxFrameSize(),
                            endpoint.peerMaxConcurrentStreams(),
                            endpoint.ownMaxConcurrentStreams(),
                            p.writeQueueBytes);
        }

        /** 받은 입력을 유한 단위로 처리한다. 수거 공간이 모자라 멈추면 사건을 수거하고 쓰기를 진행한 뒤 소비 위치부터 다시 입력한다. */
        private void processInput() throws IOException {
            while (!transportGone && !abandoned) {
                if (inputPos >= input.length) {
                    byte[] next = inputQueue.poll();
                    if (next == null) {
                        return;
                    }
                    input = next;
                    inputPos = 0;
                }
                ReceiveProgress progress = endpoint.receiveBounded(
                        input, inputPos, input.length - inputPos, MAX_UNITS_PER_STEP, collectionSpace);
                inputPos += progress.consumed();
                collectEvents();
                if (progress.collectionRequired()) {
                    boolean progressed = writeOutputs();
                    collectEvents();
                    if (!progressed) {
                        // 출력이 막혀 수거할 수 없다. 쓰기가 풀리면 같은 위치에서 재개한다.
                        return;
                    }
                } else if (progress.consumed() == 0) {
                    return;
                }
            }
        }

        /** 다음에 깨어나야 하는 시각까지 남은 나노초. 기한이 없으면 {@link Long#MAX_VALUE}. */
        private long waitLimitNanos() {
            long limitNanos = Long.MAX_VALUE;
            OptionalLong next = endpoint.nextDeadlineNanos();
            if (next.isPresent()) {
                limitNanos = next.getAsLong() - (p.nanoClock.getAsLong() - origin);
            }
            if (preparing) {
                limitNanos = Math.min(limitNanos, p.preparationDeadlineAtNanos - p.nanoClock.getAsLong());
            }
            if (closedSeen) {
                limitNanos = Math.min(limitNanos, finishDeadline - p.nanoClock.getAsLong());
            }
            if (blocked) {
                limitNanos = Math.min(limitNanos, WRITE_RETRY_NANOS);
            }
            return limitNanos;
        }

        private void scheduleTimer() {
            long limit = waitLimitNanos();
            if (limit == Long.MAX_VALUE) {
                cancelTimer();
                return;
            }
            long delayNanos = limit <= 0 ? 0 : limit;
            long at = p.nanoClock.getAsLong() + delayNanos;
            ScheduledFuture<?> current = timer;
            if (current != null && !current.isDone() && timerAtNanos - at <= 0) {
                // 이미 더 이르거나 같은 시각에 깨어나는 타이머가 있다. 그때 다시 계산한다.
                return;
            }
            cancelTimer();
            timerAtNanos = at;
            try {
                timer = loop.schedule(this::onTimer, delayNanos / NANOS_PER_MILLI + 1, TimeUnit.MILLISECONDS);
            } catch (RuntimeException e) {
                // EventLoop가 끝나는 중이다. 채널 정리가 결과를 확정한다.
                timer = null;
            }
        }

        private void cancelTimer() {
            ScheduledFuture<?> current = timer;
            if (current != null) {
                current.cancel(false);
                timer = null;
            }
        }

        private void onTimer() {
            timer = null;
            drive();
        }

        /**
         * 출력을 채널이 받아 주는 만큼 쓰고 실제 쓰기량(TCP) 또는 평문 소비량(TLS)을 보고한다. 0은 보고하지 않는다.
         *
         * @return 한 바이트라도 소비했거나 쓸 출력이 없으면 true, 채널이 받아 주지 않아 막혔으면 false
         */
        private boolean writeOutputs() throws IOException {
            boolean progressed = false;
            for (; ; ) {
                EndpointOutput output = endpoint.nextOutput();
                if (output == null) {
                    releaseWriting();
                    // 더 쓸 출력이 없다. 시작하지 못한 채 사라진 출력이 쥐고 있던 바이트 예약도 돌려준다.
                    returnAllHeldBytes();
                    return true;
                }
                if (writing == null || writing.id() != output.id()) {
                    releaseWriting();
                    writing = output;
                    writingBytes = output.bytes();
                    writingOffset = 0;
                    if (tls == null) {
                        // 출력마다 직접 버퍼 하나. 전체 전달·철회·채널 정리에서 정확히 한 번 반환한다.
                        ByteBuf buffer = p.allocator.directBuffer(writingBytes.length);
                        buffer.writeBytes(writingBytes);
                        writingBuffer = buffer;
                    }
                }
                int remaining = writingBytes.length - writingOffset;
                int count = write(remaining);
                if (count > 0) {
                    endpoint.outputProgress(output.id(), count);
                    writingOffset += count;
                    progressed = true;
                    if (writingOffset == writingBytes.length) {
                        releaseWriting();
                    }
                }
                // TCP는 남기고 못 쓴 것이 막힘이다. TLS는 레코드 하나씩 소비하므로 0일 때(암호문 공간이 차거나 핸드셰이크 전)만 막힘이다.
                if (count == 0 || (tls == null && count < remaining)) {
                    return progressed;
                }
            }
        }

        private int write(int remaining) throws IOException {
            if (tls != null) {
                return tls.writePlain(writingBytes, writingOffset, remaining);
            }
            ByteBuf buffer = Objects.requireNonNull(writingBuffer);
            int count = channel.writeAttempt(buffer.nioBuffer());
            buffer.skipBytes(count);
            return count;
        }

        /** 쓰던 출력의 참조(core 출력 본문과 채널 쓰기용 복사본)를 놓고, 그 출력 때문에 유지하던 바이트 예약을 돌려준다. */
        private void releaseWriting() {
            EndpointOutput released = writing;
            writing = null;
            writingBytes = EMPTY;
            if (released != null) {
                returnHeldBytes(released.applicationStreamId());
            }
            ByteBuf buffer = writingBuffer;
            if (buffer != null) {
                writingBuffer = null;
                buffer.release();
            }
        }

        private void collectEvents() {
            for (; ; ) {
                List<Event> batch = endpoint.drainEvents(1024);
                if (batch.isEmpty()) {
                    return;
                }
                for (Event event : batch) {
                    dispatch(event);
                }
            }
        }

        /**
         * 준비 성공 알림이 거절돼 끊는다. 이 처리 단계의 남은 사건과 입력은 처리하지 않는다. 결과와 종료 사건만 이어서 처리하며, 종료 사건은 이미 확정한
         * 준비 결과(거절 원인)를 바꾸지 못한다({@code preparing}이 이미 거짓이다).
         */
        void abandon() {
            abandoned = true;
            endpoint.disconnect();
            finished = true;
        }

        private void dispatch(Event event) {
            if (abandoned && event.kind() != Event.Kind.CLOSED && event.kind() != Event.Kind.RESULT) {
                return;
            }
            switch (event.kind()) {
                case RESULT:
                    onResult(event);
                    break;
                case REQUEST_RECEIVED:
                    onRequestReceived(this, event);
                    break;
                case REQUEST_RESET:
                    onRequestReset(event);
                    break;
                case AUTHENTICATED:
                    onClientAuthenticated(event);
                    break;
                case AUTHENTICATION_REQUIRED:
                    onAuthenticationRequired(event);
                    break;
                case CLOSED:
                    onClosed(event);
                    break;
                default:
                    throw new IllegalStateException("처리하지 않는 사건 종류: " + event.kind());
            }
        }

        private void onResult(Event event) {
            OutgoingEntry entry = pending.remove(event.ticket().getAsLong());
            if (entry != null) {
                // 결과가 확정돼도 그 요청 프레임을 쓰던 참조가 남아 있으면 본문 바이트 예약은 그 참조가 사라질 때까지 유지한다.
                entry.finish(
                        event.status(),
                        map(event.certainty().get()),
                        event.body(),
                        isBeingWritten(event.streamId()) ? event.streamId() : OutgoingEntry.NO_STREAM);
            }
        }

        /** 지금 쓰는 출력(core 출력 본문과 채널 쓰기용 복사본을 쥔다)이 이 스트림의 애플리케이션 프레임인가. 쓰기 전 프레임은 core가 철회하지만 복사본은 다음 쓰기 걸음까지 남는다. */
        private boolean isBeingWritten(long streamId) {
            return streamId > 0 && writing != null && writing.applicationStreamId() == streamId;
        }

        private void onClientAuthenticated(Event event) {
            facade.announcedCredentialLifetime(event.credentialLifetime());
            Object principal = tls != null ? tls.peerPrincipalOr(p.remoteAddress) : p.remoteAddress;
            facade.authenticated(new AuthenticatedPeer(principal, p.remoteAddress));
            activate(this, p.handler);
        }

        private void onAuthenticationRequired(Event event) {
            stage = PreparationStage.AUTHENTICATION;
            CredentialVerifier verifier = Objects.requireNonNull(p.verifier);
            long id = event.authenticationId();
            // 자격 증명 복사본(방식 이름과 비밀)은 보관하기 전에 바이트를 예약한다. 사용자 함수가 반환하면 돌려준다.
            long retained = (long) event.authenticationMechanism().get().length() + event.body().length;
            if (!p.budgets.tryReserve(Resource.PREPARATION_BYTES, retained)) {
                rejectAuthentication(id, new ResourceExhaustedException(Resource.PREPARATION_BYTES));
                return;
            }
            Credentials credentials =
                    Credentials.of(event.authenticationMechanism().get(), event.body());
            if (!p.budgets.owner().tryReserve(Resource.AUTHENTICATION_TASKS, 1)) {
                p.budgets.release(Resource.PREPARATION_BYTES, retained);
                rejectAuthentication(id, new ResourceExhaustedException(Resource.AUTHENTICATION_TASKS));
                return;
            }
            try {
                p.executor.execute(() -> runAuthentication(id, credentials, verifier, retained));
            } catch (RuntimeException | Error e) {
                p.budgets.owner().release(Resource.AUTHENTICATION_TASKS, 1);
                p.budgets.release(Resource.PREPARATION_BYTES, retained);
                rejectAuthentication(id, e);
            }
        }

        private void rejectAuthentication(long id, Throwable cause) {
            endpoint.completeAuthentication(id, AuthDecision.TRANSIENT_FAILURE);
            failPreparation(PreparationStage.AUTHENTICATION, cause);
        }

        private void onClosed(Event event) {
            CloseReason reason = event.closeReason().get();
            Throwable cause = ioFailure != null
                    ? ioFailure
                    : endCause != null ? endCause : event.cause().orElse(null);
            CloseNotice notice = notice(reason, cause);
            finalNotice = notice;
            finishDeadline = Saturating.add(p.nanoClock.getAsLong(), p.closeFinishNanos);
            closedSeen = true;
            if (preparing) {
                PreparationStage at = isAuthenticationEnd(reason) ? PreparationStage.AUTHENTICATION : stage;
                failPreparation(at, cause != null ? cause : new ConnectionEndedException(notice));
            }
            if (transportGone || reason.kind() == CloseReason.Kind.DISCONNECTED) {
                finished = true;
            }
        }

        /**
         * 준비 중 CLOSE가 연결 인증의 결과인가. 인증 실패는 {@code CLOSE(UNAUTHENTICATED)}, 일시적 실패는 {@code CLOSE(INTERNAL_ERROR)}다(규격
         * 3.3.1절). 클라이언트는 AUTH(ACK)를 받기 전에 상대가 보낸 INTERNAL_ERROR를 인증 단계의 실패로 본다.
         */
        private boolean isAuthenticationEnd(CloseReason reason) {
            if (reason.errorCode() == UNAUTHENTICATED) {
                return true;
            }
            return p.role == ConnectionParams.Role.CLIENT && reason.errorCode() == INTERNAL_ERROR && peerSent(reason);
        }

        private CloseNotice notice(CloseReason reason, @Nullable Throwable cause) {
            switch (reason.kind()) {
                case CLOSE:
                    return CloseNotice.close(sender(reason));
                case ERROR_CLOSE:
                    return CloseNotice.errorClose(reason.errorCode(), sender(reason), cause);
                default:
                    return CloseNotice.disconnected(cause);
            }
        }

        private boolean peerSent(CloseReason reason) {
            return reason.sender().equals(Optional.of(CloseReason.Sender.PEER));
        }

        private CloseNotice.Sender sender(CloseReason reason) {
            return peerSent(reason) ? CloseNotice.Sender.PEER : CloseNotice.Sender.LOCAL;
        }

        /** 채널이 끊겼다. 연결이 아직 끝나지 않았다면 CLOSE 없이 끊긴 것으로 core에 알린다. */
        void transportFailed(Throwable cause) {
            transportGone = true;
            if (closedSeen) {
                finished = true;
                return;
            }
            if (ioFailure == null) {
                ioFailure = cause;
            }
            endpoint.disconnect();
        }

        private void checkPreparation() {
            if (!preparing) {
                return;
            }
            // 단계가 SECURITY인 것은 TLS 연결뿐이다.
            if (stage == PreparationStage.SECURITY
                    && Objects.requireNonNull(tls).handshakeComplete()) {
                stage = PreparationStage.HELLO;
            }
            // 준비 중에는 CLOSED를 내지 않았다(CLOSED는 준비 결과를 먼저 확정한다).
            if (p.nanoClock.getAsLong() - p.preparationDeadlineAtNanos >= 0) {
                PreparationDeadlineExceededException deadline = new PreparationDeadlineExceededException();
                endCause = deadline;
                failPreparation(stage, deadline);
                endpoint.disconnect();
            }
        }

        /** CLOSE로 끝난 연결의 마무리: 남은 쓰기를 비우고 보내는 쪽만 닫은 뒤 상대가 끊을 때까지(또는 상한까지) 읽는다. */
        private void finishClosing() throws IOException {
            if (!closedSeen || finished) {
                return;
            }
            if (p.nanoClock.getAsLong() - finishDeadline >= 0) {
                finished = true;
                return;
            }
            if (outputShutdown || writing != null || endpoint.nextOutput() != null) {
                return;
            }
            if (tls != null) {
                if (tls.pendingOutput()) {
                    return;
                }
                if (!closeNotifyStarted) {
                    closeNotifyStarted = true;
                    tls.shutdownOutput();
                    blocked = tls.pendingOutput();
                    if (blocked) {
                        return;
                    }
                }
            }
            outputShutdown = true;
            channel.shutdownOutputNow();
        }

        /** 채널이 정리됐다. 연결이 아직 끝나지 않았다면 끊김으로 core에 알려 남은 결과를 확정하고, 내부 버퍼를 모두 반환한다. */
        void channelClosed() {
            transportGone = true;
            cancelTimer();
            try {
                if (!closedSeen) {
                    // 끊김은 늘 transportFailed·abort를 거쳐 core에 알리므로, 여기까지 CLOSED가 없었다면 상대가 끊은 것이다.
                    ioFailure = new EOFException("채널이 닫혔다");
                    endpoint.disconnect();
                }
                collectEvents();
            } finally {
                releaseWriting();
                returnAllHeldBytes();
                if (tls != null) {
                    tls.release();
                }
            }
        }

        /** 오류로 멈출 때 연결을 끊어 core가 내는 결과로 요청을 끝낸다. */
        void abort(Throwable original) {
            endCause = original;
            try {
                endpoint.disconnect();
                collectEvents();
            } catch (RuntimeException e) {
                original.addSuppressed(e);
            }
            finished = true;
        }
    }

    /**
     * 쓰기 대기열 상한(바이트). 규칙을 지키는 상대와의 정상 흐름 제어 범위다. 내 요청(상대가 알린 동시 스트림 × 상대가 알린 최대 프레임)과 상대 요청에 대한
     * 응답(내가 알린 동시 스트림 × 상대가 알린 최대 프레임), 거기에 제어 프레임 몇 개를 위한 프레임 하나를 더한다. 상대가 알린 값은 상대가 정하므로 절대
     * 한도({@code absoluteLimit})로 막는다. 절대 한도가 작아도 상대가 알린 최대 프레임 하나보다 작아지지 않는다.
     */
    static long writeQueueLimit(
            long peerMaxFrameSize, long peerMaxConcurrentStreams, long ownMaxConcurrentStreams, long absoluteLimit) {
        long range = (peerMaxConcurrentStreams + ownMaxConcurrentStreams) * peerMaxFrameSize + peerMaxFrameSize;
        return Math.max(peerMaxFrameSize, Math.min(range, absoluteLimit));
    }

    // ---------------------------------------------------------------- 보조

    private static Certainty map(io.github.ppzxc.strand.core.internal.Certainty certainty) {
        switch (certainty) {
            case RESPONSE:
                return Certainty.RESPONSE;
            case NOT_PROCESSED:
                return Certainty.NOT_PROCESSED;
            default:
                return Certainty.UNKNOWN;
        }
    }

    /** 내가 낸 요청 하나. */
    private final class OutgoingEntry {
        static final long NO_TICKET = -1L;
        static final long NO_STREAM = 0L;

        final ResultCompletion<RequestResult> result;
        final int type;
        /** core가 가져간 뒤에는 비운다(내부 참조를 놓는다). 이벤트 루프에서만 쓴다. */
        byte[] extensions;

        byte[] payload;
        final long bytes;
        final long deadlineAtNanos;
        /** 전송 스레드에서만 읽고 쓴다. */
        long ticket = NO_TICKET;

        boolean reserved;

        OutgoingEntry(
                ResultCompletion<RequestResult> result,
                int type,
                byte[] extensions,
                byte[] payload,
                long bytes,
                long deadlineAtNanos) {
            this.result = result;
            this.type = type;
            this.extensions = extensions;
            this.payload = payload;
            this.bytes = bytes;
            this.deadlineAtNanos = deadlineAtNanos;
        }

        /** 결과를 확정하고 예약을 돌려준다. 결과는 한 번만 확정된다({@code ResultCompletion}이 두 번째를 무시한다). */
        void finish(int status, Certainty certainty, byte[] body) {
            finish(status, certainty, body, NO_STREAM);
        }

        /**
         * 결과를 확정한다. 요청 수 예약은 돌려주고, 바이트 예약은 {@code heldStreamId}가 있으면 그 스트림의 출력이 사라질 때까지 유지한다.
         *
         * @param heldStreamId 이 요청의 프레임을 쓰는 참조가 남아 있을 때 그 스트림 번호, 아니면 {@link #NO_STREAM}
         */
        void finish(int status, Certainty certainty, byte[] body, long heldStreamId) {
            releaseBody();
            if (heldStreamId != NO_STREAM && reserved) {
                reserved = false;
                p.budgets.release(Resource.OUTGOING_REQUESTS, 1);
                holdBytes(heldStreamId, Resource.OUTGOING_BYTES, bytes);
            } else {
                returnReservation();
            }
            result.complete(new RequestResult(status, certainty, body));
        }

        /** core를 거치지 않은 로컬 결과. {@code exhausted}가 있으면 RESOURCE_EXHAUSTED(처리 안 됨), 아니면 주어진 status와 확정성이다. */
        void finishLocally(int status, @Nullable Certainty certainty, @Nullable Resource exhausted) {
            releaseBody();
            returnReservation();
            result.complete(
                    exhausted != null
                            ? RequestResult.resourceExhausted(exhausted)
                            : new RequestResult(status, Objects.requireNonNull(certainty), EMPTY));
        }

        /** core가 본문을 가져갔다. 이 객체가 쥔 본문 참조를 놓는다. */
        void releaseBody() {
            extensions = EMPTY;
            payload = EMPTY;
        }

        private void returnReservation() {
            if (reserved) {
                reserved = false;
                releaseOutgoing(bytes);
            }
        }
    }
}
