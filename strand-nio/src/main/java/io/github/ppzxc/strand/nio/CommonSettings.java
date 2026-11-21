package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import io.github.ppzxc.strand.nio.internal.TlsEngines;
import io.github.ppzxc.strand.nio.internal.TlsSizes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/**
 * 클라이언트와 서버 builder가 공유하는 필수 설정: 앱용 실행기, 자원 한도, 준비·종료 시간, 쓰기 대기열, 보안. 필수값 누락과 범위 오류, 최소 진행 공간 부족은
 * {@code build()}에서 거절한다. 새 한도는 임의 기본값이 없다.
 */
abstract class CommonSettings<B extends CommonSettings<B>> {
    /** 쓰기 대기열 바이트의 기본값: 64 MiB(기존 승인 기본값). */
    static final long DEFAULT_WRITE_QUEUE_BYTES = 64L * 1024 * 1024;

    /** CLOSED 뒤 마무리 시간의 기본값: 5초(기존 승인 기본값). */
    static final Duration DEFAULT_CLOSE_FINISH_DEADLINE = Duration.ofSeconds(5);

    private @Nullable Executor executor;
    private @Nullable ResourceLimits limits;
    private @Nullable Duration preparationDeadline;
    private @Nullable Duration shutdownGrace;
    private @Nullable Duration shutdownDeadline;
    private long writeQueueBytes = DEFAULT_WRITE_QUEUE_BYTES;
    private Duration closeFinishDeadline = DEFAULT_CLOSE_FINISH_DEADLINE;
    private @Nullable NioSecurity security;
    private long maxFrameSize = EndpointConfig.DEFAULT_MAX_FRAME_SIZE;
    private Duration idlePingInterval = Duration.ofMillis(EndpointConfig.DEFAULT_IDLE_INTERVAL_MS);
    private Duration ackTimeout = Duration.ofMillis(EndpointConfig.DEFAULT_ACK_TIMEOUT_MS);
    private LongSupplier nanoClock = System::nanoTime;

    abstract B self();

    /**
     * 인증·팩터리·핸들러·알림·기본 관찰을 돌릴 앱용 실행기. 앱이 소유하며 이 모듈은 종료하지 않는다. 호출 스레드에서 바로 실행하는 실행기는 지원하지
     * 않는다.
     *
     * @param value 실행기
     * @return 이 builder
     */
    public B executor(Executor value) {
        this.executor = Objects.requireNonNull(value, "executor");
        return self();
    }

    /**
     * 연결별·소유 객체 전체 자원 한도. 필수다. 최소 진행 공간 부족은 {@code build()}가 거절한다.
     *
     * @param value 자원 한도
     * @return 이 builder
     */
    public B resourceLimits(ResourceLimits value) {
        this.limits = Objects.requireNonNull(value, "resourceLimits");
        return self();
    }

    /**
     * 연결 준비 기한. 클라이언트 호출 또는 서버 TCP 수락부터 주소 해석·보안·HELLO·연결 인증·핸들러 등록까지 하나의 기한이다. 양수여야 한다. 필수다.
     *
     * @param value 준비 기한
     * @return 이 builder
     */
    public B preparationDeadline(Duration value) {
        this.preparationDeadline = Objects.requireNonNull(value, "preparationDeadline");
        return self();
    }

    /**
     * 전체 종료에서 연결이 CLOSE로 끝나기를 기다리는 시간. 0 이상이다. 필수다.
     *
     * @param value 종료 유예
     * @return 이 builder
     */
    public B shutdownGrace(Duration value) {
        this.shutdownGrace = Objects.requireNonNull(value, "shutdownGrace");
        return self();
    }

    /**
     * 전체 종료 기한. 양수이고 종료 유예보다 커야 한다. 필수다.
     *
     * @param value 종료 기한
     * @return 이 builder
     */
    public B shutdownDeadline(Duration value) {
        this.shutdownDeadline = Objects.requireNonNull(value, "shutdownDeadline");
        return self();
    }

    /**
     * 쓰기 대기열 바이트 상한. 남은 출력이 이 값을 넘으면 읽기를 멈춘다. 정하지 않으면 64 MiB다.
     *
     * @param value 바이트(프레임 크기 하한 이상)
     * @return 이 builder
     */
    public B writeQueueBytes(long value) {
        this.writeQueueBytes = value;
        return self();
    }

    /**
     * 연결이 CLOSE로 끝난 뒤 남은 쓰기를 비우고 상대가 끊기를 기다리는 마무리 시간 상한. 정하지 않으면 5초다.
     *
     * @param value 마무리 시간(양수)
     * @return 이 builder
     */
    public B closeFinishDeadline(Duration value) {
        this.closeFinishDeadline = Objects.requireNonNull(value, "closeFinishDeadline");
        return self();
    }

    /**
     * 채널 보안. 필수다.
     *
     * @param value 보안 설정
     * @return 이 builder
     */
    public B security(NioSecurity value) {
        this.security = Objects.requireNonNull(value, "security");
        return self();
    }

    /**
     * 받을 수 있는 최대 프레임 크기. 정하지 않으면 규격 기본값이다.
     *
     * @param value 바이트(16384~1048576)
     * @return 이 builder
     */
    public B maxFrameSize(long value) {
        this.maxFrameSize = value;
        return self();
    }

    /**
     * 유휴 PING 간격. 이 시간 동안 상대에게서 받은 프레임이 없으면 PING을 보낸다. 0이면 유휴 PING을 끈다. 정하지 않으면 30초다. 값은 core에 그대로
     * 전달되며 이 모듈이 따로 타이머를 두지 않는다.
     *
     * @param value 유휴 간격(0 이상)
     * @return 이 builder
     */
    public B idlePingInterval(Duration value) {
        this.idlePingInterval = Objects.requireNonNull(value, "idlePingInterval");
        return self();
    }

    /**
     * 확인 대기 시간. HELLO나 PING을 보낸 뒤 ACK를 기다리는 시간이며, 넘으면 상대가 응답하지 않는 것으로 보고 연결을 끝낸다. 정하지 않으면 5초다.
     *
     * @param value 확인 대기 시간(양수)
     * @return 이 builder
     */
    public B ackTimeout(Duration value) {
        this.ackTimeout = Objects.requireNonNull(value, "ackTimeout");
        return self();
    }

    /** 테스트가 단조 시계를 주입한다. */
    B nanoClock(LongSupplier value) {
        this.nanoClock = Objects.requireNonNull(value);
        return self();
    }

    /** 서버·클라이언트가 추가로 요구하는 필수값 누락을 더한다. */
    abstract void collectMissing(List<String> missing);

    /**
     * 설정을 검증하고 소유 객체 공통 구성을 만든다.
     *
     * @throws IllegalStateException 필수값이 빠졌을 때(빠진 것을 모두 나열한다)
     * @throws IllegalArgumentException 범위 오류나 최소 진행 공간 부족, 직접 실행 실행기일 때
     */
    final Common validate() {
        List<String> missing = new ArrayList<>();
        if (executor == null) {
            missing.add("executor");
        }
        if (limits == null) {
            missing.add("resourceLimits");
        }
        if (preparationDeadline == null) {
            missing.add("preparationDeadline");
        }
        if (shutdownGrace == null) {
            missing.add("shutdownGrace");
        }
        if (shutdownDeadline == null) {
            missing.add("shutdownDeadline");
        }
        if (security == null) {
            missing.add("security");
        }
        collectMissing(missing);
        if (!missing.isEmpty()) {
            throw new IllegalStateException("필수 설정이 없다: " + missing);
        }
        Executor appExecutor = Objects.requireNonNull(executor);
        ResourceLimits resourceLimits = Objects.requireNonNull(limits);
        long preparationNanos = positiveNanos("preparationDeadline", Objects.requireNonNull(preparationDeadline));
        Duration grace = Objects.requireNonNull(shutdownGrace);
        if (grace.isNegative()) {
            throw new IllegalArgumentException("shutdownGrace는 0 이상이어야 한다: " + grace);
        }
        Duration deadline = Objects.requireNonNull(shutdownDeadline);
        positiveNanos("shutdownDeadline", deadline);
        if (deadline.compareTo(grace) <= 0) {
            throw new IllegalArgumentException("shutdownDeadline은 shutdownGrace보다 커야 한다");
        }
        if (writeQueueBytes < EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT) {
            throw new IllegalArgumentException(
                    "writeQueueBytes는 " + EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT + " 이상이어야 한다: " + writeQueueBytes);
        }
        long closeFinishNanos = positiveNanos("closeFinishDeadline", closeFinishDeadline);
        // 범위 검사는 core가 한다(16384~1048576).
        EndpointConfig.defaults().maxFrameSize(maxFrameSize);
        long idleIntervalMs = coreMillis("idlePingInterval", idlePingInterval, false);
        long ackTimeoutMs = coreMillis("ackTimeout", ackTimeout, true);
        NioSecurity channelSecurity = Objects.requireNonNull(security);
        TlsSizes tlsSizes = null;
        if (channelSecurity.tlsContext() != null) {
            if (!TlsEngines.alpnSupported()) {
                throw new IllegalStateException("이 런타임은 ALPN을 지원하지 않아 strand over TCP의 TLS를 쓸 수 없다");
            }
            tlsSizes = TlsSizes.of(Objects.requireNonNull(channelSecurity.tlsContext()));
        }
        long outgoing = resourceLimits.perConnection().get(Resource.OUTGOING_REQUESTS);
        long incoming = resourceLimits.perConnection().get(Resource.INCOMING_REQUESTS);
        ProgressSpace space = ProgressSpace.derive(outgoing, incoming, maxFrameSize, tlsSizes);
        space.require(resourceLimits);
        SerialExecutor.requireNotDirect(appExecutor);
        return new Common(
                appExecutor,
                resourceLimits,
                new OwnerBudgets(resourceLimits),
                space,
                channelSecurity,
                preparationNanos,
                closeFinishNanos,
                writeQueueBytes,
                maxFrameSize,
                incoming,
                grace,
                deadline,
                nanoClock,
                idleIntervalMs,
                ackTimeoutMs);
    }

    /** core 설정이 받는 범위(밀리초)로 바꾸고, 벗어나면 거절한다. 범위 검사는 core가 한다. */
    private static long coreMillis(String name, Duration value, boolean positive) {
        long millis;
        try {
            if (value.toNanos() % 1_000_000 != 0) {
                throw new IllegalArgumentException(name + "은 밀리초 단위여야 한다: " + value);
            }
            millis = value.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(name + "이 너무 크다: " + value, e);
        }
        try {
            EndpointConfig config = positive
                    ? EndpointConfig.defaults().ackTimeoutMs(millis)
                    : EndpointConfig.defaults().idleIntervalMs(millis);
            Objects.requireNonNull(config);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(name + "이 범위를 벗어났다: " + value, e);
        }
        return millis;
    }

    private static long positiveNanos(String name, Duration value) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "은 양수여야 한다: " + value);
        }
        try {
            return value.toNanos();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(name + "이 너무 크다: " + value, e);
        }
    }

    /** 검증을 마친 공통 구성. */
    static final class Common {
        final Executor executor;
        final ResourceLimits limits;
        final OwnerBudgets owner;
        final ProgressSpace space;
        final NioSecurity security;
        final long preparationNanos;
        final long closeFinishNanos;
        final long writeQueueBytes;
        final long maxFrameSize;
        final long incomingRequests;
        final Duration shutdownGrace;
        final Duration shutdownDeadline;
        final LongSupplier nanoClock;
        final long idleIntervalMs;
        final long ackTimeoutMs;

        Common(
                Executor executor,
                ResourceLimits limits,
                OwnerBudgets owner,
                ProgressSpace space,
                NioSecurity security,
                long preparationNanos,
                long closeFinishNanos,
                long writeQueueBytes,
                long maxFrameSize,
                long incomingRequests,
                Duration shutdownGrace,
                Duration shutdownDeadline,
                LongSupplier nanoClock,
                long idleIntervalMs,
                long ackTimeoutMs) {
            this.executor = executor;
            this.limits = limits;
            this.owner = owner;
            this.space = space;
            this.security = security;
            this.preparationNanos = preparationNanos;
            this.closeFinishNanos = closeFinishNanos;
            this.writeQueueBytes = writeQueueBytes;
            this.maxFrameSize = maxFrameSize;
            this.incomingRequests = incomingRequests;
            this.shutdownGrace = shutdownGrace;
            this.shutdownDeadline = shutdownDeadline;
            this.nanoClock = nanoClock;
            this.idleIntervalMs = idleIntervalMs;
            this.ackTimeoutMs = ackTimeoutMs;
        }

        /** 이 소유 객체의 core 설정: 받을 수 있는 프레임 크기와 동시 스트림 수(받는 요청 수 한도). */
        EndpointConfig endpointConfig() {
            return EndpointConfig.defaults()
                    .maxFrameSize(maxFrameSize)
                    .maxConcurrentStreams(incomingRequests)
                    .idleIntervalMs(idleIntervalMs)
                    .ackTimeoutMs(ackTimeoutMs);
        }

        long deadlineAt(long now) {
            return Saturating.add(now, preparationNanos);
        }
    }
}
