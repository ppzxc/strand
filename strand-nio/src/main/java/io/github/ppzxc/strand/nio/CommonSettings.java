package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.WriteQueueLimit;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import io.github.ppzxc.strand.nio.internal.TlsEngines;
import io.github.ppzxc.strand.nio.internal.TlsSizes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

abstract class CommonSettings<B extends CommonSettings<B>> {

    static final long DEFAULT_WRITE_QUEUE_BYTES = WriteQueueLimit.DEFAULT_ABSOLUTE_LIMIT;

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
    private PassageWrapping.@Nullable Wrapper passageWrapper;

    abstract B self();

    public B executor(Executor value) {
        this.executor = Objects.requireNonNull(value, "executor");
        return self();
    }

    public B resourceLimits(ResourceLimits value) {
        this.limits = Objects.requireNonNull(value, "resourceLimits");
        return self();
    }

    public B preparationDeadline(Duration value) {
        this.preparationDeadline = Objects.requireNonNull(value, "preparationDeadline");
        return self();
    }

    public B shutdownGrace(Duration value) {
        this.shutdownGrace = Objects.requireNonNull(value, "shutdownGrace");
        return self();
    }

    public B shutdownDeadline(Duration value) {
        this.shutdownDeadline = Objects.requireNonNull(value, "shutdownDeadline");
        return self();
    }

    public B writeQueueBytes(long value) {
        this.writeQueueBytes = value;
        return self();
    }

    public B closeFinishDeadline(Duration value) {
        this.closeFinishDeadline = Objects.requireNonNull(value, "closeFinishDeadline");
        return self();
    }

    public B security(NioSecurity value) {
        this.security = Objects.requireNonNull(value, "security");
        return self();
    }

    public B maxFrameSize(long value) {
        this.maxFrameSize = value;
        return self();
    }

    public B idlePingInterval(Duration value) {
        this.idlePingInterval = Objects.requireNonNull(value, "idlePingInterval");
        return self();
    }

    public B ackTimeout(Duration value) {
        this.ackTimeout = Objects.requireNonNull(value, "ackTimeout");
        return self();
    }

    B passageWrapper(PassageWrapping.Wrapper value) {
        this.passageWrapper = Objects.requireNonNull(value);
        return self();
    }

    B nanoClock(LongSupplier value) {
        this.nanoClock = Objects.requireNonNull(value);
        return self();
    }

    abstract void collectMissing(List<String> missing);

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

        EndpointConfig.defaults().maxFrameSize(maxFrameSize);
        long idleIntervalMs = coreMillis("idlePingInterval", idlePingInterval, EndpointConfig::isValidIdleIntervalMs);
        long ackTimeoutMs = coreMillis("ackTimeout", ackTimeout, EndpointConfig::isPositiveTimeoutMs);
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
                ackTimeoutMs,
                passageWrapper);
    }

    private static long coreMillis(String name, Duration value, LongPredicate inRange) {
        long millis;
        try {
            if (value.toNanos() % 1_000_000 != 0) {
                throw new IllegalArgumentException(name + "은 밀리초 단위여야 한다: " + value);
            }
            millis = value.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(name + "이 너무 크다: " + value, e);
        }
        if (!inRange.test(millis)) {
            throw new IllegalArgumentException(name + "이 범위를 벗어났다: " + value);
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
        final PassageWrapping.@Nullable Wrapper passageWrapper;

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
                long ackTimeoutMs,
                PassageWrapping.@Nullable Wrapper passageWrapper) {
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
            this.passageWrapper = passageWrapper;
        }

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
