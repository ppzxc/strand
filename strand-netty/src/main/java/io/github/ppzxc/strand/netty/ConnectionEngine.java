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
import io.github.ppzxc.strand.core.internal.ErrorCode;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import io.github.ppzxc.strand.core.internal.WriteQueueLimit;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.MessageWire;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.TlsEngines;
import io.github.ppzxc.strand.netty.internal.TlsPipe;
import io.github.ppzxc.strand.netty.internal.WebSocketStatus;
import io.github.ppzxc.strand.netty.internal.WireFactory;
import io.github.ppzxc.strand.netty.internal.WireMessage;
import io.github.ppzxc.strand.netty.internal.WireUpgradeException;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
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
import org.jspecify.annotations.Nullable;

final class ConnectionEngine {
    static final int MAX_UNITS_PER_STEP = 64;
    static final int TCP_READ_CHUNK_BYTES = 16 * 1024;
    private static final long WRITE_RETRY_NANOS = 1_000_000L;

    private static final long DEFAULT_CLOSE_NOTICE_SLOTS = 1;

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

    private volatile boolean handlerPrepared;

    private @Nullable Loop active;
    private PreparationStage stage;
    private boolean preparing = true;
    private static final long EARLY_REQUEST_OVERHEAD = 64;

    private final ArrayDeque<Event> earlyRequests = new ArrayDeque<>();
    private @Nullable Endpoint loopEndpoint;

    private boolean preparingSlotHeld = true;
    private boolean releasedNotified;
    private long resolvedBytes;
    private final ArrayDeque<InetSocketAddress> pendingTargets = new ArrayDeque<>();
    private final Map<Long, OutgoingEntry> pending = new HashMap<>();
    private final Map<Long, IncomingRequest> incoming = new HashMap<>();
    private CloseNotice finalNotice = CloseNotice.disconnected(null);
    private @Nullable Throwable connectFailure;
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
        this.closed = new ResultCompletion<>(
                params.executor,
                params.budgets.observationBudget(),
                params.budgets.owner().observationBudget(),
                new ObservationBudget(DEFAULT_CLOSE_NOTICE_SLOTS));
        installChannel(channel);
    }

    private void installChannel(StrandChannel installed) {
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

    private boolean attemptEnded() {
        return connectFailure != null || tornDown.get();
    }

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

    @SuppressWarnings("FutureReturnValueIgnored")
    private void connectFailed(StrandChannel failed, Throwable cause) {
        if (attemptEnded()) {
            return;
        }
        if (pendingTargets.isEmpty()) {
            setupFailed(cause);
            return;
        }
        StrandChannel next = new StrandChannel();
        next.config().setTcpNoDelay(true);
        channel = next;
        installChannel(next);
        failed.close();
        connectNext();
    }

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

    private void setupFailed(Throwable cause) {
        Runnable fail = () -> {
            if (connectFailure == null) {
                connectFailure = cause;
                finalNotice = CloseNotice.disconnected(cause);
            }
            closeChannel();
        };
        if (loop.inEventLoop()) {
            fail.run();
        } else {
            runOnLoop(fail);
        }
    }

    private void runOnLoop(Runnable task) {
        try {
            loop.execute(task);
        } catch (RuntimeException e) {
            task.run();
        }
    }

    @SuppressWarnings({"FutureReturnValueIgnored", "EmptyCatch"})
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
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void closeChannel() {
        if (channel.isRegistered()) {
            channel.close();
        } else {
            channel.unsafe().closeForcibly();
            onChannelClosed();
        }
    }

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
            if (ctx.channel().equals(channel)) {
                onException(cause);
            }
        }
    }

    private void onActive() {
        if (stopped || active != null) {
            return;
        }
        pendingTargets.clear();
        releaseResolvedBytes();
        try {
            stage = p.tls != null
                    ? PreparationStage.SECURITY
                    : p.transport.isWebSocket() ? PreparationStage.UPGRADE : PreparationStage.HELLO;
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

    @SuppressWarnings("EmptyCatch")
    private void complete() {
        if (channel.socketHeld() && !loop.isShuttingDown()) {
            try {
                loop.schedule(this::complete, WRITE_RETRY_NANOS, TimeUnit.NANOSECONDS)
                        .addListener(timer -> {
                            if (timer.isCancelled()) {
                                finishClosed();
                            }
                        });
                return;
            } catch (RejectedExecutionException e) {
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

    static void cancelAdmission(ConnectionBudgets budgets, ProgressSpace space) {
        releaseProgress(budgets, space, progressKinds().length);
        budgets.owner().release(Resource.TOTAL_CONNECTIONS, 1);
        budgets.owner().release(Resource.PREPARING_CONNECTIONS, 1);
    }

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

    private void holdBytes(long streamId, Resource resource, long amount) {
        if (amount > 0) {
            heldBytes.put(streamId, new HeldBytes(resource, amount));
        }
    }

    private void returnHeldBytes(long streamId) {
        HeldBytes held = heldBytes.remove(streamId);
        if (held != null) {
            p.budgets.release(held.resource, held.amount);
        }
    }

    private void returnAllHeldBytes() {
        for (HeldBytes held : heldBytes.values()) {
            p.budgets.release(held.resource, held.amount);
        }
        heldBytes.clear();
    }

    private void releaseSlots() {
        dropEarlyRequests(false);
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

    ResourceUsage usage(Resource resource) {
        return p.budgets.usage(resource);
    }

    Result<CloseNotice> closed() {
        return closed;
    }

    void close() {
        if (closeSubmitted.compareAndSet(false, true)) {
            submit(new EndpointCall(endpoint -> endpoint.close(ErrorCode.NO_ERROR)));
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
        submit(new EndpointCall(endpoint -> endpoint.close(ErrorCode.EXCESSIVE_USE)));
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

    void shutdownClose() {
        if (p.attempt != null) {
            p.attempt.cancel();
        }
        close();
    }

    void shutdownAbort() {
        if (p.attempt != null) {
            p.attempt.cancel();
        }
        abort();
    }

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
        ResponseAcceptance ended = endedAcceptance(state);
        if (ended != null) {
            result.complete(ended);
            return result;
        }
        if (!state.responding.compareAndSet(false, true)) {
            result.complete(ResponseAcceptance.resourceExhausted(Resource.RESPONSE_BYTES));
            return result;
        }
        if (!p.budgets.tryReserve(Resource.RESPONSE_BYTES, body.length)) {
            state.responding.set(false);
            ResponseAcceptance nowEnded = endedAcceptance(state);
            result.complete(
                    nowEnded != null ? nowEnded : ResponseAcceptance.resourceExhausted(Resource.RESPONSE_BYTES));
            return result;
        }
        submit(new RespondCommand(state, status, body.clone(), result));
        return result;
    }

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

    private interface Command {
        void execute(Loop loop);

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
                    entry.finish(Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED, EMPTY);
                    return;
                }
                remainingMs = remaining / NANOS_PER_MILLI + 1;
            }
            try {
                long ticket = loop.endpoint.request(entry.type, entry.extensions, entry.payload, remainingMs);
                entry.releaseBody();
                entry.ticket = ticket;
                pending.put(ticket, entry);
            } catch (IllegalArgumentException e) {
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
                    acceptance = ResponseAcceptance.alreadyResponded();
                } else if (loop.endpoint.respond(state.streamId, status, body)) {
                    state.responded = true;
                    accepted = true;
                    acceptance = ResponseAcceptance.accepted();
                } else {
                    acceptance = ResponseAcceptance.requestEnded();
                    returnIncoming(state);
                }
            } catch (IllegalArgumentException e) {
                assert status >= 0 && status <= 16 : "검증을 지난 status여야 한다: " + status;
                acceptance = ResponseAcceptance.resourceExhausted(Resource.PEER_FRAME_SIZE);
            }
            if (accepted) {
                holdBytes(state.streamId, Resource.RESPONSE_BYTES, body.length);
            } else {
                p.budgets.release(Resource.RESPONSE_BYTES, body.length);
            }
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
                loop.endpoint.close(ErrorCode.INTERNAL_ERROR);
                return;
            }
            if (!preparing || loop.closedSeen) {
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
            wakeQueued.set(false);
            abandonRemaining();
        }
    }

    private void failPreparation(PreparationStage at, Throwable cause) {
        if (preparing) {
            preparing = false;
            dropEarlyRequests(true);
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
            completeReleased();
            notifyReady(loop);
        }
    }

    private void confirmAttempt() {
        if (p.attempt != null) {
            p.attempt.confirm();
        }
    }

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

    private void notifyReady(Loop loop) {
        try {
            p.appExecutor.execute(() -> p.observer.ready(facade));
        } catch (RuntimeException | Error e) {
            if (p.role == ConnectionParams.Role.CLIENT) {
                loop.abandon();
                p.observer.rejected(new PreparationFailure(p.remoteAddress, stage, e));
            }
        }
    }

    @SuppressWarnings("EmptyCatch")
    private void notifyFailed(PreparationFailure failure) {
        try {
            p.appExecutor.execute(() -> p.observer.failed(failure));
        } catch (RuntimeException | Error e) {
            try {
                failure.cause().addSuppressed(e);
            } catch (IllegalArgumentException sameAsCause) {
            }
            p.observer.rejected(failure);
        }
    }

    private void activate(Loop loop, @Nullable RequestHandler handler) {
        activeHandler = handler;
        handlerPrepared = true;
        becomeReady(loop);
        Event early;
        while ((early = earlyRequests.poll()) != null) {
            p.budgets.release(Resource.PREPARATION_BYTES, earlyCost(early));
            onRequestReceived(loop, early);
        }
    }

    private static long earlyCost(Event event) {
        return (long) event.body().length + event.extensions().length + EARLY_REQUEST_OVERHEAD;
    }

    private boolean holdEarlyRequest(Event event) {
        if (!preparing || !p.budgets.tryReserve(Resource.PREPARATION_BYTES, earlyCost(event))) {
            return false;
        }
        earlyRequests.add(event);
        return true;
    }

    private void dropEarlyRequests(boolean refuse) {
        Endpoint endpoint = loopEndpoint;
        Event early;
        while ((early = earlyRequests.poll()) != null) {
            if (refuse && endpoint != null) {
                endpoint.refuseRequest(early.streamId());
            }
            p.budgets.release(Resource.PREPARATION_BYTES, earlyCost(early));
        }
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
            loop.endpoint.close(ErrorCode.INTERNAL_ERROR);
            return;
        }
        try {
            p.executor.execute(() -> runFactory(factory));
        } catch (RuntimeException | Error e) {
            p.budgets.owner().release(Resource.FACTORY_TASKS, 1);
            failPreparation(PreparationStage.HANDLER_FACTORY, e);
            loop.endpoint.close(ErrorCode.INTERNAL_ERROR);
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

    private void onRequestReceived(Loop loop, Event event) {
        long streamId = event.streamId();
        byte[] body = event.body();
        byte[] wireExtensions = event.extensions();
        if (!handlerPrepared) {
            if (!holdEarlyRequest(event)) {
                loop.endpoint.refuseRequest(streamId);
            }
            return;
        }
        RequestHandler handler = activeHandler;
        if (handler == null) {
            loop.endpoint.respond(streamId, Status.UNIMPLEMENTED, EMPTY);
            return;
        }
        long bytes = (long) body.length + wireExtensions.length;
        if (!reserveIncoming(bytes)) {
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
            incoming.remove(streamId);
            state.handlerSettled();
            returnIncoming(state);
            p.budgets.release(Resource.HANDLER_TASKS, 1);
            loop.endpoint.refuseRequest(streamId);
        }
    }

    private void onRequestReset(Event event) {
        if (earlyRequests.removeIf(early -> {
            boolean same = early.streamId() == event.streamId();
            if (same) {
                p.budgets.release(Resource.PREPARATION_BYTES, earlyCost(early));
            }
            return same;
        })) {
            return;
        }
        IncomingRequest request = incoming.get(event.streamId());
        if (request != null) {
            finishIncoming(
                    request.state(), RequestEnd.peerReset(event.resetErrorCode().getAsLong()));
        }
    }

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

    private void runHandler(IncomingRequest request, RequestHandler handler) {
        try {
            if (request.ended().isDone() || stopped) {
                return;
            }
            handler.handle(request);
        } catch (Throwable t) {
            diagnoseHandlerFailure(request, t);
            request.respond(Status.INTERNAL, EMPTY);
        } finally {
            request.state().handlerSettled();
            returnIncomingBytes(request.state());
            p.budgets.release(Resource.HANDLER_TASKS, 1);
        }
    }

    @SuppressWarnings("EmptyCatch")
    private void diagnoseHandlerFailure(IncomingRequest request, Throwable failure) {
        try {
            LOGGER.log(
                    Level.WARNING,
                    "받은 요청 핸들러가 예외를 던졌다(type=" + request.type() + ")",
                    sanitizedFailure(failure));
        } catch (RuntimeException ignored) {
        }
    }

    private static Throwable sanitizedFailure(Throwable failure) {
        Throwable copy = new Throwable(failure.getClass().getName());
        copy.setStackTrace(failure.getStackTrace());
        IdentityHashMap<Throwable, Boolean> seen = new IdentityHashMap<>();
        seen.put(failure, Boolean.TRUE);
        Throwable tail = copy;
        for (Throwable cause = failure.getCause(); cause != null && !seen.containsKey(cause); cause = cause.getCause()) {
            seen.put(cause, Boolean.TRUE);
            Throwable causeCopy = new Throwable(cause.getClass().getName());
            causeCopy.setStackTrace(cause.getStackTrace());
            tail.initCause(causeCopy);
            tail = causeCopy;
        }
        return copy;
    }

    private interface Outbound {
        boolean write() throws IOException;

        boolean pending();

        boolean writingNow();

        boolean isWriting(long streamId);

        boolean finishClose(CloseReason reason) throws IOException;

        void release();
    }

    private interface Inbound {
        void offer(byte[] plain);

        void process() throws IOException;

        boolean hasBufferedBytes();

        boolean hasPendingInput();

        PreparationStage stageAfterSecurity();

        boolean upgraded();

        void release();
    }

    private final class Loop {
        final Endpoint endpoint;
        final @Nullable TlsPipe tls;
        final Inbound inbound;
        final Outbound outbound;

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
        private @Nullable ScheduledFuture<?> timer;
        private long timerAtNanos;
        private @Nullable CloseReason closeReason;

        Loop() throws IOException {
            this.origin = p.nanoClock.getAsLong();
            this.endpoint =
                    p.role == ConnectionParams.Role.CLIENT ? Endpoint.client(p.config) : Endpoint.server(p.config);
            loopEndpoint = this.endpoint;
            ConnectionParams.Tls security = p.tls;
            if (security == null) {
                this.tls = null;
            } else {
                boolean alpn = !p.transport.isWebSocket();
                SSLEngine engine;
                if (p.role == ConnectionParams.Role.CLIENT) {
                    String host = Objects.requireNonNull(security.clientHost);
                    engine = alpn
                            ? TlsEngines.client(security.context, host, security.clientPort)
                            : TlsEngines.clientWithoutAlpn(security.context, host, security.clientPort);
                } else {
                    engine =
                            alpn ? TlsEngines.server(security.context) : TlsEngines.serverWithoutAlpn(security.context);
                }
                this.tls = new TlsPipe(engine, security.sizes, p.allocator, channel::writeAttempt, alpn);
            }
            if (p.transport.isWebSocket()) {
                MessageWire wire = createWire();
                if (p.role == ConnectionParams.Role.CLIENT) {
                    wire.start();
                }
                this.inbound = new MessageInbound(wire);
                this.outbound = new MessageOutbound(wire);
            } else {
                this.inbound = new StreamInbound();
                this.outbound = new StreamOutbound();
            }
        }

        private MessageWire createWire() throws IOException {
            int maxMessage = (int) p.maxFrameSize;
            String path = Objects.requireNonNull(p.transport.path());
            if (p.role == ConnectionParams.Role.SERVER) {
                return WireFactory.server(path, maxMessage);
            }
            InetSocketAddress target = (InetSocketAddress) p.remoteAddress;
            try {
                URI uri = new URI(
                        p.tls != null ? "wss" : "ws", null, target.getHostString(), target.getPort(), path, null, null);
                return WireFactory.client(uri, maxMessage);
            } catch (URISyntaxException e) {
                throw new IOException("연결 주소로 WebSocket 주소를 만들 수 없다: " + target, e);
            }
        }

        void accept(byte[] bytes) {
            try {
                if (tls != null) {
                    tls.feed(bytes);
                    moveDecrypted();
                } else {
                    inbound.offer(bytes);
                }
            } catch (IOException e) {
                transportFailed(e);
            }
            drive();
        }

        private void moveDecrypted() {
            byte[] plain = Objects.requireNonNull(tls).pollPlain();
            while (plain != null) {
                inbound.offer(plain);
                plain = tls.pollPlain();
            }
        }

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
                if (tls.peerEnded() && !inbound.hasBufferedBytes()) {
                    transportFailed(new EOFException("상대가 TLS를 끝냈다"));
                }
            }
            inbound.process();
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
            blocked = outbound.pending() || (tls != null && tls.pendingOutput());
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

        private void updateRead() {
            boolean want = !transportGone
                    && !inbound.hasPendingInput()
                    && (tls == null || tls.canAcceptRead())
                    && !backlogged();
            if (want != reading) {
                reading = want;
                channel.config().setAutoRead(want);
            }
        }

        private boolean backlogged() {
            return endpoint.pendingOutputBytes()
                    > WriteQueueLimit.of(
                            endpoint.peerMaxFrameSize(),
                            endpoint.peerMaxConcurrentStreams(),
                            endpoint.ownMaxConcurrentStreams(),
                            p.writeQueueBytes);
        }

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
                return;
            }
            cancelTimer();
            timerAtNanos = at;
            try {
                timer = loop.schedule(this::onTimer, delayNanos / NANOS_PER_MILLI + 1, TimeUnit.MILLISECONDS);
            } catch (RuntimeException e) {
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

        private boolean writeOutputs() throws IOException {
            return outbound.write();
        }

        private final class StreamInbound implements Inbound {
            private final ArrayDeque<byte[]> inputQueue = new ArrayDeque<>();
            private byte[] input = EMPTY;
            private int inputPos;

            @Override
            public void offer(byte[] plain) {
                inputQueue.add(plain);
            }

            @Override
            public boolean hasBufferedBytes() {
                return !inputQueue.isEmpty() || inputPos < input.length;
            }

            @Override
            public boolean hasPendingInput() {
                return hasBufferedBytes();
            }

            @Override
            public PreparationStage stageAfterSecurity() {
                return PreparationStage.HELLO;
            }

            @Override
            public boolean upgraded() {
                return true;
            }

            @Override
            public void release() {}

            @Override
            public void process() throws IOException {
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
                    if (closedSeen) {
                        inputPos = input.length;
                        inputQueue.clear();
                        return;
                    }
                    if (progress.collectionRequired()) {
                        boolean progressed = writeOutputs();
                        collectEvents();
                        if (!progressed) {
                            return;
                        }
                    } else if (progress.consumed() == 0) {
                        return;
                    }
                }
            }
        }

        private final class MessageInbound implements Inbound {
            private final MessageWire messages;
            private final ArrayDeque<byte[]> inputQueue = new ArrayDeque<>();
            private @Nullable WireMessage pendingMessage;

            private MessageInbound(MessageWire messages) {
                this.messages = messages;
            }

            @Override
            public void offer(byte[] plain) {
                inputQueue.add(plain);
            }

            @Override
            public boolean hasBufferedBytes() {
                return !inputQueue.isEmpty();
            }

            @Override
            public boolean hasPendingInput() {
                return !inputQueue.isEmpty() || pendingMessage != null || messages.hasMessages();
            }

            @Override
            public PreparationStage stageAfterSecurity() {
                return PreparationStage.UPGRADE;
            }

            @Override
            public boolean upgraded() {
                return messages.established();
            }

            @Override
            public void release() {
                messages.release();
            }

            @Override
            public void process() throws IOException {
                while (!transportGone && !abandoned) {
                    WireMessage message = pendingMessage;
                    if (message == null) {
                        message = messages.pollMessage();
                    }
                    if (message == null) {
                        byte[] chunk = inputQueue.poll();
                        if (chunk == null) {
                            return;
                        }
                        try {
                            messages.feed(chunk);
                            if (stage == PreparationStage.UPGRADE && messages.established()) {
                                stage = PreparationStage.HELLO;
                            }
                        } catch (WireUpgradeException e) {
                            transportFailed(
                                    new WebSocketUpgradeException(String.valueOf(e.getMessage()), e.httpStatus(), e));
                            return;
                        }
                        continue;
                    }
                    pendingMessage = null;
                    if (message.kind() == WireMessage.Kind.PEER_CLOSED) {
                        transportFailed(new EOFException("상대가 WebSocket Close를 보냈다"));
                        return;
                    }
                    if (closedSeen) {
                        continue;
                    }
                    ReceiveProgress progress = message.kind() == WireMessage.Kind.BINARY
                            ? endpoint.receiveUnit(message.bytes(), 0, message.bytes().length, collectionSpace)
                            : endpoint.receiveUnsupportedInput(collectionSpace);
                    collectEvents();
                    if (progress.collectionRequired()) {
                        pendingMessage = message;
                        boolean progressed = writeOutputs();
                        collectEvents();
                        if (!progressed) {
                            return;
                        }
                    }
                }
            }
        }

        private final class StreamOutbound implements Outbound {
            private @Nullable EndpointOutput writing;
            private byte[] writingBytes = EMPTY;
            private int writingOffset;
            private @Nullable ByteBuf writingBuffer;

            @Override
            public boolean write() throws IOException {
                boolean progressed = false;
                for (; ; ) {
                    EndpointOutput output = endpoint.nextOutput();
                    if (output == null) {
                        release();
                        returnAllHeldBytes();
                        return true;
                    }
                    if (writing == null || writing.id() != output.id()) {
                        release();
                        writing = output;
                        writingBytes = output.readOnlyBytes();
                        writingOffset = 0;
                        if (tls == null) {
                            ByteBuf buffer = p.allocator.directBuffer(writingBytes.length);
                            buffer.writeBytes(writingBytes);
                            writingBuffer = buffer;
                        }
                    }
                    int remaining = writingBytes.length - writingOffset;
                    int count = writeSome(remaining);
                    if (count > 0) {
                        endpoint.outputProgress(output.id(), count);
                        writingOffset += count;
                        progressed = true;
                        if (writingOffset == writingBytes.length) {
                            release();
                        }
                    }
                    if (count == 0 || (tls == null && count < remaining)) {
                        return progressed;
                    }
                }
            }

            private int writeSome(int remaining) throws IOException {
                if (tls != null) {
                    return tls.writePlain(writingBytes, writingOffset, remaining);
                }
                ByteBuf buffer = Objects.requireNonNull(writingBuffer);
                int count = channel.writeAttempt(buffer.nioBuffer());
                buffer.skipBytes(count);
                return count;
            }

            @Override
            public boolean pending() {
                return writing != null || endpoint.nextOutput() != null;
            }

            @Override
            public boolean writingNow() {
                return writing != null;
            }

            @Override
            public boolean isWriting(long streamId) {
                return writing != null && writing.applicationStreamId() == streamId;
            }

            @Override
            public boolean finishClose(CloseReason reason) {
                return true;
            }

            @Override
            public void release() {
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
        }

        private final class MessageOutbound implements Outbound {
            private final MessageWire messages;
            private byte @Nullable [] pendingBytes;

            private int pendingOffset;

            private MessageOutbound(MessageWire messages) {
                this.messages = messages;
            }

            @Override
            public boolean write() throws IOException {
                boolean progressed = false;
                for (; ; ) {
                    if (!flush()) {
                        return progressed;
                    }
                    if (!messages.established()) {
                        return true;
                    }
                    EndpointOutput output = endpoint.nextOutput();
                    if (output == null) {
                        returnAllHeldBytes();
                        return true;
                    }
                    byte[] frame = output.readOnlyBytes();
                    messages.sendMessage(frame);
                    endpoint.outputProgress(output.id(), frame.length);
                    returnHeldBytes(output.applicationStreamId());
                    progressed = true;
                }
            }

            private boolean flush() throws IOException {
                for (; ; ) {
                    byte[] bytes = pendingBytes;
                    if (bytes == null) {
                        bytes = messages.pollWire();
                        if (bytes == null) {
                            return true;
                        }
                        pendingBytes = bytes;
                        pendingOffset = 0;
                    }
                    int remaining = bytes.length - pendingOffset;
                    int count = tls != null
                            ? tls.writePlain(bytes, pendingOffset, remaining)
                            : channel.writeAttempt(ByteBuffer.wrap(bytes, pendingOffset, remaining));
                    pendingOffset += count;
                    if (pendingOffset == bytes.length) {
                        pendingBytes = null;
                    } else if (count == 0 || tls == null) {
                        return false;
                    }
                }
            }

            @Override
            public boolean pending() {
                return pendingBytes != null
                        || (messages.established() && endpoint.nextOutput() != null)
                        || messages.wirePending();
            }

            @Override
            public boolean writingNow() {
                return false;
            }

            @Override
            public boolean isWriting(long streamId) {
                return false;
            }

            @Override
            public boolean finishClose(CloseReason reason) throws IOException {
                if (messages.established()) {
                    int status = WebSocketStatus.of(reason);
                    if (status != WebSocketStatus.NONE) {
                        messages.sendClose(status);
                    }
                    return flush();
                }
                return true;
            }

            @Override
            public void release() {}
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
                entry.finish(
                        event.status(),
                        map(event.certainty().get()),
                        event.body(),
                        isBeingWritten(event.streamId()) ? event.streamId() : OutgoingEntry.NO_STREAM);
            }
        }

        private boolean isBeingWritten(long streamId) {
            return streamId > 0 && outbound.isWriting(streamId);
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
            Throwable cause = ioFailure != null ? ioFailure : endCause;
            CloseNotice notice = notice(reason, cause);
            finalNotice = notice;
            finishDeadline = Saturating.add(p.nanoClock.getAsLong(), p.closeFinishNanos);
            closedSeen = true;
            closeReason = reason;
            if (preparing) {
                PreparationStage at = isAuthenticationEnd(reason) ? PreparationStage.AUTHENTICATION : stage;
                failPreparation(at, cause != null ? cause : new ConnectionEndedException(notice));
            }
            if (transportGone || reason.kind() == CloseReason.Kind.DISCONNECTED) {
                finished = true;
            }
        }

        private boolean isAuthenticationEnd(CloseReason reason) {
            if (reason.errorCode() == ErrorCode.UNAUTHENTICATED) {
                return true;
            }
            return p.role == ConnectionParams.Role.CLIENT
                    && reason.errorCode() == ErrorCode.INTERNAL_ERROR
                    && peerSent(reason);
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
            if (stage == PreparationStage.SECURITY
                    && Objects.requireNonNull(tls).handshakeComplete()) {
                stage = inbound.stageAfterSecurity();
            }
            if (stage == PreparationStage.UPGRADE && inbound.upgraded()) {
                stage = PreparationStage.HELLO;
            }
            if (p.nanoClock.getAsLong() - p.preparationDeadlineAtNanos >= 0) {
                PreparationDeadlineExceededException deadline = new PreparationDeadlineExceededException();
                endCause = deadline;
                failPreparation(stage, deadline);
                endpoint.disconnect();
            }
        }

        private void finishClosing() throws IOException {
            if (!closedSeen || finished) {
                return;
            }
            if (p.nanoClock.getAsLong() - finishDeadline >= 0) {
                finished = true;
                return;
            }
            if (outputShutdown || outbound.writingNow() || endpoint.nextOutput() != null) {
                return;
            }
            if (!outbound.finishClose(Objects.requireNonNull(closeReason))) {
                blocked = true;
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

        void channelClosed() {
            transportGone = true;
            cancelTimer();
            try {
                if (!closedSeen) {
                    ioFailure = new EOFException("채널이 닫혔다");
                    endpoint.disconnect();
                }
                collectEvents();
            } finally {
                outbound.release();
                returnAllHeldBytes();
                inbound.release();
                if (tls != null) {
                    tls.release();
                }
            }
        }

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

    private final class OutgoingEntry {
        static final long NO_TICKET = -1L;
        static final long NO_STREAM = 0L;

        final ResultCompletion<RequestResult> result;
        final int type;
        byte[] extensions;

        byte[] payload;
        final long bytes;
        final long deadlineAtNanos;
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

        void finish(int status, Certainty certainty, byte[] body) {
            finish(status, certainty, body, NO_STREAM);
        }

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

        void finishLocally(int status, @Nullable Certainty certainty, @Nullable Resource exhausted) {
            releaseBody();
            returnReservation();
            result.complete(
                    exhausted != null
                            ? RequestResult.resourceExhausted(exhausted)
                            : new RequestResult(status, Objects.requireNonNull(certainty), EMPTY));
        }

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
