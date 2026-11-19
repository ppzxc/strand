package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Extensions;
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
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.TlsPassage;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * 연결 하나를 돌리는 전송 스레드와 앱 호출의 경계. 전용 전송 스레드 하나만 {@link Endpoint}를 부르고, 다른 스레드의 호출은 예약을 먼저 확보한 뒤 명령으로
 * 큐에 넣고 전송 스레드를 깨운다. 명령 큐는 예약 한도(송신 요청, 받은 요청의 응답)와 연결·인증·팩터리 결과 하나씩이 세므로 유한하다.
 *
 * <p>core는 유한 단위로만 구동한다. 입력은 {@code receiveBounded}로 한 번에 최대 {@value #MAX_UNITS_PER_STEP}단위씩 처리하고, 사건은 제어 출력·결과
 * 사건 공간({@link ProgressSpace}에서 정한 {@link CollectionSpace})만큼만 수거한다. 수거 공간이 모자라 멈추면 쓰기를 진행해 출력을 비우고 소비 위치부터
 * 다시 입력한다. 출력은 {@code nextOutput}으로 고르고 통로의 실제 쓰기량(TCP) 또는 평문 소비량(TLS)을 {@code outputProgress}로 보고한다.
 * 확인·시도·보고 사이에는 같은 연결의 다른 core 명령을 처리하지 않는다. 쓰기 0은 보고하지 않는다.
 *
 * <p>사용자 코드(인증·팩터리·핸들러·알림·기본 관찰)는 연결의 {@code SerialExecutor}에서만 돌려 전송 스레드와 분리하고 같은 연결의 호출을 직렬화한다.
 * 결과 보존은 그 실행을 기다리지 않는다.
 */
final class ConnectionEngine {
    /** 규격 2.5.2절의 error_code. */
    private static final int NO_ERROR = 0;

    private static final int INTERNAL_ERROR = 2;
    /** 규격 2.5.2절 UNAUTHENTICATED. 준비 중 CLOSE가 이 코드면 실패 단계는 연결 인증이다. */
    private static final long UNAUTHENTICATED = 7;

    private static final int UNIMPLEMENTED = 12;
    /** 한 번에 처리하는 입력 단위의 상한. 넘으면 시계와 명령 처리를 먼저 한다. */
    static final int MAX_UNITS_PER_STEP = 64;

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final byte[] EMPTY = new byte[0];
    private static final long NO_DEADLINE = Long.MIN_VALUE;

    /** 통로를 열기 전의 자리. 깨우거나 닫아도 할 일이 없다. */
    private static final Passage NOT_OPENED = new Passage() {
        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) {
            return Inbound.none();
        }

        @Override
        public int write(OutboundMessage message, int offset) {
            return 0;
        }

        @Override
        public void wakeup() {}

        @Override
        public void close() {}
    };

    private final NioStrandConnection facade;
    private final ConnectionParams p;
    private final CollectionSpace collectionSpace;
    private final Queue<Command> commands = new ConcurrentLinkedQueue<>();
    private final Thread thread;
    private final ResultCompletion<CloseNotice> closed;
    private final AtomicBoolean closeSubmitted = new AtomicBoolean();
    private volatile boolean abortRequested;
    private volatile boolean stopped;
    private volatile Passage openedPassage = NOT_OPENED;
    private volatile @Nullable RequestHandler activeHandler;

    // 전송 스레드에서만 읽고 쓰는 상태. 처음 쓰는 곳 외에는 {@link #thread}에서만 접근한다.
    private PreparationStage stage;
    private boolean preparing = true;
    private boolean preparingSlotHeld = true;
    private boolean releasedNotified;
    private final Map<Long, OutgoingEntry> pending = new HashMap<>();
    private final Map<Long, IncomingRequest> incoming = new HashMap<>();
    private CloseNotice finalNotice = CloseNotice.disconnected(null);

    ConnectionEngine(NioStrandConnection facade, ConnectionParams params) {
        this.facade = facade;
        this.p = params;
        this.stage =
                params.role == ConnectionParams.Role.CLIENT ? PreparationStage.CONNECTION : PreparationStage.SECURITY;
        this.collectionSpace = CollectionSpace.of(
                ProgressSpace.eventSlots(usageLimit(Resource.RESULT_EVENT_BYTES), params.maxFrameSize),
                ProgressSpace.outputSlots(usageLimit(Resource.CONTROL_OUTPUT_BYTES)));
        this.closed = new ResultCompletion<>(
                params.executor,
                params.budgets.observationBudget(),
                params.budgets.owner().observationBudget());
        this.thread = new Thread(this::run, "strand-nio-connection");
        this.thread.setDaemon(true);
    }

    private long usageLimit(Resource resource) {
        return p.budgets.usage(resource).limit();
    }

    void start() {
        if (p.attempt != null) {
            p.attempt.bind(this);
        }
        thread.start();
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

    private void releaseSlots() {
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

    void abort() {
        abortRequested = true;
        wake();
    }

    /** 연결 시도 취소를 전송 스레드에 넘긴다. 성공과 취소 가운데 이기는 쪽은 전송 스레드가 처리하는 순서가 정한다. */
    void cancelAttempt() {
        submit(new CancelAttemptCommand());
    }

    RequestHandle request(Request request, RequestOptions options) {
        byte[] payload = request.payload();
        byte[] extensions = request.extensions().toWire();
        long bytes = (long) payload.length + extensions.length;
        long deadlineAt = options.deadline().isPresent()
                ? deadlineAfter(options.deadline().get())
                : NO_DEADLINE;
        OutgoingEntry entry = new OutgoingEntry(
                new ResultCompletion<>(
                        p.executor,
                        p.budgets.observationBudget(),
                        p.budgets.owner().observationBudget()),
                request.type(),
                extensions,
                payload,
                bytes,
                deadlineAt);
        RequestHandle handle = new Handle(entry);
        if (stopped) {
            entry.finishLocally(Status.UNAVAILABLE, Certainty.NOT_PROCESSED, null);
            return handle;
        }
        Resource exhausted = reserveOutgoing(bytes);
        if (exhausted != null) {
            entry.finishLocally(0, null, exhausted);
            return handle;
        }
        entry.reserved = true;
        submit(new RequestCommand(entry));
        return handle;
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
        ResultCompletion<ResponseAcceptance> result = new ResultCompletion<>(
                p.executor, p.budgets.observationBudget(), p.budgets.owner().observationBudget());
        if (!p.budgets.tryReserve(Resource.RESPONSE_BYTES, body.length)) {
            result.complete(ResponseAcceptance.resourceExhausted(Resource.RESPONSE_BYTES));
            return result;
        }
        submit(new RespondCommand(request.state(), status, body.clone(), result));
        return result;
    }

    private final class Handle implements RequestHandle {
        private final OutgoingEntry entry;
        private final AtomicBoolean cancelSubmitted = new AtomicBoolean();

        Handle(OutgoingEntry entry) {
            this.entry = entry;
        }

        @Override
        public Result<RequestResult> result() {
            return entry.result;
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
                    releaseIncoming(state);
                }
            } catch (IllegalArgumentException e) {
                // 협상한 프레임 크기를 넘는 응답. 본문을 보관하지 않고 응답 권한도 소비하지 않는다.
                acceptance = ResponseAcceptance.resourceExhausted(Resource.PEER_FRAME_SIZE);
            }
            // 예약을 돌려준 뒤에 결과를 보존해, 결과를 본 앱이 곧바로 다시 응답해도 예약이 이미 돌아와 있다.
            p.budgets.release(Resource.RESPONSE_BYTES, body.length);
            result.complete(acceptance);
            if (accepted) {
                finishIncoming(state, RequestEnd.responseAccepted());
            }
        }

        @Override
        public void abandon() {
            p.budgets.release(Resource.RESPONSE_BYTES, body.length);
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
                    loop.endpoint.completeAuthentication(id, AuthDecision.PASS);
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
            // 폐기 실행이 거절됐다. 폐기 동작을 전송 스레드에서 대신 실행하지 않고 앱이 조회·회수하도록 보존한다. 예약은 앱이 인수할 때까지 유지한다.
            UnreleasedHandlers registry = p.unreleased;
            if (registry == null) {
                p.budgets.owner().release(Resource.FACTORY_TASKS, 1);
            } else {
                registry.add(new UnreleasedHandler(
                        handler, registry, () -> p.budgets.owner().release(Resource.FACTORY_TASKS, 1)));
            }
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
        openedPassage.wakeup();
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

    private void becomeReady() {
        if (preparing) {
            preparing = false;
            confirmAttempt();
            releasePreparingSlot();
            // 성공한 시도는 통로를 연결에 넘겼으므로 시도가 정리할 것이 없다.
            completeReleased();
            notifyReady();
        }
    }

    private void confirmAttempt() {
        if (p.attempt != null) {
            p.attempt.confirm();
        }
    }

    /** 시도가 소유한 통로 정리 완료를 알린다. 알림은 앱용 실행기를 거치며, 거절되면 실행기 없이 확정한다(앱이 붙인 의존 단계는 이 스레드에서 실행될 수 있다. 공통 API의 변환 Stage가 거절을 실패로 확정하는 것과 같다). */
    private void completeReleased() {
        java.util.concurrent.CompletableFuture<Void> released = p.released;
        if (released == null || releasedNotified) {
            return;
        }
        releasedNotified = true;
        try {
            p.appExecutor.execute(() -> released.complete(null));
        } catch (RuntimeException | Error e) {
            released.complete(null);
        }
    }

    /**
     * 준비 성공 알림은 앱용 실행기에서 한다. 같은 연결의 인증·팩터리가 반환하지 않아도 알릴 수 있도록 연결별 직렬 실행을 거치지 않는다. 실행기가 거절하면
     * 알림을 대신할 스레드를 만들지 않고, 앱이 받을 수 없는 연결이므로 끊고 시도를 거절 원인으로 실패시킨다. 서버는 성공 알림이 필요 없다.
     */
    private void notifyReady() {
        if (p.role != ConnectionParams.Role.CLIENT) {
            return;
        }
        try {
            p.appExecutor.execute(() -> p.observer.ready(facade));
        } catch (RuntimeException | Error e) {
            abortRequested = true;
            wake();
            p.observer.rejected(new PreparationFailure(p.remoteAddress, stage, e));
        }
    }

    /** 준비 실패 알림도 앱용 실행기에서 한다. 거절되면 원래 실패에 거절을 덧붙여 전송 스레드에서 확정한다(앱이 붙인 의존 단계는 이 스레드에서 실행될 수 있다). */
    private void notifyFailed(PreparationFailure failure) {
        try {
            p.appExecutor.execute(() -> p.observer.failed(failure));
        } catch (RuntimeException | Error e) {
            failure.cause().addSuppressed(e);
            p.observer.rejected(failure);
        }
    }

    private void activate(Loop loop, @Nullable RequestHandler handler) {
        activeHandler = handler;
        loop.endpoint.handlerReady();
        becomeReady();
    }

    private void startFactory(Loop loop) {
        if (!preparing) {
            // 이미 취소·기한·종료로 준비가 확정됐다. 늦은 인증 완료로 팩터리를 시작하지 않는다.
            return;
        }
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
            // 실행 거절은 핸들러 시작 전 REFUSED다.
            incoming.remove(streamId);
            releaseIncoming(state);
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

    private void releaseIncoming(IncomingState state) {
        if (!state.reservationReleased) {
            state.reservationReleased = true;
            p.budgets.release(Resource.INCOMING_BYTES, state.reservedBytes);
            p.budgets.release(Resource.INCOMING_REQUESTS, 1);
        }
    }

    private void finishIncoming(IncomingState state, RequestEnd end) {
        incoming.remove(state.streamId);
        releaseIncoming(state);
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
            // 아직 살아 있고 응답 전이면 빈 INTERNAL 응답을 시도한다. 예외 메시지는 상대에게 보내지 않고 연결은 유지한다.
            request.respond(Status.INTERNAL, EMPTY);
        } finally {
            p.budgets.release(Resource.HANDLER_TASKS, 1);
        }
    }

    // ---------------------------------------------------------------- 전송 스레드

    private void run() {
        Throwable fatal = null;
        Loop loop = null;
        try {
            Passage passage = open();
            openedPassage = passage;
            loop = new Loop(passage);
            loop.drive();
        } catch (IOException e) {
            failedToOpen(e);
        } catch (RuntimeException e) {
            // 예기치 못한 오류. 연결을 끊어 core가 내는 결과로 요청을 끝내고 원인을 보존한다. Error는 그대로 전파한다.
            fatal = e;
            if (loop != null) {
                loop.abort(e);
            } else {
                failedToOpen(e);
            }
        } finally {
            teardown(fatal);
        }
    }

    private Passage open() throws IOException {
        long remaining = p.preparationDeadlineAtNanos - p.nanoClock.getAsLong();
        Passage passage = p.opener.open(remaining);
        stage = passage instanceof TlsPassage ? PreparationStage.SECURITY : PreparationStage.HELLO;
        return passage;
    }

    private void failedToOpen(Throwable original) {
        // 통로를 여는 동안에는 명령을 처리하지 않으므로, 그사이 접수된 취소가 열기 실패의 원인이다.
        Throwable cause =
                p.attempt != null && p.attempt.cancelRequested() ? new ConnectionAttemptCancelledException() : original;
        failPreparation(stage, cause);
        finalNotice = CloseNotice.disconnected(cause);
    }

    /** 전송 스레드가 끝나기 전의 마무리: 실제 통로 정리, 남은 결과·요청 확정, 예약 반환, 종료 알림 보존. */
    private void teardown(@Nullable Throwable fatal) {
        try {
            openedPassage.close();
        } finally {
            stopped = true;
            try {
                abandonRemaining();
                failLeftovers();
                failPreparation(stage, fatal != null ? fatal : new IllegalStateException("연결이 준비 중에 끝났다"));
            } finally {
                releaseSlots();
                completeReleased();
                closed.complete(finalNotice);
            }
        }
    }

    private void failLeftovers() {
        pending.values().forEach(entry -> entry.finish(Status.UNAVAILABLE, Certainty.UNKNOWN, EMPTY));
        pending.clear();
        new ArrayList<>(incoming.values())
                .forEach(request -> finishIncoming(request.state(), RequestEnd.connectionEnd()));
    }

    /** 전송 스레드 안에서만 쓰는 상태. */
    private final class Loop {
        final Endpoint endpoint;
        final Passage passage;
        final long origin;
        long lastElapsed;
        boolean finished;
        boolean closedSeen;
        private boolean transportGone;
        private boolean outputShutdown;
        private long finishDeadline;
        private @Nullable IOException ioFailure;
        private @Nullable Throwable endCause;
        private byte[] input = EMPTY;
        private int inputPos;
        private @Nullable EndpointOutput writing;
        private byte[] writingBytes = EMPTY;
        private int writingOffset;

        Loop(Passage passage) {
            this.passage = passage;
            this.origin = p.nanoClock.getAsLong();
            this.endpoint = p.role == ConnectionParams.Role.CLIENT
                    ? Endpoint.client(p.config)
                    : Endpoint.serverWithDeferredAuthentication(p.config);
        }

        void drive() {
            runCommands();
            while (!finished) {
                step();
            }
            collectEvents();
        }

        private void step() {
            try {
                receive();
            } catch (IOException e) {
                transportFailed(e);
            }
            advanceClock();
            runCommands();
            if (abortRequested) {
                endpoint.disconnect();
                finished = true;
            }
            checkPreparation();
            collectEvents();
            try {
                writeOutputs();
            } catch (IOException e) {
                transportFailed(e);
            }
            collectEvents();
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

        /** 기다린 뒤 지금 읽을 수 있는 입력을 받아 처리한다. 처리하지 못한 입력이 남았으면 새로 읽지 않는다. */
        private void receive() throws IOException {
            boolean pendingInput = inputPos < input.length;
            boolean readEnabled = !pendingInput && !backlogged();
            boolean wantWrite = writing != null || endpoint.nextOutput() != null || passage.pendingOutput();
            Inbound inbound = passage.await(pendingInput ? 0 : waitLimitMs(), readEnabled, wantWrite);
            switch (inbound.kind()) {
                case BYTES:
                    input = inbound.data();
                    inputPos = 0;
                    break;
                case END:
                    transportFailed(new EOFException("상대가 연결을 끊었다"));
                    break;
                default:
                    break;
            }
            processInput();
        }

        /** 남은 출력이 상한을 넘으면 읽기를 멈춘다(상대가 읽지 않으면서 계속 보내는 경우 core 출력이 끝없이 늘지 않게 한다). */
        private boolean backlogged() {
            return endpoint.pendingOutputBytes()
                    > ConnectionDriver.writeQueueLimit(
                            endpoint.peerMaxFrameSize(),
                            endpoint.peerMaxConcurrentStreams(),
                            endpoint.ownMaxConcurrentStreams(),
                            p.writeQueueBytes);
        }

        /** 받은 입력을 유한 단위로 처리한다. 수거 공간이 모자라 멈추면 사건을 수거하고 쓰기를 진행한 뒤 소비 위치부터 다시 입력한다. */
        private void processInput() throws IOException {
            while (inputPos < input.length && !transportGone) {
                ReceiveProgress progress = endpoint.receiveBounded(
                        input, inputPos, input.length - inputPos, MAX_UNITS_PER_STEP, collectionSpace);
                inputPos += progress.consumed();
                collectEvents();
                if (progress.collectionRequired()) {
                    boolean progressed = writeOutputs();
                    collectEvents();
                    if (!progressed) {
                        // 출력이 막혀 수거할 수 없다. 쓰기 가능을 기다렸다가 같은 위치에서 재개한다.
                        return;
                    }
                } else if (progress.consumed() == 0) {
                    return;
                }
            }
        }

        private long waitLimitMs() {
            long limitNanos = Long.MAX_VALUE;
            java.util.OptionalLong next = endpoint.nextDeadlineNanos();
            if (next.isPresent()) {
                limitNanos = next.getAsLong() - (p.nanoClock.getAsLong() - origin);
            }
            if (preparing) {
                limitNanos = Math.min(limitNanos, p.preparationDeadlineAtNanos - p.nanoClock.getAsLong());
            }
            if (closedSeen) {
                limitNanos = Math.min(limitNanos, finishDeadline - p.nanoClock.getAsLong());
            }
            if (limitNanos == Long.MAX_VALUE) {
                return -1;
            }
            if (limitNanos <= 0) {
                return 0;
            }
            long millis = limitNanos / NANOS_PER_MILLI + 1;
            return Math.min(millis, Integer.MAX_VALUE);
        }

        /**
         * 출력을 통로가 받아 주는 만큼 쓰고 실제 쓰기량(TCP) 또는 평문 소비량(TLS)을 보고한다. 0은 보고하지 않는다.
         *
         * @return 한 바이트라도 소비했거나 쓸 출력이 없으면 true, 통로가 받아 주지 않아 막혔으면 false
         */
        private boolean writeOutputs() throws IOException {
            boolean progressed = false;
            for (; ; ) {
                EndpointOutput output = endpoint.nextOutput();
                if (output == null) {
                    writing = null;
                    return true;
                }
                if (writing == null || writing.id() != output.id()) {
                    writing = output;
                    writingBytes = output.bytes();
                    writingOffset = 0;
                }
                int remaining = writingBytes.length - writingOffset;
                int count = passage.writeBytes(writingBytes, writingOffset, remaining);
                if (count > 0) {
                    endpoint.outputProgress(output.id(), count);
                    writingOffset += count;
                    progressed = true;
                    if (writingOffset == writingBytes.length) {
                        writing = null;
                    }
                }
                if (count < remaining) {
                    return progressed;
                }
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

        private void dispatch(Event event) {
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
                    onClientAuthenticated();
                    break;
                case AUTHENTICATION_REQUIRED:
                    onAuthenticationRequired(event);
                    break;
                case CLOSED:
                    onClosed(event);
                    break;
                default:
                    break;
            }
        }

        private void onResult(Event event) {
            Optional.ofNullable(pending.remove(event.ticket().getAsLong()))
                    .ifPresent(entry ->
                            entry.finish(event.status(), map(event.certainty().get()), event.body()));
        }

        private void onClientAuthenticated() {
            Object principal = principalOf();
            facade.authenticated(new AuthenticatedPeer(principal, p.remoteAddress));
            activate(this, p.handler);
        }

        private Object principalOf() {
            return passage instanceof TlsPassage
                    ? ((TlsPassage) passage).peerPrincipalOr(p.remoteAddress)
                    : p.remoteAddress;
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

        /** 통로가 끊겼다. 연결이 아직 끝나지 않았다면 CLOSE 없이 끊긴 것으로 core에 알린다. */
        private void transportFailed(IOException cause) {
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
            // 단계가 SECURITY인 것은 TLS 통로뿐이다.
            if (stage == PreparationStage.SECURITY && ((TlsPassage) passage).handshakeComplete()) {
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
        private void finishClosing() {
            if (!closedSeen || finished) {
                return;
            }
            if (p.nanoClock.getAsLong() - finishDeadline >= 0) {
                finished = true;
                return;
            }
            if (!outputShutdown && writing == null && endpoint.nextOutput() == null && !passage.pendingOutput()) {
                outputShutdown = true;
                passage.shutdownOutput();
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
        }
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

        final ResultCompletion<RequestResult> result;
        final int type;
        final byte[] extensions;
        final byte[] payload;
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
            returnReservation();
            result.complete(new RequestResult(status, certainty, body));
        }

        /** core를 거치지 않은 로컬 결과. {@code exhausted}가 있으면 RESOURCE_EXHAUSTED(처리 안 됨), 아니면 주어진 status와 확정성이다. */
        void finishLocally(int status, @Nullable Certainty certainty, @Nullable Resource exhausted) {
            returnReservation();
            result.complete(
                    exhausted != null
                            ? RequestResult.resourceExhausted(exhausted)
                            : new RequestResult(status, Objects.requireNonNull(certainty), EMPTY));
        }

        private void returnReservation() {
            if (reserved) {
                reserved = false;
                releaseOutgoing(bytes);
            }
        }
    }
}
