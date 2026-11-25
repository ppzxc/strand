package io.github.ppzxc.strand.core.internal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import org.jspecify.annotations.Nullable;

public final class Endpoint {

    public static final int FRAME_HEADER_SIZE = Frames.HEADER_SIZE;

    private static final byte[] NO_BYTES = new byte[0];

    private static final int FIRST_SERVER_STREAM_ID = 2;

    static final long AUTHENTICATION_ID = 1;

    private static final int FIRST_CLIENT_STREAM_ID = 1;

    private static final int PING_PAYLOAD_SIZE = 8;
    private static final int AUTH_ACK_PARAM_SIZE = 6;

    private static final int RESET_PAYLOAD_SIZE = 4;
    private static final int TLV_HEADER_SIZE = 4;
    private static final int TIMEOUT_TAG = 0x0001;
    private static final int TIMEOUT_LENGTH = 4;
    private static final int CLOSE_ERROR_CODE_SIZE = Integer.BYTES;
    private static final int CLOSE_MIN_PAYLOAD_SIZE = Frames.CLOSE_FIXED_PAYLOAD_SIZE;
    private static final int IDEMPOTENCY_KEY_TAG = 0x0003;
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 64;

    private static final int MAX_EXTENSIONS_LENGTH = 8192;

    private final EndpointConfig config;
    private final Outbox outbox = new Outbox();

    private final Deque<Event> events = new ArrayDeque<>();

    private long drainedPeerStreamId;

    private final Set<Long> refusedRequests = new HashSet<>();

    private final OwnRequestLedger ledger;

    private final Map<Long, Integer> peerPendingStreams = new LinkedHashMap<>();

    private final Deque<DeferredPing> deferredPings = new ArrayDeque<>();

    private final UnitSlicer slicer;

    private final UnitSink unitSink = new UnitSink();

    private boolean receiving;

    private final ConnectionPhase phase;

    private long peerMaxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;

    private long now;

    private long elapsedNanos;

    private final LivenessTimers timers;

    private final boolean server;

    private static final int OUTPUT_CONTROL_RESERVE = 4;

    private boolean authenticationPending;
    private final OutputProgress output;

    private long highestPeerStreamId;

    private Endpoint(boolean server, EndpointConfig config) {
        this.config = config;
        this.timers = new LivenessTimers(config.ackTimeoutMs(), config.authTimeoutMs(), config.idleIntervalMs());
        long firstStreamId = config.firstStreamId(server ? FIRST_SERVER_STREAM_ID : FIRST_CLIENT_STREAM_ID);
        int ownParity = server ? 0 : 1;
        if ((firstStreamId & 1) != ownParity) {
            throw new IllegalArgumentException(
                    "nextStreamId " + firstStreamId + "는 " + (server ? "서버(짝수)" : "클라이언트(홀수)") + "의 번호 공간이 아니다");
        }
        this.ledger = new OwnRequestLedger(outbox, firstStreamId, ownParity);
        this.slicer = new UnitSlicer(config.maxFrameSize());
        this.output = new OutputProgress(outbox, timers, ledger);
        this.server = server;
        this.phase = server ? ConnectionPhase.server() : ConnectionPhase.client();
    }

    public static Endpoint server(EndpointConfig config) {
        return new Endpoint(true, config);
    }

    public static Endpoint client(EndpointConfig config) {
        Endpoint endpoint = new Endpoint(false, config);
        endpoint.send(Frames.preface());
        endpoint.sendHello();
        return endpoint;
    }

    public ReceiveProgress receiveBounded(
            byte[] bytes, int offset, int length, int maximumUnits, CollectionSpace space) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(space, "space");
        if (offset < 0 || length < 0 || offset > bytes.length - length || maximumUnits < 0) {
            throw new IllegalArgumentException("유효하지 않은 입력 범위 또는 처리 한도");
        }
        if (maximumUnits == 0 || !enterReceive()) {
            return new ReceiveProgress(0, false);
        }
        try {
            unitSink.space = space;
            return slicer.slice(bytes, offset, length, maximumUnits, unitSink);
        } finally {
            receiving = false;
        }
    }

    public static long eventsNeeded(long ownRequests) {
        return ownRequests + 2;
    }

    public static long outputsNeeded(long ownRequests, long deferredPings, long peerPendingRequests) {
        return ownRequests + deferredPings + peerPendingRequests + OUTPUT_CONTROL_RESERVE;
    }

    private boolean hasSpace(CollectionSpace space) {
        long ownRequests = (long) ledger.openCount() + ledger.queuedCount();
        long eventsNeeded = eventsNeeded(ownRequests);
        long outputsNeeded = outputsNeeded(ownRequests, deferredPings.size(), peerPendingStreams.size());
        if (events.size() + eventsNeeded <= space.events() && outbox.size() + outputsNeeded <= space.outputs()) {
            return true;
        }
        if (events.isEmpty() && outbox.isEmpty()) {
            throw new IllegalStateException("수거 공간이 한 단위를 처리하기에 모자라다");
        }
        return false;
    }

    private boolean enterReceive() {
        if (receiving) {
            throw new IllegalStateException("입력을 처리하는 중에 같은 엔드포인트에 다시 입력할 수 없다");
        }
        if (phase.isEnded()) {
            return false;
        }
        receiving = true;
        return true;
    }

    public ReceiveProgress receiveUnit(byte[] bytes, int offset, int length, CollectionSpace space) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(space, "space");
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IllegalArgumentException("유효하지 않은 입력 범위");
        }
        return receiveWholeUnit(bytes, offset, length, true, space);
    }

    public ReceiveProgress receiveUnsupportedInput(CollectionSpace space) {
        Objects.requireNonNull(space, "space");
        return receiveWholeUnit(NO_BYTES, 0, 0, false, space);
    }

    private ReceiveProgress receiveWholeUnit(
            byte[] bytes, int offset, int length, boolean supported, CollectionSpace space) {
        if (slicer.hasBuffered()) {
            throw new IllegalStateException("스트림 입력으로 받아 둔 바이트가 남아 있어 단위로 입력할 수 없다");
        }
        if (!enterReceive()) {
            return new ReceiveProgress(0, false);
        }
        try {
            if (!hasSpace(space)) {
                return new ReceiveProgress(0, true);
            }
            slicer.sliceWhole(bytes, offset, length, supported, unitSink);
            return new ReceiveProgress(supported ? length : 0, false);
        } finally {
            receiving = false;
        }
    }

    public enum TransportClose {
        NONE,

        AFTER_PENDING_OUTPUT,

        IMMEDIATE
    }

    public TransportClose transportClose() {
        if (!phase.isEnded()) {
            return TransportClose.NONE;
        }
        boolean outputPending = output.isPending();
        return outputPending ? TransportClose.AFTER_PENDING_OUTPUT : TransportClose.IMMEDIATE;
    }

    public void advanceTo(long elapsedNanos) {
        if (elapsedNanos < 0 || elapsedNanos < this.elapsedNanos) {
            throw new IllegalArgumentException("경과 나노초는 음수이거나 이전 값보다 작을 수 없다: " + elapsedNanos);
        }
        long target = elapsedNanos / Deadlines.NANOS_PER_MILLI;
        long delta = target - now;
        this.elapsedNanos = elapsedNanos;
        if (delta > 0) {
            advanceMillis(delta);
        }
    }

    private void advanceMillis(long ms) {
        if (phase.isEnded()) {
            return;
        }
        now = Deadlines.plus(now, ms);
        elapsedNanos = Math.max(elapsedNanos, Deadlines.millisToNanos(now));
        LivenessTimers.Expiry expiry = timers.expiry(now);
        if (expiry == LivenessTimers.Expiry.UNAUTHENTICATED) {
            connectionError(ErrorCode.UNAUTHENTICATED);
            return;
        }
        if (expiry == LivenessTimers.Expiry.UNRESPONSIVE) {
            connectionError(ErrorCode.UNRESPONSIVE);
            return;
        }
        expireRequests();
        byte[] idlePing = phase.mayIdlePing() ? timers.idlePing(now) : null;
        if (idlePing != null) {

            timers.idlePingQueued(sendPing(idlePing), now);
        }
    }

    public OptionalLong nextDeadlineNanos() {
        OptionalLong millis = nextDeadline();
        return millis.isPresent() ? OptionalLong.of(Deadlines.millisToNanos(millis.getAsLong())) : millis;
    }

    public long peerMaxFrameSize() {
        return peerMaxFrameSize;
    }

    public long peerMaxConcurrentStreams() {
        return ledger.peerMaxConcurrentStreams();
    }

    public long ownMaxConcurrentStreams() {
        return config.maxConcurrentStreams();
    }

    public OptionalLong nextDeadline() {
        if (phase.isEnded()) {
            return OptionalLong.empty();
        }
        OptionalLong requests = ledger.nextExpiryAt();
        OptionalLong liveness = timers.nextAt(phase.mayIdlePing());
        if (!requests.isPresent()) {
            return liveness;
        }
        if (!liveness.isPresent()) {
            return requests;
        }
        return OptionalLong.of(Math.min(requests.getAsLong(), liveness.getAsLong()));
    }

    public long request(int type, byte[] payload) {
        return requestUntil(type, payload, null);
    }

    public long request(int type, byte[] extensions, byte[] payload, long deadlineMs) {
        if (deadlineMs < 0) {
            throw new IllegalArgumentException("deadlineMs는 음수일 수 없다: " + deadlineMs);
        }
        if (extensions.length > MAX_EXTENSIONS_LENGTH
                || scanExtensions(extensions, 0, extensions.length, 0) != Extensions.VALID) {
            throw new IllegalArgumentException("요청 확장 영역이 규격 2.4절을 어긴다");
        }
        return requestUntil(type, extensions, payload, deadlineMs == 0 ? null : Deadlines.plus(now, deadlineMs));
    }

    private long requestUntil(int type, byte[] payload, @Nullable Long deadlineAt) {
        return requestUntil(type, NO_BYTES, payload, deadlineAt);
    }

    private long requestUntil(int type, byte[] extensions, byte[] payload, @Nullable Long deadlineAt) {
        ConnectionPhase.ApplicationSend send = phase.applicationSend();
        if (send == ConnectionPhase.ApplicationSend.DEFER) {
            return ledger.queue(type, extensions, payload, deadlineAt);
        }
        if (!phase.isActive()) {
            long ticket = ledger.issueTicket();
            failLocally(ticket);
            return ticket;
        }

        long ticket = ledger.nextTicket();
        if (refuseLocally(ticket, ledger.open(type, extensions, payload, deadlineAt, peerMaxFrameSize))
                == OwnRequestLedger.Admission.OVERSIZE) {
            throw oversizeFrame("요청");
        }
        return ticket;
    }

    private OwnRequestLedger.Admission refuseLocally(long ticket, OwnRequestLedger.Admission admission) {
        switch (admission) {
            case CLOSING:
            case STREAM_LIMIT:
                failLocally(ticket);
                break;
            case EXHAUSTED:
                failLocally(ticket);
                close(ErrorCode.NO_ERROR);
                break;
            default:
                break;
        }
        return admission;
    }

    public long request(int type, byte[] payload, long deadlineMs) {
        if (deadlineMs < 1) {
            throw new IllegalArgumentException("deadlineMs는 1 이상이어야 한다: " + deadlineMs);
        }
        return requestUntil(type, payload, Deadlines.plus(now, deadlineMs));
    }

    public void cancelRequest(long ticket) {
        endOwnRequest(ledger.cancelTicket(ticket));
    }

    public boolean respond(long streamId, int status, byte[] payload) {
        if (!Status.isValid(status)) {
            throw new IllegalArgumentException("status는 0~16이어야 한다: " + status);
        }
        Integer type = peerPendingStreams.get(streamId);
        if (type == null) {
            return false;
        }
        byte[] frame = Frames.response(type, streamId, status, payload);
        if (exceedsPeerFrameSize(frame)) {
            throw oversizeFrame("응답");
        }
        peerPendingStreams.remove(streamId);
        send(frame);
        conclude();
        return true;
    }

    public void refuseRequest(long streamId) {
        if (peerPendingStreams.remove(streamId) == null) {
            return;
        }
        send(Frames.reset(streamId, ErrorCode.REFUSED));
        conclude();
    }

    public void cancel(long streamId) {
        endOwnRequest(ledger.cancelStream(streamId, Status.CANCELLED));
    }

    private void expireRequests() {
        if (ledger.expire(now, this::applyOutcome)) {
            conclude();
        }
    }

    private void endOwnRequest(OwnRequestLedger.@Nullable Outcome outcome) {
        if (outcome == null) {
            return;
        }
        applyOutcome(outcome);
        if (outcome.numbered()) {

            conclude();
        }
    }

    private void applyOutcome(OwnRequestLedger.Outcome outcome) {
        if (outcome.resetToSend) {
            send(Frames.reset(outcome.streamId, ErrorCode.CANCEL));
        }
        events.add(Event.result(outcome.ticket, outcome.streamId, outcome.status, outcome.certainty, outcome.body));
    }

    public void ping(byte[] payload) {
        if (payload.length != PING_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("PING payload는 정확히 8바이트여야 한다: " + payload.length);
        }
        ConnectionPhase.ApplicationSend send = phase.applicationSend();
        if (send == ConnectionPhase.ApplicationSend.REFUSE) {
            return;
        }
        byte[] copy = payload.clone();
        if (send == ConnectionPhase.ApplicationSend.DEFER) {
            deferredPings.addLast(new DeferredPing(copy, ledger.nextTicket()));
        } else {
            sendPing(copy);
        }
    }

    public void close(int errorCode) {
        close(errorCode, "");
    }

    public void close(int errorCode, String reason) {
        Objects.requireNonNull(reason, "reason");
        if (!ErrorCode.isCloseCode(errorCode)) {
            throw new IllegalArgumentException("CLOSE에 쓸 수 없는 error_code: " + errorCode);
        }
        if (!phase.isActive()) {
            return;
        }
        if (errorCode != ErrorCode.NO_ERROR) {
            refuseUnhandedRequests();
        }
        long maxFrameSize = phase.hasPeerHello() ? peerMaxFrameSize : EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT;
        int reasonCapacity = (int) Math.max(0, maxFrameSize - Frames.HEADER_SIZE - Frames.CLOSE_FIXED_PAYLOAD_SIZE);
        send(Frames.close(errorCode, highestPeerStreamId, utf8Prefix(reason, reasonCapacity)));
        phase.closeSent(errorCode);
        endQueuedRequests();
        conclude();
    }

    public void credentialExpired() {
        closeForCredentialEnd(ErrorCode.NO_ERROR);
    }

    public void credentialRevoked() {
        closeForCredentialEnd(ErrorCode.UNAUTHENTICATED);
    }

    private void closeForCredentialEnd(int errorCode) {
        if (!server || !phase.hasPassedAuthGate()) {
            throw new IllegalStateException("자격 증명 종료 통지는 서버의 AUTH 관문 통과 뒤에만 가능하다");
        }
        if (!phase.isActive()) {
            return;
        }
        close(errorCode);
    }

    public void disconnect() {
        phase.transportDisconnected();
        conclude();
        output.disconnect();
    }

    public void abortClosing() {
        if (phase.phase() != ConnectionPhase.Phase.CLOSING) {
            return;
        }
        refuseUnhandedRequests();

        phase.pendingStreamsReachedZero();
        conclude();
    }

    public long pendingOutputBytes() {
        return outbox.bytes() + output.writingRemaining();
    }

    public @Nullable EndpointOutput nextOutput() {
        return output.next();
    }

    public void outputProgress(long id, int count) {
        output.progress(id, count, now);
    }

    public List<Event> drainEvents(int maximum) {
        if (maximum < 0) {
            throw new IllegalArgumentException("사건 수거 한도는 음수일 수 없다");
        }
        if (events.isEmpty()) {
            return Collections.emptyList();
        }
        List<Event> drained = new ArrayList<>();
        while (!events.isEmpty() && drained.size() < maximum) {
            Event event = events.removeFirst();
            if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                if (refusedRequests.remove(event.streamId())) {
                    continue;
                }
                drainedPeerStreamId = Math.max(drainedPeerStreamId, event.streamId());
            }
            drained.add(event);
        }
        return Collections.unmodifiableList(drained);
    }

    private void rejectBadLength(byte[] bytes, int off) {
        if (Frames.typeOf(bytes, off) == FrameType.CLOSE) {
            phase.connectionFailed();
            conclude();
        } else {
            connectionError();
        }
    }

    private void handle(byte[] bytes, int off, int size) {
        dispatch(bytes, off, size);

        conclude();
    }

    private void dispatch(byte[] bytes, int off, int size) {

        timers.frameReceived(now);
        int type = Frames.typeOf(bytes, off);
        int extensionsLength = Frames.extensionsLengthOf(bytes, off);
        long streamId = Frames.streamIdOf(bytes, off);
        if (type == FrameType.CLOSE) {
            handleClose(bytes, off, size, extensionsLength, streamId);
            return;
        }
        if (extensionsLength > size - Frames.HEADER_SIZE) {
            connectionError();
            return;
        }
        if (type >= FrameType.FIRST_APPLICATION) {
            handleApplication(bytes, off, size, type, extensionsLength, streamId);
            return;
        }
        boolean ack = (Frames.flagsOf(bytes, off) & Flags.ACK) != 0;
        ConnectionPhase.Admission admission = phase.admit(type, ack);
        if (admission == ConnectionPhase.Admission.RESERVED) {

            if (extensionsLength != 0) {
                connectionError();
            }
            return;
        }
        if (extensionsLength != 0 || (type != FrameType.RESET && streamId != 0)) {
            connectionError();
            return;
        }
        if (admission == ConnectionPhase.Admission.CONNECTION_ERROR) {

            connectionError();
            return;
        }
        int payloadAt = off + Frames.HEADER_SIZE;
        int payloadSize = size - Frames.HEADER_SIZE;
        if (type == FrameType.HELLO) {
            handleHello(ack, bytes, payloadAt, payloadSize);
        } else if (type == FrameType.PING) {
            handlePing(ack, bytes, payloadAt, payloadSize);
        } else if (type == FrameType.RESET) {
            handleReset(streamId, bytes, payloadAt, payloadSize);
        } else if (type == FrameType.AUTH) {
            handleAuth(bytes, payloadAt, payloadSize);
        }
    }

    private void handleReset(long streamId, byte[] bytes, int payloadAt, int payloadSize) {
        if (isInvalidStreamId(streamId) || payloadSize != RESET_PAYLOAD_SIZE) {
            connectionError();
            return;
        }
        if (isOwnSpace(streamId)) {
            OwnRequestLedger.Outcome outcome = ledger.onReset(streamId, Frames.readUnsignedInt(bytes, payloadAt));
            if (outcome != null) {
                applyOutcome(outcome);
            }
        } else if (peerPendingStreams.remove(streamId) != null) {

            events.add(Event.requestReset(streamId, Frames.readUnsignedInt(bytes, payloadAt)));
        }
    }

    private static boolean isInvalidStreamId(long streamId) {
        return !Frames.isValidStreamId(streamId);
    }

    private boolean isOwnSpace(long streamId) {
        return ledger.isOwnSpace(streamId);
    }

    private void handleApplication(byte[] bytes, int off, int size, int type, int extensionsLength, long streamId) {

        Extensions extensionsResult = extensionsLength > MAX_EXTENSIONS_LENGTH
                ? Extensions.MALFORMED
                : scanExtensions(bytes, off, extensionsLength);
        if (extensionsResult == Extensions.MALFORMED
                || phase.admit(type, false) != ConnectionPhase.Admission.ALLOW
                || isInvalidStreamId(streamId)) {
            connectionError();
        } else if (isOwnSpace(streamId)) {
            handleResponse(bytes, off, size, extensionsLength, streamId);
        } else {
            handleRequest(type, extensionsResult, streamId, bytes, off, size, extensionsLength);
        }
    }

    private void handleRequest(
            int type,
            Extensions extensionsResult,
            long streamId,
            byte[] bytes,
            int off,
            int size,
            int extensionsLength) {
        if (streamId <= highestPeerStreamId) {

            connectionError();
            return;
        }
        highestPeerStreamId = streamId;
        if (phase.closeExchanged()) {

            return;
        }
        if (peerPendingStreams.size() + 1 > config.maxConcurrentStreams()) {
            send(Frames.reset(streamId, ErrorCode.REFUSED));
        } else if (!config.knows(type)) {
            send(Frames.response(type, streamId, Status.UNIMPLEMENTED));
        } else if (extensionsResult == Extensions.INVALID_VALUE) {
            send(Frames.response(type, streamId, Status.INVALID_ARGUMENT));
        } else {

            peerPendingStreams.put(streamId, type);
            int extensionsAt = off + Frames.HEADER_SIZE;
            events.add(Event.requestReceived(
                    streamId,
                    type,
                    Arrays.copyOfRange(bytes, extensionsAt, extensionsAt + extensionsLength),
                    bodyOf(bytes, off, size, extensionsLength)));
        }
    }

    private enum Extensions {
        MALFORMED,
        INVALID_VALUE,
        VALID
    }

    private static Extensions scanExtensions(byte[] bytes, int off, int extensionsLength) {
        return scanExtensions(bytes, off, extensionsLength, Frames.HEADER_SIZE);
    }

    private static Extensions scanExtensions(byte[] bytes, int off, int extensionsLength, int headerSize) {
        Set<Integer> tags = new HashSet<>();
        boolean valuesValid = true;
        int at = off + headerSize;
        int end = at + extensionsLength;
        while (at < end) {
            if (end - at < TLV_HEADER_SIZE) {
                return Extensions.MALFORMED;
            }
            int tag = Frames.readUnsignedShort(bytes, at);
            int length = Frames.readUnsignedShort(bytes, at + 2);
            at += TLV_HEADER_SIZE + length;
            if (at > end || !tags.add(tag)) {
                return Extensions.MALFORMED;
            }
            if ((tag == TIMEOUT_TAG && length != TIMEOUT_LENGTH)
                    || (tag == IDEMPOTENCY_KEY_TAG && (length < 1 || length > MAX_IDEMPOTENCY_KEY_LENGTH))) {
                valuesValid = false;
            }
        }
        return valuesValid ? Extensions.VALID : Extensions.INVALID_VALUE;
    }

    private static byte[] bodyOf(byte[] bytes, int off, int size, int extensionsLength) {
        int from = off + Frames.HEADER_SIZE + extensionsLength;
        return Arrays.copyOfRange(bytes, from, off + size);
    }

    private void handleResponse(byte[] bytes, int off, int size, int extensionsLength, long streamId) {
        OwnRequestLedger.Outcome outcome = ledger.onResponse(
                streamId,
                Frames.typeOf(bytes, off),
                Frames.statusOf(bytes, off),
                bodyOf(bytes, off, size, extensionsLength));
        if (outcome == null) {

            return;
        }
        applyOutcome(outcome);
    }

    private void handleHello(boolean ack, byte[] bytes, int payloadAt, int payloadSize) {
        if (ack) {

            if (payloadSize != 0) {
                connectionError();
                return;
            }
            phase.ownHelloAckReceived();
            timers.ackReceived(LivenessTimers.Kind.HELLO, null);
            return;
        }
        PeerLimits peer = peerLimitsOf(bytes, payloadAt, payloadSize);
        if (peer == null) {
            connectionError();
            return;
        }
        phase.peerHelloAcked();
        ledger.setPeerMaxConcurrentStreams(peer.maxConcurrentStreams);
        peerMaxFrameSize = peer.maxFrameSize;
        send(Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        if (server) {
            timers.authWaitStarted(now);
            return;
        }
        byte[] auth = Frames.auth(config.authMechanism(), config.authCredentials());
        if (exceedsPeerFrameSize(auth)) {

            phase.connectionFailed();
            return;
        }

        sendAwaitingAck(auth, LivenessTimers.Kind.AUTH, null);
    }

    private static byte[] utf8Prefix(String reason, int maxBytes) {
        byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(bytes.length, maxBytes);
        CharsetDecoder decoder = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        while (length > 0) {
            try {
                decoder.reset().decode(ByteBuffer.wrap(bytes, 0, length));
                break;
            } catch (CharacterCodingException invalidPrefix) {
                length--;
            }
        }
        return Arrays.copyOf(bytes, length);
    }

    private void handleAuth(byte[] bytes, int payloadAt, int payloadSize) {

        if (server) {
            handleServerAuth(bytes, payloadAt, payloadSize);
        } else {
            handleClientAuthAck(bytes, payloadAt, payloadSize);
        }
    }

    private void handleServerAuth(byte[] bytes, int payloadAt, int payloadSize) {
        if (authenticationPending) {
            connectionError();
            return;
        }
        timers.authWaitEnded();
        int mechanismLength = payloadSize == 0 ? 0 : bytes[payloadAt] & 0xFF;
        if (mechanismLength < 1
                || mechanismLength > EndpointConfig.MAX_AUTH_MECHANISM_LENGTH
                || payloadSize < 1 + mechanismLength) {
            connectionError();
            return;
        }
        String mechanism = new String(bytes, payloadAt + 1, mechanismLength, StandardCharsets.US_ASCII);
        if (!EndpointConfig.isValidAuthMechanism(mechanism)) {
            connectionError();
            return;
        }

        byte[] credentials = Arrays.copyOfRange(bytes, payloadAt + 1 + mechanismLength, payloadAt + payloadSize);
        authenticationPending = true;
        events.add(Event.authenticationRequired(mechanism, credentials));
    }

    public void completeAuthentication(long id, AuthDecision decision) {
        completeAuthentication(id, decision, OptionalLong.empty());
    }

    public void completeAuthentication(long id, AuthDecision decision, OptionalLong credentialLifetime) {
        Objects.requireNonNull(decision, "decision");
        Objects.requireNonNull(credentialLifetime, "credentialLifetime");
        if (credentialLifetime.isPresent() && !EndpointConfig.isUnsigned32(credentialLifetime.getAsLong())) {
            throw new IllegalArgumentException(
                    "credentialLifetime은 부호 없는 32비트 정수여야 한다: " + credentialLifetime.getAsLong());
        }
        if (id != AUTHENTICATION_ID || !authenticationPending || !phase.isActive()) {
            return;
        }
        authenticationPending = false;
        finishAuthentication(decision, credentialLifetime);
    }

    private void finishAuthentication(AuthDecision decision, OptionalLong credentialLifetime) {
        if (!phase.isActive()) {

            return;
        }
        if (decision != AuthDecision.PASS) {

            connectionError(decision == AuthDecision.REJECT ? ErrorCode.UNAUTHENTICATED : ErrorCode.INTERNAL_ERROR);
            return;
        }
        send(Frames.authAck(credentialLifetime));
        openAuthGate();
    }

    private void handleClientAuthAck(byte[] bytes, int payloadAt, int payloadSize) {
        if (payloadSize % AUTH_ACK_PARAM_SIZE != 0) {
            connectionError();
            return;
        }
        OptionalLong lifetime = OptionalLong.empty();
        Set<Integer> seen = new HashSet<>();
        for (int at = payloadAt; at < payloadAt + payloadSize; at += AUTH_ACK_PARAM_SIZE) {
            if (!seen.add(Frames.readUnsignedShort(bytes, at))) {
                connectionError();
                return;
            }

            if (Frames.readUnsignedShort(bytes, at) == Frames.AUTH_PARAM_CREDENTIAL_LIFETIME) {
                lifetime = OptionalLong.of(Frames.readUnsignedInt(bytes, at + 2));
            }
        }
        timers.ackReceived(LivenessTimers.Kind.AUTH, null);
        events.add(Event.authenticated(lifetime));
        openAuthGate();
    }

    private void openAuthGate() {
        phase.authGatePassed();
        while (true) {
            DeferredPing ping = deferredPings.peekFirst();
            long ticket = ledger.firstQueuedTicket();
            OwnRequestLedger.Admission admission =
                    ledger.openQueued(ping == null ? Long.MAX_VALUE : ping.beforeTicket, peerMaxFrameSize);
            if (admission != OwnRequestLedger.Admission.NONE) {

                if (refuseLocally(ticket, admission) == OwnRequestLedger.Admission.OVERSIZE) {
                    failLocally(ticket);
                }
            } else if (ping == null) {
                return;
            } else {
                deferredPings.removeFirst();
                if (!phase.closeExchanged()) {
                    sendPing(ping.payload);
                }
            }
        }
    }

    private static @Nullable PeerLimits peerLimitsOf(byte[] bytes, int payloadAt, int payloadSize) {
        if (payloadSize % HelloParams.HELLO_PARAM_SIZE != 0) {
            return null;
        }
        long maxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
        long maxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;
        Set<Integer> seen = new HashSet<>();
        for (int at = payloadAt; at < payloadAt + payloadSize; at += HelloParams.HELLO_PARAM_SIZE) {
            int identifier = Frames.readUnsignedShort(bytes, at);
            if (!seen.add(identifier)) {
                return null;
            }
            long value = Frames.readUnsignedInt(bytes, at + 2);
            if (identifier == HelloParams.MAX_FRAME_SIZE_ID
                    && (value < EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT
                            || value > HelloParams.MAX_FRAME_SIZE_UPPER_LIMIT)) {
                return null;
            }
            if (identifier == HelloParams.MAX_FRAME_SIZE_ID) {
                maxFrameSize = value;
            } else if (identifier == HelloParams.MAX_CONCURRENT_STREAMS_ID) {
                maxConcurrentStreams = value;
            }
        }
        return new PeerLimits(maxConcurrentStreams, maxFrameSize);
    }

    private void handlePing(boolean ack, byte[] bytes, int payloadAt, int payloadSize) {
        if (payloadSize != PING_PAYLOAD_SIZE) {
            connectionError();
            return;
        }

        byte[] payload = Arrays.copyOfRange(bytes, payloadAt, payloadAt + PING_PAYLOAD_SIZE);
        if (ack) {

            timers.ackReceived(LivenessTimers.Kind.PING, payload);
        } else {
            send(Frames.control(FrameType.PING, Flags.ACK, payload));
        }
    }

    private void handleClose(byte[] bytes, int off, int size, int extensionsLength, long streamId) {
        int payloadSize = size - Frames.HEADER_SIZE - extensionsLength;
        long lastStreamId = 0;
        boolean malformed = extensionsLength != 0 || streamId != 0 || payloadSize < CLOSE_MIN_PAYLOAD_SIZE;
        if (!malformed) {
            lastStreamId = Frames.readLong(bytes, off + Frames.HEADER_SIZE + CLOSE_ERROR_CODE_SIZE);

            malformed = lastStreamId != 0
                    && (!isOwnSpace(lastStreamId)
                            || lastStreamId < 0
                            || lastStreamId > ledger.highestWrittenStreamId());
        }
        if (malformed) {
            phase.connectionFailed();
            return;
        }
        if (phase.closeReceived(ErrorCode.normalizeReceived(Frames.readUnsignedInt(bytes, off + Frames.HEADER_SIZE)))) {
            for (OwnRequestLedger.Outcome outcome : ledger.endAfter(lastStreamId)) {
                applyOutcome(outcome);
            }
        }
        endQueuedRequests();
    }

    private void endQueuedRequests() {
        for (OwnRequestLedger.Outcome outcome : ledger.onClosing()) {
            applyOutcome(outcome);
        }
    }

    private void refuseUnhandedRequests() {
        Iterator<Long> pending = peerPendingStreams.keySet().iterator();
        while (pending.hasNext()) {
            long streamId = pending.next();
            if (streamId > drainedPeerStreamId) {
                pending.remove();
                send(Frames.reset(streamId, ErrorCode.REFUSED));
                refusedRequests.add(streamId);
            }
        }
    }

    private void connectionError() {
        connectionError(ErrorCode.PROTOCOL_ERROR);
    }

    private void connectionError(int errorCode) {
        refuseUnhandedRequests();
        if (!phase.closeExchanged()) {
            send(Frames.close(errorCode, highestPeerStreamId));
            phase.closeSent(errorCode);
        }
        phase.connectionFailed(errorCode);
        conclude();
    }

    private void closeWithoutClose() {
        phase.connectionFailed();
        conclude();
    }

    private void conclude() {
        if (phase.isEnded()) {
            return;
        }
        if (ledger.openCount() == 0 && peerPendingStreams.isEmpty()) {
            phase.pendingStreamsReachedZero();
        }
        if (!phase.shouldCloseTransport()) {
            return;
        }
        phase.transportClosed();
        slicer.discard();
        timers.clear();
        for (OwnRequestLedger.Outcome outcome : ledger.onTransportClosed(phase.endedByDisconnect())) {
            applyOutcome(outcome);
        }
        deferredPings.clear();
        peerPendingStreams.clear();
        events.add(Event.closed(phase.closeReason()));
    }

    private void sendHello() {
        sendAwaitingAck(Frames.hello(config), LivenessTimers.Kind.HELLO, null);
    }

    private long sendPing(byte[] payload) {
        return sendAwaitingAck(Frames.control(FrameType.PING, 0, payload), LivenessTimers.Kind.PING, payload);
    }

    OwnRequestLedger.@Nullable RequestState requestState(long ticket) {
        return ledger.stateOfTicket(ticket);
    }

    int openStreamCount() {
        return ledger.openCount();
    }

    private boolean exceedsPeerFrameSize(byte[] frame) {
        return frame.length > peerMaxFrameSize;
    }

    private IllegalArgumentException oversizeFrame(String kind) {
        return new IllegalArgumentException(kind + " 프레임이 상대의 max_frame_size(" + peerMaxFrameSize + ")를 넘는다");
    }

    private void failLocally(long ticket) {
        events.add(Event.localUnavailable(ticket));
    }

    private Outbox.Node send(byte[] frame) {
        OutboundMessage message = OutboundMessage.of(frame);
        if (message.isApplication()) {
            return outbox.addApplication(message);
        }
        if (Frames.typeOf(frame, 0) == FrameType.CLOSE) {
            Outbox.Node lastRequest = ledger.lastUnwrittenNode();
            if (lastRequest != null) {
                return outbox.addAfter(lastRequest, message);
            }
        }
        return outbox.addControl(message);
    }

    private long sendAwaitingAck(byte[] frame, LivenessTimers.Kind kind, byte @Nullable [] payload) {
        Outbox.Node node = send(frame);
        node.waitId = timers.await(kind, payload);
        return node.waitId;
    }

    private final class UnitSink implements UnitSlicer.Sink {

        private @Nullable CollectionSpace space;

        @Override
        public boolean isEnded() {
            return phase.isEnded();
        }

        @Override
        public UnitSlicer.Stage stage() {
            if (phase.awaitingPreface()) {
                return UnitSlicer.Stage.PREFACE;
            }
            return phase.awaitingFirstFrame() ? UnitSlicer.Stage.FIRST_FRAME : UnitSlicer.Stage.FRAME;
        }

        @Override
        public boolean hasSpace() {
            return Endpoint.this.hasSpace(Objects.requireNonNull(space));
        }

        @Override
        public void prefaceReceived() {
            phase.prefaceVerified();
            sendHello();
        }

        @Override
        public void firstFrameVerified() {
            phase.firstFrameVerified();
        }

        @Override
        public void frame(byte[] bytes, int off, int size) {
            handle(bytes, off, size);
        }

        @Override
        public void closeWithoutClose() {
            Endpoint.this.closeWithoutClose();
        }

        @Override
        public void badLength(byte[] bytes, int off) {
            rejectBadLength(bytes, off);
        }

        @Override
        public void connectionError() {
            Endpoint.this.connectionError();
        }
    }

    private static final class DeferredPing {
        final byte[] payload;

        final long beforeTicket;

        DeferredPing(byte[] payload, long beforeTicket) {
            this.payload = payload;
            this.beforeTicket = beforeTicket;
        }
    }

    private static final class PeerLimits {
        final long maxConcurrentStreams;
        final long maxFrameSize;

        PeerLimits(long maxConcurrentStreams, long maxFrameSize) {
            this.maxConcurrentStreams = maxConcurrentStreams;
            this.maxFrameSize = maxFrameSize;
        }
    }
}
