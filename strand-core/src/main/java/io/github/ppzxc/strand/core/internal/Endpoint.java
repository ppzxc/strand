package io.github.ppzxc.strand.core.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * 엔드포인트(서버 또는 클라이언트 역할). 입출력을 하지 않는 순수 상태 기계다. 받은 바이트는 {@link #receive}, 애플리케이션의 호출은
 * {@link #request}·{@link #ping}·{@link #close}, 시간은 {@link #advance}로 넣는다. 그러면 보낼 바이트와 애플리케이션 이벤트가
 * 쌓이고, {@link #drainMessages}와 {@link #drainEvents}로 한 번씩 꺼낸다. 스레드·소켓·실제 시계에 접근하지 않는다.
 */
public final class Endpoint {

    private static final int PREFACE_PREFIX_LENGTH = 6;
    /** 서버는 짝수, 클라이언트는 홀수 번호를 쓰고 첫 번호는 각각 2와 1이다(규격 2.2.5절). */
    private static final int FIRST_SERVER_STREAM_ID = 2;

    private static final int FIRST_CLIENT_STREAM_ID = 1;

    private static final int PING_PAYLOAD_SIZE = 8;
    private static final int MAX_AUTH_MECHANISM_LENGTH = 20;
    private static final int AUTH_ACK_PARAM_SIZE = 6;
    private static final Pattern AUTH_MECHANISM_GRAMMAR = Pattern.compile("[A-Z0-9_-]+");
    private static final int CREDENTIAL_LIFETIME_ID = 0x0001;
    /** AUTH 확인 대기를 PING(8바이트)·HELLO(null)의 대기와 구분하는 표지. */
    private static final byte[] AUTH_WAIT = new byte[0];

    private static final int RESET_PAYLOAD_SIZE = 4;
    private static final int TLV_HEADER_SIZE = 4;
    private static final int TIMEOUT_TAG = 0x0001;
    private static final int TIMEOUT_LENGTH = 4;
    private static final int CLOSE_ERROR_CODE_SIZE = 4;
    private static final int CLOSE_MIN_PAYLOAD_SIZE = 12;
    private static final int IDEMPOTENCY_KEY_TAG = 0x0003;
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 64;
    /** extensions_length의 한도(규격 2.4절). */
    private static final int MAX_EXTENSIONS_LENGTH = 8192;

    private final EndpointConfig config;
    private final List<OutboundMessage> outbox = new ArrayList<>();
    private final List<Event> events = new ArrayList<>();
    /** 대기 중인 스트림 가운데 내가 연 것(결과를 기다리는 내 요청)의 번호와 요청 type. 보낸 순서대로다(규격 1.3절). */
    private final Map<Long, Integer> ownPendingStreams = new LinkedHashMap<>();
    /**
     * 요청 프레임이 아직 {@link #drainMessages()}로 전송 계층에 넘어가지 않은 내 스트림(규격 3.5절 "연결이 끝날 때"). 프레임 하나가 메시지
     * 하나이므로 메시지를 꺼내면 마지막 바이트가 넘어간 것으로 본다.
     */
    private final Map<Long, OutboundMessage> undrainedRequests = new LinkedHashMap<>();
    /** 대기 중인 스트림 가운데 상대가 연 것(아직 응답하지 않은 요청)의 번호와 요청 type. 응답에 같은 type을 쓴다(규격 1.3절, 4.1절). */
    private final Map<Long, Integer> peerPendingStreams = new LinkedHashMap<>();
    /** AUTH 관문을 통과하기 전이라 미뤄 둔 송신(규격 3.3절). */
    private final List<DeferredSend> deferredSends = new ArrayList<>();

    private byte[] receiveBuffer = new byte[0];
    private boolean prefaceChecked;
    private boolean firstFrameChecked;
    /** 상대의 파라미터 HELLO를 받아 내가 ACK를 보냈다(규격 3.3절). */
    private boolean peerHelloAcked;
    /** 내가 보낸 HELLO에 대해 상대가 보낸 ACK를 받았다(규격 3.3절 ACK 관문). */
    private boolean ownHelloAcked;
    /** 서버는 {@code AUTH(ACK)}를 보냈고 클라이언트는 {@code AUTH(ACK)}를 받았다(규격 3.3절 AUTH 관문). */
    private boolean authGatePassed;
    /** 상대가 HELLO로 알린 {@code max_concurrent_streams}. 알리지 않았으면 기본값이다(규격 3.2.1절). */
    private long peerMaxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
    /** 상대가 HELLO로 알린 {@code max_frame_size}. 알리지 않았으면 기본값이다(규격 3.2.1절). */
    private long peerMaxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;
    /** CLOSE 교환: CLOSE를 보냈거나 받았다(규격 1.3절, 3.5절). */
    private boolean closeExchanged;
    /** 상대의 CLOSE를 이미 받았다. 처음 받은 CLOSE의 {@code last_stream_id}만 쓴다(규격 3.5절). */
    private boolean closeReceived;
    /** 요청 프레임을 전송 계층에 넘긴 내 스트림 가운데 가장 큰 번호. 상대 CLOSE의 {@code last_stream_id} 검사 기준이다(규격 3.5절). */
    private long highestSentOwnStreamId;

    /** 전송 계층이 닫혀 연결이 끝났다(규격 1.3절 연결이 끝남). */
    private boolean transportClosed;

    private long now;
    /** 서버의 AUTH 대기가 끝나는 시각. 대기 중이 아니면 null이다(규격 3.3절 AUTH 대기). */
    private @Nullable Long authDeadline;
    /** ACK를 기다리는 HELLO와 PING들. 보낸 순서대로다(규격 3.3절). */
    private final List<AckWait> ackWaits = new ArrayList<>();
    /** 마지막으로 프레임을 받은 가상 시각(규격 3.4.1절 유휴 판단). */
    private long lastReceivedAt;

    private long idlePingCount;

    private long nextStreamId;
    /** 내 번호 공간의 홀짝: 클라이언트는 1, 서버는 0이다(규격 2.2.5절). */
    private final int ownParity;

    private final boolean server;
    /** 지금까지 쓰인 상대 공간 번호 중 가장 큰 것(규격 2.2.5절). */
    private long highestPeerStreamId;

    private Endpoint(boolean server, EndpointConfig config) {
        this.config = config;
        this.nextStreamId = config.firstStreamId(server ? FIRST_SERVER_STREAM_ID : FIRST_CLIENT_STREAM_ID);
        this.ownParity = server ? 0 : 1;
        this.server = server;
    }

    /**
     * 서버 역할 엔드포인트를 만든다. 연결 서문을 받으면 HELLO를 보낸다.
     *
     * @param config 엔드포인트의 설정
     * @return 연결 서문을 기다리는 엔드포인트
     */
    public static Endpoint server(EndpointConfig config) {
        return new Endpoint(true, config);
    }

    /**
     * 클라이언트 역할 엔드포인트를 만든다. 만들자마자 연결 서문과 HELLO를 보낼 바이트로 쌓는다.
     *
     * @param config 엔드포인트의 설정
     * @return 서버 HELLO를 기다리는 엔드포인트
     */
    public static Endpoint client(EndpointConfig config) {
        Endpoint endpoint = new Endpoint(false, config);
        endpoint.prefaceChecked = true;
        endpoint.send(Frames.preface());
        endpoint.sendHello();
        return endpoint;
    }

    /**
     * 상대에게서 받은 바이트를 넣는다. 프레임 경계와 무관하게 어느 위치에서 잘라도 된다.
     *
     * @param bytes 받은 바이트
     */
    public void receive(byte[] bytes) {
        if (config.isWebSocket()) {
            throw new IllegalStateException("WebSocket 엔드포인트는 receiveMessage로 입력한다");
        }
        if (transportClosed) {
            return;
        }
        receiveBuffer = Frames.concat(receiveBuffer, bytes);
        if (!prefaceChecked && !checkPreface()) {
            return;
        }
        while (!transportClosed && readFrame()) {
            // 한 프레임씩 처리한다.
        }
    }

    /**
     * WebSocket 메시지 하나를 넣는다. 서문은 첫 바이너리 메시지, 그 뒤로는 메시지 하나가 프레임 하나다(규격 1.2절). 바이너리가 아닌
     * 메시지, 크기가 {@code 4 + length}가 아닌 메시지는 연결 오류다. type이 CLOSE이면 크기가 어긋나도 받은 CLOSE로 본다.
     *
     * @param kind 메시지 종류
     * @param message 메시지 내용
     */
    public void receiveMessage(WebSocketMessageKind kind, byte[] message) {
        if (!config.isWebSocket()) {
            throw new IllegalStateException("바이트 스트림 엔드포인트는 receive로 입력한다");
        }
        if (transportClosed) {
            return;
        }
        if (!prefaceChecked) {
            if (kind == WebSocketMessageKind.BINARY
                    && message.length == Frames.preface().length
                    && matchesPrefacePrefix(message)) {
                prefaceChecked = true;
                sendHello();
            } else {
                closeTransport();
            }
            return;
        }
        if (!firstFrameChecked && !checkFirstMessage(kind, message)) {
            closeTransport();
            return;
        }
        if (kind != WebSocketMessageKind.BINARY || message.length < Frames.TYPE_END) {
            connectionError();
            return;
        }
        long length = Frames.readUnsignedInt(message, 0);
        if (length < Frames.MIN_LENGTH
                || length > config.maxFrameSize() - Frames.LENGTH_FIELD_SIZE
                || message.length != Frames.LENGTH_FIELD_SIZE + length) {
            rejectBadLength(message);
            return;
        }
        handle(message);
    }

    /**
     * 가상 시계를 앞으로 보낸다. 확인 대기 시간이 지나도록 ACK가 없으면 연결 오류로 닫는다.
     *
     * @param ms 밀리초
     */
    public void advance(long ms) {
        if (transportClosed) {
            return;
        }
        now += ms;
        if (authDeadline != null && now > authDeadline) {
            closeWithError(ErrorCode.UNAUTHENTICATED);
            return;
        }
        for (AckWait wait : ackWaits) {
            if (wait.started && now > wait.deadline) {
                // 닫으면 대기 목록이 비므로 반복을 이어 가지 않고 바로 끝낸다.
                closeWithError(ErrorCode.UNRESPONSIVE);
                return;
            }
        }
        sendIdlePingIfDue();
    }

    /**
     * 유휴 간격 동안 받은 프레임이 없고 ACK를 기다리는 PING이 없으면 PING을 보낸다(규격 3.4.1절). 간격이 설정돼 있지 않으면 보내지 않는다.
     * 보내는 때는 규격 3.3절의 송신 규칙을 따라 AUTH 관문을 통과한 뒤에만 보낸다. 종료 중에도 보낸다.
     */
    private void sendIdlePingIfDue() {
        long idle = config.idleIntervalMs();
        if (idle <= 0 || !authGatePassed || now - lastReceivedAt < idle || hasOutstandingPing()) {
            return;
        }
        byte[] payload = new byte[PING_PAYLOAD_SIZE];
        for (int i = PING_PAYLOAD_SIZE - 1; i >= 0; i--) {
            payload[i] = (byte) (idlePingCount >>> (8 * (PING_PAYLOAD_SIZE - 1 - i)));
        }
        idlePingCount++;
        sendPing(payload);
    }

    private boolean hasOutstandingPing() {
        for (AckWait wait : ackWaits) {
            if (wait.payload != null && wait.payload.length == PING_PAYLOAD_SIZE) {
                return true;
            }
        }
        return false;
    }

    /**
     * 애플리케이션이 요청을 보내게 한다. AUTH 관문을 통과하기 전이면 그 뒤로 미룬다(규격 3.3절). CLOSE를 보냈거나 받은 뒤,
     * 번호 소진(규격 2.2.5절), 상대가 알린 {@code max_concurrent_streams} 도달(규격 3.2.1절)이면 프레임을 보내지 않고 로컬에서
     * 실패시킨다. 이때 스트림 번호 없는 {@link Event.Kind#RESULT} 이벤트(status {@code UNAVAILABLE})가 한 번 나온다. 한도에 닿은
     * 요청은 대기시키지 않고 즉시 실패시키며 스트림 번호를 쓰지 않는다.
     *
     * @param type 애플리케이션 {@code type}(256 이상)
     * @param payload 본문
     * @throws IllegalArgumentException 지금 보낼 수 있는 요청의 프레임이 상대가 알린 {@code max_frame_size}를 넘을 때. 규격은 그런
     *     프레임을 보내지 못하게 하고(MUST NOT, 3.2.1절) 로컬 처리를 정하지 않으므로, 아무것도 보내지 않고 스트림 번호도 쓰지 않는다.
     *     AUTH 관문 전이라 미룬 요청은 호출자가 돌아간 뒤에 보내므로 던지지 않고, 보낼 때 넘으면 로컬 {@code UNAVAILABLE}로
     *     실패시킨다.
     */
    public void request(int type, byte[] payload) {
        if (transportClosed) {
            failLocally();
            return;
        }
        if (authGatePassed) {
            if (sendRequest(type, payload) == RequestOutcome.OVERSIZE) {
                throw oversizeFrame("요청");
            }
        } else {
            deferredSends.add(DeferredSend.request(() -> {
                if (sendRequest(type, payload) == RequestOutcome.OVERSIZE) {
                    failLocally();
                }
            }));
        }
    }

    /**
     * 애플리케이션이 상대의 요청에 응답하게 한다. 스트림이 끝나고 종료 중이었다면 닫는다. 대기 중이 아닌 스트림이면 아무것도 하지 않는다.
     *
     * @param streamId 상대가 연 스트림 번호
     * @param status 규격 2.5.1절의 status 값
     * @param payload 본문
     * @throws IllegalArgumentException 프레임이 상대가 알린 {@code max_frame_size}를 넘을 때. 규격은 그런 프레임을 보내지 못하게 하고
     *     (MUST NOT, 3.2.1절) 로컬 처리를 정하지 않으므로, 아무것도 보내지 않고 스트림도 그대로 둔다. 호출자는 줄인 payload로 다시 응답할 수 있다.
     */
    public void respond(long streamId, int status, byte[] payload) {
        Integer type = transportClosed ? null : peerPendingStreams.get(streamId);
        if (type == null) {
            return;
        }
        byte[] frame = Frames.response(type, streamId, status, payload);
        if (exceedsPeerFrameSize(frame)) {
            throw oversizeFrame("응답");
        }
        peerPendingStreams.remove(streamId);
        send(frame);
        closeTransportIfNoPendingStreams();
    }

    /**
     * 애플리케이션이 자기 요청을 취소하게 한다. RESET(CANCEL)을 보내고 로컬 {@code CANCELLED} 결과를 넘긴다(규격 4.4절). 대기 중이
     * 아닌 스트림이면 아무것도 하지 않는다.
     *
     * @param streamId 내가 연 스트림 번호
     */
    public void cancel(long streamId) {
        if (transportClosed || ownPendingStreams.remove(streamId) == null) {
            return;
        }
        // 요청 프레임은 이미 쓰기 대기열에 있고 끊지 않고 마저 쓴 뒤 RESET을 보낸다(규격 4.4절). 마지막 바이트를 넘기기 전 취소는 #190이 다룬다.
        undrainedRequests.remove(streamId);
        send(Frames.reset(streamId, ErrorCode.CANCEL));
        events.add(Event.result(streamId, Status.CANCELLED, Certainty.UNKNOWN));
        closeTransportIfNoPendingStreams();
    }

    /**
     * 애플리케이션이 PING을 보내게 한다. AUTH 관문을 통과하기 전이면 그 뒤로 미룬다(규격 3.3절).
     *
     * @param payload 정확히 8바이트
     */
    public void ping(byte[] payload) {
        if (!transportClosed) {
            sendAfterAuthGate(DeferredSend.ping(() -> sendPing(payload)));
        }
    }

    /**
     * 애플리케이션이 연결을 닫게 한다. 이미 CLOSE를 보냈거나 받았다면 아무것도 하지 않는다(규격 3.5절).
     *
     * @param errorCode 규격 2.5.2절의 error_code 값
     */
    public void close(int errorCode) {
        if (transportClosed || closeExchanged) {
            return;
        }
        if (errorCode != ErrorCode.NO_ERROR) {
            refuseUnhandedRequests();
        }
        send(Frames.close(errorCode, highestPeerStreamId));
        closeExchanged = true;
        endQueuedRequests();
        closeTransportUnlessClosing(errorCode);
    }

    /**
     * 전송 계층이 CLOSE 없이 끊겼음을 알리는 입력이다(TCP EOF·RST, WebSocket close). 대기 중인 요청을 로컬 UNAVAILABLE로 끝내고 닫는다.
     * 이미 닫혔다면 아무것도 하지 않는다(규격 3.5절).
     */
    public void disconnect() {
        closeTransport();
    }

    /**
     * 종료 중인 연결(CLOSE를 보냈거나 받음)에서 애플리케이션이 더 기다리지 않고 닫기로 했을 때 부른다. 받아들였지만 아직 애플리케이션에
     * 넘기지 않은 상대의 요청을 {@code RESET(REFUSED)}로 돌려보낸 뒤 전송 계층을 닫는다(규격 3.5.1절). 종료 중이 아니거나 이미 닫혔으면
     * 아무것도 하지 않는다.
     */
    public void abortClosing() {
        if (transportClosed || !closeExchanged) {
            return;
        }
        refuseUnhandedRequests();
        closeTransportAfterNoError();
    }

    /**
     * 지금까지 쌓인, 상대에게 보낼 메시지를 보낸 순서대로 꺼낸다. 프레임 하나가 메시지 하나다. 바이트 스트림 전송은 메시지를 이어 붙여 보내면
     * 된다.
     *
     * @return 보낼 메시지. 없으면 빈 목록
     */
    public List<OutboundMessage> drainMessages() {
        List<OutboundMessage> drained = new ArrayList<>(outbox);
        outbox.clear();
        // 확인 대기는 프레임을 전송 계층에 넘긴 때부터 센다(규격 3.3절). 넘긴 때는 꺼낸 때로 본다.
        for (AckWait wait : ackWaits) {
            if (!wait.started && drained.contains(wait.message)) {
                wait.start(now + config.ackTimeoutMs());
            }
        }
        for (long streamId : undrainedRequests.keySet()) {
            highestSentOwnStreamId = Math.max(highestSentOwnStreamId, streamId);
        }
        undrainedRequests.clear();
        return drained;
    }

    /**
     * 지금까지 쌓인 애플리케이션 이벤트를 꺼낸다.
     *
     * @return 이벤트 목록. 없으면 빈 목록
     */
    public List<Event> drainEvents() {
        List<Event> drained = new ArrayList<>(events);
        events.clear();
        return drained;
    }

    /** 서문의 앞 6바이트를 비교한다(규격 3.1절). 판정할 만큼 모였으면 true. 서문은 8바이트를 모두 받은 뒤 건너뛴다. */
    private boolean checkPreface() {
        byte[] preface = Frames.preface();
        if (!matchesPrefacePrefix(receiveBuffer)) {
            closeTransport();
            return false;
        }
        if (receiveBuffer.length < preface.length) {
            return false;
        }
        receiveBuffer = Arrays.copyOfRange(receiveBuffer, preface.length, receiveBuffer.length);
        prefaceChecked = true;
        sendHello();
        return true;
    }

    /** 서문의 앞 6바이트와, 받은 만큼의 앞부분이 일치하는가. */
    private static boolean matchesPrefacePrefix(byte[] bytes) {
        byte[] preface = Frames.preface();
        int compared = Math.min(bytes.length, PREFACE_PREFIX_LENGTH);
        for (int i = 0; i < compared; i++) {
            if (bytes[i] != preface[i]) {
                return false;
            }
        }
        return true;
    }

    /** 첫 프레임의 헤더가 첫 프레임 검사(규격 3.2.4절)를 통과하는가: HELLO이고 stream_id 0, flags 0. */
    private static boolean isFirstFrameHeader(byte[] bytes) {
        return Frames.typeOf(bytes) == FrameType.HELLO && Frames.streamIdOf(bytes) == 0 && Frames.flagsOf(bytes) == 0;
    }

    /** WebSocket의 첫 프레임 메시지 검사(규격 1.2절, 3.2.4절): 바이너리이고 헤더를 읽을 수 있고 HELLO여야 한다. 통과하면 첫 프레임 검사를 마친 것으로 기록한다. */
    private boolean checkFirstMessage(WebSocketMessageKind kind, byte[] message) {
        boolean valid = kind == WebSocketMessageKind.BINARY
                && message.length >= Frames.HEADER_SIZE
                && Frames.readUnsignedInt(message, 0) >= Frames.MIN_LENGTH
                && isFirstFrameHeader(message);
        firstFrameChecked = valid;
        return valid;
    }

    /** 길이가 어긋난 프레임: type이 CLOSE이면 받은 CLOSE로 보고 CLOSE 없이 닫고, 아니면 연결 오류다(규격 3.5절, 5.1절). */
    private void rejectBadLength(byte[] bytes) {
        if (Frames.typeOf(bytes) == FrameType.CLOSE) {
            closeReceivedMalformed();
        } else {
            connectionError();
        }
    }

    /** 프레임 하나를 읽어 처리한다. 더 읽을 수 있으면 true. */
    private boolean readFrame() {
        if (receiveBuffer.length < Frames.LENGTH_FIELD_SIZE) {
            return false;
        }
        long length = Frames.readUnsignedInt(receiveBuffer, 0);
        if (!firstFrameChecked) {
            // 첫 프레임 검사(규격 3.2.4절): 헤더를 읽을 수 없거나 HELLO가 아니면 아무것도 보내지 않고 닫는다.
            if (length < Frames.MIN_LENGTH) {
                closeTransport();
                return false;
            }
            if (receiveBuffer.length < Frames.HEADER_SIZE) {
                return false;
            }
            if (!isFirstFrameHeader(receiveBuffer)) {
                closeTransport();
                return false;
            }
            firstFrameChecked = true;
        }
        if (length < Frames.MIN_LENGTH || length > config.maxFrameSize() - Frames.LENGTH_FIELD_SIZE) {
            if (receiveBuffer.length < Frames.TYPE_END) {
                // type을 읽을 때까지 기다린다. CLOSE이면 받은 CLOSE로 보고 CLOSE 없이 닫아야 하므로 도착 단위에 따라 결과가 달라지면 안 된다.
                return false;
            }
            rejectBadLength(receiveBuffer);
            return false;
        }
        int frameSize = (int) (Frames.LENGTH_FIELD_SIZE + length);
        if (receiveBuffer.length < frameSize) {
            return false;
        }
        byte[] frame = Arrays.copyOfRange(receiveBuffer, 0, frameSize);
        receiveBuffer = Arrays.copyOfRange(receiveBuffer, frameSize, receiveBuffer.length);
        handle(frame);
        return true;
    }

    private void handle(byte[] frame) {
        lastReceivedAt = now;
        int type = Frames.typeOf(frame);
        int extensionsLength = Frames.extensionsLengthOf(frame);
        long streamId = Frames.streamIdOf(frame);
        if (type == FrameType.CLOSE) {
            handleClose(frame, extensionsLength, streamId);
            return;
        }
        if (extensionsLength > frame.length - Frames.HEADER_SIZE) {
            connectionError();
            return;
        }
        if (type >= FrameType.FIRST_APPLICATION) {
            handleApplication(frame, type, extensionsLength, streamId);
            return;
        }
        if (FrameType.isReserved(type)) {
            // 예약 제어 타입은 확장 영역 검사만 받고 무시한다(규격 2.2.2절, 2.4.3절). stream_id와 ACK 관문은 보지 않는다.
            if (extensionsLength != 0) {
                connectionError();
            }
            return;
        }
        if (extensionsLength != 0 || (type != FrameType.RESET && streamId != 0)) {
            connectionError();
            return;
        }
        if (!ownHelloAcked && type != FrameType.HELLO) {
            // ACK 관문(규격 3.3절): 내 HELLO의 ACK를 받기 전에는 HELLO와 CLOSE만 받는다. AUTH는 예약이 아니라서 여기에 걸린다.
            connectionError();
            return;
        }
        if (!authGatePassed && type != FrameType.HELLO && type != FrameType.AUTH) {
            // AUTH 관문(규격 3.3절): 통과 전에는 HELLO·AUTH·CLOSE만 받는다. ACK 관문이 먼저 적용된다.
            connectionError();
            return;
        }
        byte[] payload = Arrays.copyOfRange(frame, Frames.HEADER_SIZE, frame.length);
        boolean ack = (Frames.flagsOf(frame) & Flags.ACK) != 0;
        if (type == FrameType.HELLO) {
            handleHello(ack, payload);
        } else if (type == FrameType.PING) {
            handlePing(ack, payload);
        } else if (type == FrameType.RESET) {
            handleReset(streamId, payload);
        } else if (type == FrameType.AUTH) {
            handleAuth(ack, payload);
        }
    }

    /**
     * 받은 RESET(규격 4.4절). {@code stream_id}가 0이거나 최상위 비트가 1이거나 payload가 4바이트가 아니면 연결 오류다. 대기 중인
     * 스트림이면 끝내고, 내 요청이었다면 로컬 결과를 넘긴다. 대기 중이 아니면 조용히 버린다(규격 4.3절). flags는 모두 미정의라 보지
     * 않는다(규격 2.3절).
     */
    private void handleReset(long streamId, byte[] payload) {
        if (isInvalidStreamId(streamId) || payload.length != RESET_PAYLOAD_SIZE) {
            connectionError();
            return;
        }
        if (isOwnSpace(streamId)) {
            if (ownPendingStreams.remove(streamId) != null) {
                long errorCode = Frames.readUnsignedInt(payload, 0);
                // 거절한 요청은 처리를 시작하지 않았다(규격 4.4절 결과 표).
                Certainty certainty = errorCode == ErrorCode.REFUSED ? Certainty.NOT_PROCESSED : Certainty.UNKNOWN;
                events.add(Event.result(streamId, Status.forReset(errorCode), certainty));
            }
        } else {
            peerPendingStreams.remove(streamId);
        }
        closeTransportIfNoPendingStreams();
    }

    /** {@code stream_id}가 0이거나 최상위 비트가 1이다(규격 2.2.5절). 최상위 비트가 1이면 {@code long}으로 음수다. */
    private static boolean isInvalidStreamId(long streamId) {
        return !Frames.isValidStreamId(streamId);
    }

    private boolean isOwnSpace(long streamId) {
        return (streamId & 1) == ownParity;
    }

    /**
     * 애플리케이션 프레임. 확장 영역 형식 위반은 요청인지 응답인지, type을 아는지와 상관없이 연결 오류다(규격 2.4.1절). 내 번호 공간의
     * 프레임은 응답이고 상대 공간의 프레임은 새 요청이다(규격 4.2절).
     */
    private void handleApplication(byte[] frame, int type, int extensionsLength, long streamId) {
        // 연결 오류 집합은 검사 순서가 없다(규격 5.1절 3번). 확장 영역 형식, 상대 ACK 전의 프레임(3.3절), 잘못된 stream_id.
        Extensions extensionsResult = extensionsLength > MAX_EXTENSIONS_LENGTH
                ? Extensions.MALFORMED
                : scanExtensions(frame, extensionsLength);
        if (extensionsResult == Extensions.MALFORMED
                || !ownHelloAcked
                || !authGatePassed
                || isInvalidStreamId(streamId)) {
            connectionError();
        } else if (isOwnSpace(streamId)) {
            handleResponse(frame, streamId);
        } else {
            handleRequest(type, extensionsResult, streamId);
        }
    }

    /**
     * 상대의 새 요청(규격 5.1절 5번). (가) 거절, (나) 모르는 type, (다) 확장 값 오류 순서로 보고, 모두 지나면 애플리케이션에 넘긴다.
     * 거절한 요청과 오류로 답한 요청은 대기 스트림에 넣지 않지만 번호는 쓰인 것으로 센다(규격 4.5절, 5.1절).
     */
    private void handleRequest(int type, Extensions extensionsResult, long streamId) {
        if (streamId <= highestPeerStreamId) {
            // 번호 역행: 재사용한 번호, 건너뛰고 나중에 온 번호, 대기 중인 스트림의 번호 모두 연결 오류다(규격 4.2.1절).
            connectionError();
            return;
        }
        highestPeerStreamId = streamId;
        if (closeExchanged) {
            // CLOSE를 보냈거나 받은 뒤의 새 요청은 끝까지 읽고 조용히 버린다. 어떤 프레임도 보내지 않는다(규격 3.5절).
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
            events.add(Event.requestReceived(streamId, type));
        }
    }

    /** 확장 영역 검사 결과. 형식 위반이 값 위반보다 우선한다. */
    private enum Extensions {
        MALFORMED,
        INVALID_VALUE,
        VALID
    }

    /**
     * 확장 영역을 한 번 훑어 검사한다. 형식(규격 2.4.1절)은 항목 길이의 합이 {@code extensions_length}와 같고 같은 tag가 없는 것이고, 값(규격
     * 2.4.2절)은 timeout이 4바이트, idempotency-key가 1~64바이트인 것이다.
     */
    private static Extensions scanExtensions(byte[] frame, int extensionsLength) {
        Set<Integer> tags = new HashSet<>();
        boolean valuesValid = true;
        int at = Frames.HEADER_SIZE;
        int end = at + extensionsLength;
        while (at < end) {
            if (end - at < TLV_HEADER_SIZE) {
                return Extensions.MALFORMED;
            }
            int tag = Frames.readUnsignedShort(frame, at);
            int length = Frames.readUnsignedShort(frame, at + 2);
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

    /** 내 요청의 응답. 대기 중인 요청이면 결과를 애플리케이션에 넘긴다. 종료 중이고 마지막 요청이었다면 닫는다(규격 3.5절). */
    private void handleResponse(byte[] frame, long streamId) {
        Integer requestType = ownPendingStreams.remove(streamId);
        if (requestType == null) {
            // 대기 중이 아닌 스트림의 프레임은 조용히 버린다(규격 4.3절).
            return;
        }
        // 응답 type이 요청과 다르면 status와 payload를 버리고 로컬 INTERNAL을 넘긴다. 와이어로는 아무것도 보내지 않는다(규격 4.1절).
        int status = Frames.typeOf(frame) == requestType
                ? Status.normalize(Frames.statusOf(frame))
                : Event.RESPONSE_TYPE_MISMATCH;
        events.add(Event.result(streamId, status, Certainty.RESPONSE));
        closeTransportIfNoPendingStreams();
    }

    /** CLOSE를 보냈거나 받아 종료 중인데 대기 중인 스트림이 모두 끝났으면 닫는다(규격 3.5절). */
    private void closeTransportIfNoPendingStreams() {
        if (closeExchanged && ownPendingStreams.isEmpty() && peerPendingStreams.isEmpty()) {
            closeTransportAfterNoError();
        }
    }

    private void handleHello(boolean ack, byte[] payload) {
        if (ack) {
            // 합친 프레임(ACK이면서 payload가 있음)과 두 번째 ACK는 연결 오류다(규격 3.2.5절).
            if (payload.length != 0 || ownHelloAcked) {
                connectionError();
                return;
            }
            ownHelloAcked = true;
            endOldestWait(null);
            return;
        }
        PeerLimits peer = peerHelloAcked ? null : peerLimitsOf(payload);
        if (peer == null) {
            connectionError();
            return;
        }
        peerHelloAcked = true;
        peerMaxConcurrentStreams = peer.maxConcurrentStreams;
        peerMaxFrameSize = peer.maxFrameSize;
        send(Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        if (server) {
            authDeadline = now + config.authTimeoutMs();
        } else {
            // 클라이언트는 서버 HELLO에 ACK를 보낸 직후 AUTH를 한 번 보낸다. 서버 ACK를 기다리지 않는다(규격 3.3절 4번).
            sendAwaitingAck(Frames.auth(config.authMechanism(), config.authCredentials()), AUTH_WAIT);
        }
    }

    /**
     * 받은 {@code AUTH}와 {@code AUTH(ACK)}(규격 3.3.1절). 서버는 첫 {@code AUTH}를 애플리케이션에 넘기고, 클라이언트는 첫
     * {@code AUTH(ACK)}에서 {@code credential_lifetime}을 알린다. 방향 위반, 두 번째 {@code AUTH}·{@code AUTH(ACK)},
     * 형태 위반은 연결 오류다.
     */
    private void handleAuth(boolean ack, byte[] payload) {
        if (server && !ack && !authGatePassed) {
            handleServerAuth(payload);
        } else if (!server && ack && peerHelloAcked && !authGatePassed) {
            handleClientAuthAck(payload);
        } else {
            connectionError();
        }
    }

    private void handleServerAuth(byte[] payload) {
        authDeadline = null;
        int mechanismLength = payload.length == 0 ? 0 : payload[0] & 0xFF;
        if (mechanismLength < 1
                || mechanismLength > MAX_AUTH_MECHANISM_LENGTH
                || payload.length < 1 + mechanismLength) {
            connectionError();
            return;
        }
        String mechanism = new String(payload, 1, mechanismLength, StandardCharsets.US_ASCII);
        if (!AUTH_MECHANISM_GRAMMAR.matcher(mechanism).matches()) {
            connectionError();
            return;
        }
        byte[] credentials = Arrays.copyOfRange(payload, 1 + mechanismLength, payload.length);
        AuthDecision decision = config.authenticator().authenticate(mechanism, credentials);
        if (decision != AuthDecision.PASS) {
            // 거부와 모르는 메커니즘은 UNAUTHENTICATED, 일시 실패는 INTERNAL_ERROR. 이유는 비운다(규격 3.3.1절).
            closeWithError(decision == AuthDecision.REJECT ? ErrorCode.UNAUTHENTICATED : ErrorCode.INTERNAL_ERROR);
            return;
        }
        send(Frames.control(FrameType.AUTH, Flags.ACK, new byte[0]));
        openAuthGate();
    }

    private void handleClientAuthAck(byte[] payload) {
        if (payload.length % AUTH_ACK_PARAM_SIZE != 0) {
            connectionError();
            return;
        }
        OptionalLong lifetime = OptionalLong.empty();
        Set<Integer> seen = new HashSet<>();
        for (int at = 0; at < payload.length; at += AUTH_ACK_PARAM_SIZE) {
            if (!seen.add(Frames.readUnsignedShort(payload, at))) {
                connectionError();
                return;
            }
            // 모르는 identifier는 무시한다(규격 3.3.1절).
            if (Frames.readUnsignedShort(payload, at) == CREDENTIAL_LIFETIME_ID) {
                lifetime = OptionalLong.of(Frames.readUnsignedInt(payload, at + 2));
            }
        }
        endOldestWait(AUTH_WAIT);
        events.add(Event.authenticated(lifetime));
        openAuthGate();
    }

    /** AUTH 관문을 연다. 미뤄 둔 송신을 보낸다. */
    private void openAuthGate() {
        authGatePassed = true;
        // 보내다가 닫히면 closeTransport가 남은 요청을 closed 이벤트 이전에 실패시키고 목록을 비운다. CLOSE를 보낸 뒤에는 PING을 보내지 않는다.
        while (!deferredSends.isEmpty()) {
            DeferredSend deferred = deferredSends.remove(0);
            if (deferred.request || !closeExchanged) {
                deferred.sending.run();
            }
        }
    }

    /**
     * HELLO payload를 한 번 훑는다(규격 3.2.3절, 3.2.5절). 6의 배수, identifier 중복 없음, max_frame_size 범위를 검증하고, 유효하면 상대가 알린
     * {@code max_concurrent_streams}와 {@code max_frame_size}(없으면 각 기본값)를 돌려주고, 유효하지 않으면 null을 돌려준다.
     */
    private static @Nullable PeerLimits peerLimitsOf(byte[] payload) {
        if (payload.length % HelloParams.HELLO_PARAM_SIZE != 0) {
            return null;
        }
        long maxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
        long maxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;
        Set<Integer> seen = new HashSet<>();
        for (int at = 0; at < payload.length; at += HelloParams.HELLO_PARAM_SIZE) {
            int identifier = Frames.readUnsignedShort(payload, at);
            if (!seen.add(identifier)) {
                return null;
            }
            long value = Frames.readUnsignedInt(payload, at + 2);
            if (identifier == HelloParams.MAX_FRAME_SIZE_ID
                    && (value < HelloParams.MAX_FRAME_SIZE_LOWER_LIMIT
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

    private void handlePing(boolean ack, byte[] payload) {
        if (payload.length != PING_PAYLOAD_SIZE) {
            connectionError();
        } else if (ack) {
            // 보낸 PING과 맞춰 보는 일은 로컬 몫이고, 맞지 않는 ACK는 무시한다(규격 3.4절).
            endOldestWait(payload);
        } else {
            send(Frames.control(FrameType.PING, Flags.ACK, payload));
        }
    }

    /** 받은 CLOSE(규격 3.5절). 형태가 잘못돼도 받은 CLOSE로 치고, 그때는 오류 CLOSE처럼 CLOSE 없이 닫는다. */
    private void handleClose(byte[] frame, int extensionsLength, long streamId) {
        int payloadSize = frame.length - Frames.HEADER_SIZE - extensionsLength;
        if (extensionsLength != 0 || streamId != 0 || payloadSize < CLOSE_MIN_PAYLOAD_SIZE) {
            closeReceivedMalformed();
            return;
        }
        long lastStreamId = Frames.readLong(frame, Frames.HEADER_SIZE + CLOSE_ERROR_CODE_SIZE);
        if (lastStreamId != 0
                && (!isOwnSpace(lastStreamId) || lastStreamId < 0 || lastStreamId > highestSentOwnStreamId)) {
            // 최상위 비트가 1이면 long으로 음수이고 보낸 번호보다 크다. 이 값은 쓰지 않는다.
            closeReceivedMalformed();
            return;
        }
        closeExchanged = true;
        if (!closeReceived) {
            closeReceived = true;
            endRequestsAfter(lastStreamId);
        }
        endQueuedRequests();
        closeTransportUnlessClosing(Frames.readUnsignedInt(frame, Frames.HEADER_SIZE));
    }

    /**
     * CLOSE의 {@code last_stream_id}보다 큰 번호의 대기 중인 내 요청을 지금 로컬 UNAVAILABLE(처리 안 됨)로 끝낸다(규격 3.5절).
     * 상대가 처리하지 않은 요청이므로 아직 넘기지 못한 프레임도 보내지 않는다.
     */
    private void endRequestsAfter(long lastStreamId) {
        Iterator<Long> pending = ownPendingStreams.keySet().iterator();
        while (pending.hasNext()) {
            long streamId = pending.next();
            if (streamId > lastStreamId) {
                pending.remove();
                OutboundMessage unsent = undrainedRequests.remove(streamId);
                if (unsent != null) {
                    outbox.removeIf(message -> message == unsent);
                }
                events.add(Event.result(streamId, Status.UNAVAILABLE, Certainty.NOT_PROCESSED));
            }
        }
    }

    /**
     * CLOSE를 보내거나 받은 즉시, 번호를 받지 못하고 기다리던 요청(AUTH 관문 전에 미룬 요청)을 로컬 UNAVAILABLE(처리 안 됨)로 끝낸다(규격
     * 3.5절). 미룬 PING은 그대로 둔다.
     */
    private void endQueuedRequests() {
        deferredSends.removeIf(deferred -> {
            if (deferred.request) {
                failLocally();
            }
            return deferred.request;
        });
    }

    /**
     * 끊기 전에 돌려보낸다(규격 3.5.1절, SHOULD). 받아들였지만 아직 애플리케이션이 이벤트로 받지 않은 상대의 요청을 {@code
     * RESET(REFUSED)}로 돌려보내고 스트림을 끝낸다. 오류 CLOSE보다 먼저 호출한다.
     */
    private void refuseUnhandedRequests() {
        events.removeIf(event -> {
            if (event.kind() != Event.Kind.REQUEST_RECEIVED || peerPendingStreams.remove(event.streamId()) == null) {
                return false;
            }
            send(Frames.reset(event.streamId(), ErrorCode.REFUSED));
            return true;
        });
    }

    /** 형태가 잘못된 CLOSE를 받았다. 받은 CLOSE로 치고 CLOSE를 보내지 않은 채 닫는다(규격 3.5절). */
    private void closeReceivedMalformed() {
        closeExchanged = true;
        closeTransport();
    }

    /** 오류 CLOSE이거나 대기 중인 스트림이 없으면 닫고, 아니면 대기 중인 스트림이 끝날 때까지 종료 중으로 둔다(규격 3.5절). */
    private void closeTransportUnlessClosing(long errorCode) {
        if (errorCode != ErrorCode.NO_ERROR) {
            closeTransport();
        } else if (ownPendingStreams.isEmpty() && peerPendingStreams.isEmpty()) {
            closeTransportAfterNoError();
        }
    }

    /**
     * {@code NO_ERROR} CLOSE를 보냈거나 받아 끝나는 연결을 닫는다. 호출부는 이 경우에만 이 메서드를 부른다(오류 CLOSE는 곧바로 {@link
     * #closeTransport()}로 닫는다). WebSocket이면 마지막 프레임 뒤에 WebSocket Close(1000)를 낸다(규격 1.2절). 오류 종료의 WebSocket
     * 상태 코드는 규격이 정하지 않아 모델링하지 않는다.
     */
    private void closeTransportAfterNoError() {
        if (!transportClosed && config.isWebSocket()) {
            outbox.add(OutboundMessage.normalWebSocketClose());
        }
        closeTransport();
    }

    /**
     * 연결 오류: CLOSE(PROTOCOL_ERROR)를 보내고 닫는다. 이미 CLOSE를 보냈거나 받았다면 보내지 않는다(규격 3.5절, 5.1절). 어느 쪽이든
     * 이 호출 뒤에는 CLOSE 교환이 된 상태다.
     */
    private void connectionError() {
        closeWithError(ErrorCode.PROTOCOL_ERROR);
    }

    /**
     * 오류 {@code errorCode}의 CLOSE를 보내고 닫는다(연결 오류, 확인 대기 초과, 연결 인증의 거부·일시 실패·대기 초과). 이미 CLOSE를 보냈거나
     * 받았다면 보내지 않는다(규격 3.5절).
     */
    private void closeWithError(int errorCode) {
        refuseUnhandedRequests();
        if (!closeExchanged) {
            send(Frames.close(errorCode, highestPeerStreamId));
        }
        closeExchanged = true;
        closeTransport();
    }

    /**
     * 연결이 끝난다. 결과를 받지 못한 요청마다 로컬 UNAVAILABLE을 한 번씩 넘긴 뒤 closed를 낸다(규격 3.5절). 이후 받은 바이트와
     * 시간은 처리하지 않는다.
     */
    private void closeTransport() {
        if (transportClosed) {
            return;
        }
        transportClosed = true;
        receiveBuffer = new byte[0];
        ackWaits.clear();
        for (long streamId : ownPendingStreams.keySet()) {
            // 요청의 마지막 바이트를 전송 계층에 넘겼는지로 정한다(규격 3.5절). 못 넘긴 프레임은 이후에도 넘기지 않도록 거둔다.
            OutboundMessage unsent = undrainedRequests.get(streamId);
            if (unsent == null) {
                events.add(Event.result(streamId, Status.UNAVAILABLE, Certainty.UNKNOWN));
            } else {
                outbox.removeIf(message -> message == unsent);
                events.add(Event.result(streamId, Status.UNAVAILABLE, Certainty.NOT_PROCESSED));
            }
        }
        undrainedRequests.clear();
        for (DeferredSend deferred : deferredSends) {
            if (deferred.request) {
                failLocally();
            }
        }
        deferredSends.clear();
        ownPendingStreams.clear();
        peerPendingStreams.clear();
        events.add(Event.closed());
    }

    private void sendHello() {
        sendAwaitingAck(Frames.hello(config), null);
    }

    private void sendPing(byte[] payload) {
        sendAwaitingAck(Frames.control(FrameType.PING, 0, payload), payload);
    }

    /** ACK 하나가 끝내는 대기는 가장 먼저 시작한 것 하나다. HELLO 대기는 payload가 null이다. */
    private void endOldestWait(byte @Nullable [] payload) {
        for (Iterator<AckWait> it = ackWaits.iterator(); it.hasNext(); ) {
            if (Arrays.equals(it.next().payload, payload)) {
                it.remove();
                return;
            }
        }
    }

    /** AUTH 관문을 통과한 뒤면 바로, 아니면 통과할 때까지 미뤄서 보낸다. */
    private void sendAfterAuthGate(DeferredSend sending) {
        if (authGatePassed) {
            sending.run();
        } else {
            deferredSends.add(sending);
        }
    }

    /** 요청을 보낸 결과. */
    private enum RequestOutcome {
        /** 요청 프레임을 보냈다. */
        SENT,
        /** 프레임을 열지 못해 로컬에서 실패시켰다(이미 이벤트를 냈다). */
        FAILED_LOCALLY,
        /** 프레임이 상대의 {@code max_frame_size}를 넘어 아무것도 하지 않았다. 규격 3.2.1절: 보내지 않고 번호도 쓰지 않는다. */
        OVERSIZE
    }

    private RequestOutcome sendRequest(int type, byte[] payload) {
        if (failRequestIfRefused()) {
            return RequestOutcome.FAILED_LOCALLY;
        }
        long streamId = nextStreamId;
        byte[] frame = Frames.request(type, streamId, payload);
        if (exceedsPeerFrameSize(frame)) {
            return RequestOutcome.OVERSIZE;
        }
        nextStreamId += 2; // 2^63-1을 넘으면 음수로 넘쳐 소진으로 읽힌다.
        ownPendingStreams.put(streamId, type);
        OutboundMessage message = OutboundMessage.binary(frame);
        outbox.add(message);
        undrainedRequests.put(streamId, message);
        return RequestOutcome.SENT;
    }

    /** 프레임을 열지 못하는 요청이면 로컬에서 실패시키고 {@code true}를 돌려준다. 실패한 요청은 대기시키지 않고 번호도 쓰지 않는다. */
    private boolean failRequestIfRefused() {
        if (closeExchanged) {
            // CLOSE를 보냈거나 받았다(규격 3.5절).
            failLocally();
            return true;
        }
        if (!Frames.isValidStreamId(nextStreamId)) {
            // 번호가 소진됐다(2^63-1을 넘으면 long이 음수로 넘친다). 상대가 알린 한도보다 먼저 본다. 프레임 없이 로컬에서 실패시키고 연결을 닫는다(규격 2.2.5절).
            failLocally();
            close(ErrorCode.NO_ERROR);
            return true;
        }
        if (ownPendingStreams.size() >= peerMaxConcurrentStreams) {
            // 상대가 알린 한도에 닿았다(규격 3.2.1절).
            failLocally();
            return true;
        }
        return false;
    }

    /** 프레임이 상대가 알린 {@code max_frame_size}를 넘는다(규격 3.2.1절). */
    private boolean exceedsPeerFrameSize(byte[] frame) {
        return frame.length > peerMaxFrameSize;
    }

    private IllegalArgumentException oversizeFrame(String kind) {
        return new IllegalArgumentException(kind + " 프레임이 상대의 max_frame_size(" + peerMaxFrameSize + ")를 넘는다");
    }

    /** 스트림을 열기 전에 요청이 로컬에서 실패했다. 스트림 번호 없는 UNAVAILABLE 결과를 넘긴다. */
    private void failLocally() {
        events.add(Event.localUnavailable());
    }

    private void send(byte[] frame) {
        outbox.add(OutboundMessage.binary(frame));
    }

    /** ACK를 기다리는 프레임을 쌓는다. 대기 시간은 {@link #drainMessages}가 이 프레임을 돌려줄 때 시작한다. */
    private void sendAwaitingAck(byte[] frame, byte @Nullable [] payload) {
        OutboundMessage message = OutboundMessage.binary(frame);
        outbox.add(message);
        ackWaits.add(new AckWait(payload, message));
    }

    /** AUTH 관문을 통과할 때까지 미룬 송신 하나. 요청이면 보내지 못하고 끝날 때 로컬에서 실패시킨다. */
    private static final class DeferredSend {
        final boolean request;
        final Runnable sending;

        private DeferredSend(boolean request, Runnable sending) {
            this.request = request;
            this.sending = sending;
        }

        void run() {
            sending.run();
        }

        static DeferredSend request(Runnable sending) {
            return new DeferredSend(true, sending);
        }

        static DeferredSend ping(Runnable sending) {
            return new DeferredSend(false, sending);
        }
    }

    /** 상대 HELLO가 알린 한도. 없는 파라미터는 규격 기본값이다. */
    private static final class PeerLimits {
        final long maxConcurrentStreams;
        final long maxFrameSize;

        PeerLimits(long maxConcurrentStreams, long maxFrameSize) {
            this.maxConcurrentStreams = maxConcurrentStreams;
            this.maxFrameSize = maxFrameSize;
        }
    }

    /** 확인 대기 하나. {@code payload}는 PING이면 보낸 8바이트, HELLO이면 null이다. 기한은 {@code message}를 넘긴 때부터 센다. */
    private static final class AckWait {
        final byte @Nullable [] payload;
        final OutboundMessage message;
        boolean started;
        long deadline;

        AckWait(byte @Nullable [] payload, OutboundMessage message) {
            this.payload = payload;
            this.message = message;
        }

        void start(long deadline) {
            this.started = true;
            this.deadline = deadline;
        }
    }
}
