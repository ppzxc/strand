package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final int MIN_FRAME_LENGTH = 12;
    private static final int LENGTH_SIZE = 4;
    /** type 필드가 끝나는 위치. 길이가 어긋난 프레임도 여기까지는 읽어 type을 본다. */
    private static final int TYPE_END = 6;

    private static final int PING_PAYLOAD_SIZE = 8;
    private static final int RESET_PAYLOAD_SIZE = 4;
    private static final long MAX_STREAM_ID = Integer.MAX_VALUE;
    private static final int TLV_HEADER_SIZE = 4;
    private static final int TIMEOUT_TAG = 0x0001;
    private static final int TIMEOUT_LENGTH = 4;
    private static final int IDEMPOTENCY_KEY_TAG = 0x0003;
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 64;
    /** extensions_length의 한도(규격 2.4절). */
    private static final int MAX_EXTENSIONS_LENGTH = 8192;

    private final EndpointConfig config;
    private final List<OutboundMessage> outbox = new ArrayList<>();
    private final List<Event> events = new ArrayList<>();
    /** 대기 중인 스트림 가운데 내가 연 것(결과를 기다리는 내 요청)의 번호와 요청 type. 보낸 순서대로다(규격 1.3절). */
    private final Map<Integer, Integer> ownPendingStreams = new LinkedHashMap<>();
    /** 대기 중인 스트림 가운데 상대가 연 것(아직 응답하지 않은 요청)의 번호와 요청 type. 응답에 같은 type을 쓴다(규격 1.3절, 4.1절). */
    private final Map<Integer, Integer> peerPendingStreams = new LinkedHashMap<>();
    /** 상대 HELLO에 ACK를 보내기 전이라 미뤄 둔 송신(규격 3.3절). */
    private final List<DeferredSend> deferredSends = new ArrayList<>();

    private byte[] receiveBuffer = new byte[0];
    private boolean prefaceChecked;
    private boolean firstFrameChecked;
    /** 상대의 파라미터 HELLO를 받아 내가 ACK를 보냈다(규격 3.3절). */
    private boolean peerHelloAcked;
    /** 내가 보낸 HELLO에 대해 상대가 보낸 ACK를 받았다(규격 3.3절 ACK 관문). */
    private boolean ownHelloAcked;
    /** 상대가 HELLO로 알린 {@code max_concurrent_streams}. 알리지 않았으면 기본값이다(규격 3.2.1절). */
    private long peerMaxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
    /** 상대가 HELLO로 알린 {@code max_frame_size}. 알리지 않았으면 기본값이다(규격 3.2.1절). */
    private long peerMaxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;
    /** CLOSE 교환: CLOSE를 보냈거나 받았다(규격 1.3절, 3.5절). */
    private boolean closeExchanged;

    /** 전송 계층이 닫혀 연결이 끝났다(규격 1.3절 연결이 끝남). */
    private boolean transportClosed;

    private long now;
    /** ACK를 기다리는 HELLO와 PING들. 보낸 순서대로다(규격 3.3절). */
    private final List<AckWait> ackWaits = new ArrayList<>();

    private long nextStreamId;
    /** 내 번호 공간의 홀짝: 클라이언트는 1, 서버는 0이다(규격 2.2.5절). */
    private final int ownParity;
    /** 지금까지 쓰인 상대 공간 번호 중 가장 큰 것(규격 2.2.5절). */
    private long highestPeerStreamId;

    private Endpoint(boolean server, EndpointConfig config) {
        this.config = config;
        this.nextStreamId = config.firstStreamId(server ? FIRST_SERVER_STREAM_ID : FIRST_CLIENT_STREAM_ID);
        this.ownParity = server ? 0 : 1;
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
        if (kind != WebSocketMessageKind.BINARY || message.length < TYPE_END) {
            connectionError();
            return;
        }
        long length = Frames.readUnsignedInt(message, 0);
        if (length < MIN_FRAME_LENGTH
                || length > config.maxFrameSize() - LENGTH_SIZE
                || message.length != LENGTH_SIZE + length) {
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
        for (AckWait wait : ackWaits) {
            if (now > wait.deadline) {
                // 닫으면 대기 목록이 비므로 반복을 이어 가지 않고 바로 끝낸다.
                connectionError();
                return;
            }
        }
    }

    /**
     * 애플리케이션이 요청을 보내게 한다. 상대 HELLO에 ACK를 보내기 전이면 그 뒤로 미룬다(규격 3.3절). CLOSE를 보냈거나 받은 뒤,
     * 번호 소진(규격 2.2.5절), 상대가 알린 {@code max_concurrent_streams} 도달(규격 3.2.1절)이면 프레임을 보내지 않고 로컬에서
     * 실패시킨다. 이때 스트림 번호 없는 {@link Event.Kind#RESULT} 이벤트(status {@code UNAVAILABLE})가 한 번 나온다. 한도에 닿은
     * 요청은 대기시키지 않고 즉시 실패시키며 스트림 번호를 쓰지 않는다.
     *
     * @param type 애플리케이션 {@code type}(256 이상)
     * @param payload 본문
     * @throws IllegalArgumentException 지금 보낼 수 있는 요청의 프레임이 상대가 알린 {@code max_frame_size}를 넘을 때. 규격은 그런
     *     프레임을 보내지 못하게 하고(MUST NOT, 3.2.1절) 로컬 처리를 정하지 않으므로, 아무것도 보내지 않고 스트림 번호도 쓰지 않는다.
     *     상대 HELLO에 ACK하기 전이라 미룬 요청은 호출자가 돌아간 뒤에 보내므로 던지지 않고, 보낼 때 넘으면 로컬 {@code UNAVAILABLE}로
     *     실패시킨다.
     */
    public void request(int type, byte[] payload) {
        if (transportClosed) {
            failLocally();
            return;
        }
        if (peerHelloAcked) {
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
    public void respond(int streamId, int status, byte[] payload) {
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
    public void cancel(int streamId) {
        if (transportClosed || ownPendingStreams.remove(streamId) == null) {
            return;
        }
        send(Frames.reset(streamId, ErrorCode.CANCEL));
        events.add(Event.result(streamId, Status.CANCELLED));
        closeTransportIfNoPendingStreams();
    }

    /**
     * 애플리케이션이 PING을 보내게 한다. 상대 HELLO에 ACK를 보내기 전이면 그 뒤로 미룬다(규격 3.3절).
     *
     * @param payload 정확히 8바이트
     */
    public void ping(byte[] payload) {
        if (!transportClosed) {
            sendAfterAckingPeerHello(DeferredSend.ping(() -> sendPing(payload)));
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
        send(Frames.close(errorCode));
        closeExchanged = true;
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
     * 지금까지 쌓인, 상대에게 보낼 메시지를 보낸 순서대로 꺼낸다. 프레임 하나가 메시지 하나다. 바이트 스트림 전송은 메시지를 이어 붙여 보내면
     * 된다.
     *
     * @return 보낼 메시지. 없으면 빈 목록
     */
    public List<OutboundMessage> drainMessages() {
        List<OutboundMessage> drained = new ArrayList<>(outbox);
        outbox.clear();
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
                && Frames.readUnsignedInt(message, 0) >= MIN_FRAME_LENGTH
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
        if (receiveBuffer.length < LENGTH_SIZE) {
            return false;
        }
        long length = Frames.readUnsignedInt(receiveBuffer, 0);
        if (!firstFrameChecked) {
            // 첫 프레임 검사(규격 3.2.4절): 헤더를 읽을 수 없거나 HELLO가 아니면 아무것도 보내지 않고 닫는다.
            if (length < MIN_FRAME_LENGTH) {
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
        if (length < MIN_FRAME_LENGTH || length > config.maxFrameSize() - LENGTH_SIZE) {
            if (receiveBuffer.length < TYPE_END) {
                // type을 읽을 때까지 기다린다. CLOSE이면 받은 CLOSE로 보고 CLOSE 없이 닫아야 하므로 도착 단위에 따라 결과가 달라지면 안 된다.
                return false;
            }
            rejectBadLength(receiveBuffer);
            return false;
        }
        int frameSize = (int) (LENGTH_SIZE + length);
        if (receiveBuffer.length < frameSize) {
            return false;
        }
        byte[] frame = Arrays.copyOfRange(receiveBuffer, 0, frameSize);
        receiveBuffer = Arrays.copyOfRange(receiveBuffer, frameSize, receiveBuffer.length);
        handle(frame);
        return true;
    }

    private void handle(byte[] frame) {
        int type = Frames.typeOf(frame);
        int extensionsLength = Frames.extensionsLengthOf(frame);
        long streamId = Frames.streamIdOf(frame);
        if (type == FrameType.CLOSE) {
            handleClose(frame, extensionsLength, streamId);
            return;
        }
        if (extensionsLength > frame.length - LENGTH_SIZE - MIN_FRAME_LENGTH) {
            connectionError();
            return;
        }
        if (type >= FrameType.FIRST_APPLICATION) {
            handleApplication(frame, type, extensionsLength, streamId);
            return;
        }
        if (type != FrameType.HELLO && type != FrameType.PING && type != FrameType.RESET) {
            // 예약 제어 타입은 확장 영역 검사만 받고 무시한다(규격 2.2.2절, 2.4.3절).
            if (extensionsLength != 0) {
                connectionError();
            }
            return;
        }
        if (extensionsLength != 0 || (type != FrameType.RESET && streamId != 0)) {
            connectionError();
            return;
        }
        if (!ownHelloAcked && (type == FrameType.PING || type == FrameType.RESET)) {
            connectionError();
            return;
        }
        byte[] payload = Arrays.copyOfRange(frame, Frames.HEADER_SIZE, frame.length);
        boolean ack = (Frames.flagsOf(frame) & Flags.ACK) != 0;
        if (type == FrameType.HELLO) {
            handleHello(ack, payload);
        } else if (type == FrameType.PING) {
            handlePing(ack, payload);
        } else {
            handleReset(streamId, payload);
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
        int id = (int) streamId;
        if (isOwnSpace(streamId)) {
            if (ownPendingStreams.remove(id) != null) {
                events.add(Event.result(id, Status.forReset(Frames.readUnsignedInt(payload, 0))));
            }
        } else {
            peerPendingStreams.remove(id);
        }
        closeTransportIfNoPendingStreams();
    }

    /** {@code stream_id}가 0이거나 최상위 비트가 1이다(규격 2.2.5절). */
    private static boolean isInvalidStreamId(long streamId) {
        return streamId == 0 || streamId > MAX_STREAM_ID;
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
        Extensions extensionsCheck = extensionsLength > MAX_EXTENSIONS_LENGTH
                ? Extensions.MALFORMED
                : scanExtensions(frame, extensionsLength);
        if (extensionsCheck == Extensions.MALFORMED || !ownHelloAcked || isInvalidStreamId(streamId)) {
            connectionError();
        } else if (isOwnSpace(streamId)) {
            handleResponse(frame, (int) streamId);
        } else {
            handleRequest(type, extensionsCheck, (int) streamId);
        }
    }

    /**
     * 상대의 새 요청(규격 5.1절 5번). (가) 거절, (나) 모르는 type, (다) 확장 값 오류 순서로 보고, 모두 지나면 애플리케이션에 넘긴다.
     * 거절한 요청과 오류로 답한 요청은 대기 스트림에 넣지 않지만 번호는 쓰인 것으로 센다(규격 4.5절, 5.1절).
     */
    private void handleRequest(int type, Extensions extensionsCheck, int streamId) {
        if (streamId <= highestPeerStreamId) {
            // 번호 역행: 재사용한 번호, 건너뛰고 나중에 온 번호, 대기 중인 스트림의 번호 모두 연결 오류다(규격 4.2.1절).
            connectionError();
            return;
        }
        highestPeerStreamId = streamId;
        if (closeExchanged || peerPendingStreams.size() + 1 > config.maxConcurrentStreams()) {
            send(Frames.reset(streamId, ErrorCode.REFUSED));
        } else if (!config.knows(type)) {
            send(Frames.response(type, streamId, Status.UNIMPLEMENTED));
        } else if (extensionsCheck == Extensions.INVALID_VALUE) {
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
    private void handleResponse(byte[] frame, int streamId) {
        Integer requestType = ownPendingStreams.remove(streamId);
        if (requestType == null) {
            // 대기 중이 아닌 스트림의 프레임은 조용히 버린다(규격 4.3절).
            return;
        }
        // 응답 type이 요청과 다르면 status와 payload를 버리고 로컬 INTERNAL을 넘긴다. 와이어로는 아무것도 보내지 않는다(규격 4.1절).
        int status = Frames.typeOf(frame) == requestType
                ? Status.normalize(Frames.statusOf(frame))
                : Status.RESPONSE_TYPE_MISMATCH;
        events.add(Event.result(streamId, status));
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
        if (extensionsLength != 0 || streamId != 0 || payloadSize < 4) {
            closeReceivedMalformed();
            return;
        }
        closeExchanged = true;
        closeTransportUnlessClosing(Frames.readUnsignedInt(frame, Frames.HEADER_SIZE));
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
        if (!closeExchanged) {
            send(Frames.close(ErrorCode.PROTOCOL_ERROR));
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
        for (int streamId : ownPendingStreams.keySet()) {
            events.add(Event.result(streamId, Status.UNAVAILABLE));
        }
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
        send(Frames.hello(config));
        ackWaits.add(new AckWait(null, now + config.ackTimeoutMs()));
    }

    private void sendPing(byte[] payload) {
        send(Frames.control(FrameType.PING, 0, payload));
        ackWaits.add(new AckWait(payload, now + config.ackTimeoutMs()));
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

    /** 상대 HELLO에 ACK를 보낸 뒤면 바로, 아니면 ACK를 보낼 때까지 미뤄서 보낸다. */
    private void sendAfterAckingPeerHello(DeferredSend sending) {
        if (peerHelloAcked) {
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
        int streamId = (int) nextStreamId;
        byte[] frame = Frames.request(type, streamId, payload);
        if (exceedsPeerFrameSize(frame)) {
            return RequestOutcome.OVERSIZE;
        }
        nextStreamId += 2;
        ownPendingStreams.put(streamId, type);
        send(frame);
        return RequestOutcome.SENT;
    }

    /** 프레임을 열지 못하는 요청이면 로컬에서 실패시키고 {@code true}를 돌려준다. 실패한 요청은 대기시키지 않고 번호도 쓰지 않는다. */
    private boolean failRequestIfRefused() {
        if (closeExchanged) {
            // CLOSE를 보냈거나 받았다(규격 3.5절).
            failLocally();
            return true;
        }
        if (nextStreamId > MAX_STREAM_ID) {
            // 번호가 소진됐다. 상대가 알린 한도보다 먼저 본다. 프레임 없이 로컬에서 실패시키고 연결을 닫는다(규격 2.2.5절).
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

    /** 상대 HELLO에 ACK를 보낼 때까지 미룬 송신 하나. 요청이면 보내지 못하고 끝날 때 로컬에서 실패시킨다. */
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

    /** 확인 대기 하나. {@code payload}는 PING이면 보낸 8바이트, HELLO이면 null이다. */
    private static final class AckWait {
        final byte @Nullable [] payload;
        final long deadline;

        AckWait(byte @Nullable [] payload, long deadline) {
            this.payload = payload;
            this.deadline = deadline;
        }
    }
}
