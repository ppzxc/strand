package io.github.ppzxc.strand.core.internal;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 엔드포인트(서버 또는 클라이언트 역할). 입출력을 하지 않는 순수 상태 기계다. 받은 바이트는 {@link #receive}, 애플리케이션의 호출은
 * {@link #request}·{@link #ping}·{@link #close}, 시간은 {@link #advance}로 넣는다. 그러면 보낼 바이트와 애플리케이션 이벤트가
 * 쌓이고, {@link #drainOutput}과 {@link #drainEvents}로 한 번씩 꺼낸다. 스레드·소켓·실제 시계에 접근하지 않는다.
 */
public final class Endpoint {

    private static final int PREFACE_MAGIC_LENGTH = 6;
    /** 서버는 짝수, 클라이언트는 홀수 번호를 쓰고 첫 번호는 각각 2와 1이다(규격 2.2.5절). */
    private static final int FIRST_SERVER_STREAM_ID = 2;

    private static final int FIRST_CLIENT_STREAM_ID = 1;
    private static final int MIN_FRAME_LENGTH = 12;
    private static final int LENGTH_SIZE = 4;
    private static final int PING_PAYLOAD_SIZE = 8;
    private static final int PARAM_SIZE = 6;
    private static final int MAX_FRAME_SIZE_ID = 0x0001;
    private static final long MIN_MAX_FRAME_SIZE = 16384L;
    private static final long MAX_MAX_FRAME_SIZE = 16_777_216L;

    private final EndpointConfig config;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final List<Event> events = new ArrayList<>();
    private final List<Integer> pendingStreams = new ArrayList<>();
    /** 상대 HELLO에 ACK를 보내기 전이라 미뤄 둔 송신(규격 3.3절). */
    private final List<Runnable> deferredSends = new ArrayList<>();

    private byte[] inbound = new byte[0];
    private boolean prefaceChecked;
    private boolean firstFrameChecked;
    /** 상대의 파라미터 HELLO를 받아 ACK를 보냈다. */
    private boolean helloAcknowledged;
    /** 내가 보낸 HELLO에 대한 상대의 ACK를 받았다. */
    private boolean peerAckReceived;
    /** CLOSE를 보냈거나 받았다(규격 3.5절). */
    private boolean closeSeen;

    private boolean closed;
    private long now;
    private boolean waitingForAck;
    private long ackDeadline;
    private int nextStreamId;

    private Endpoint(boolean server, EndpointConfig config) {
        this.config = config;
        this.nextStreamId = server ? FIRST_SERVER_STREAM_ID : FIRST_CLIENT_STREAM_ID;
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
        if (closed) {
            return;
        }
        inbound = Frames.concat(inbound, bytes);
        if (!prefaceChecked && !checkPreface()) {
            return;
        }
        while (!closed && readFrame()) {
            // 한 프레임씩 처리한다.
        }
    }

    /**
     * 가상 시계를 앞으로 보낸다. 확인 대기 시간이 지나도록 ACK가 없으면 연결 오류로 닫는다.
     *
     * @param ms 밀리초
     */
    public void advance(long ms) {
        if (closed) {
            return;
        }
        now += ms;
        if (waitingForAck && now > ackDeadline) {
            connectionError();
        }
    }

    /**
     * 애플리케이션이 요청을 보내게 한다. 상대 HELLO에 ACK를 보내기 전이면 그 뒤로 미룬다(규격 3.3절). CLOSE를 보냈거나 받은 뒤라면
     * 프레임을 보내지 않고 로컬에서 실패시키며, 스트림이 없으므로 이벤트도 없다(규격 3.5절).
     *
     * @param type 애플리케이션 {@code type}(256 이상)
     * @param payload 본문
     */
    public void request(int type, byte[] payload) {
        if (closed || closeSeen) {
            return;
        }
        sendAfterAck(() -> sendRequest(type, payload));
    }

    /**
     * 애플리케이션이 PING을 보내게 한다. 상대 HELLO에 ACK를 보내기 전이면 그 뒤로 미룬다(규격 3.3절).
     *
     * @param payload 정확히 8바이트
     */
    public void ping(byte[] payload) {
        if (!closed) {
            sendAfterAck(() -> send(Frames.control(Frames.PING, 0, payload)));
        }
    }

    /**
     * 애플리케이션이 연결을 닫게 한다. 이미 CLOSE를 보냈거나 받았다면 아무것도 하지 않는다(규격 3.5절).
     *
     * @param errorCode 규격 2.5.2절의 error_code 값
     */
    public void close(int errorCode) {
        if (closed || closeSeen) {
            return;
        }
        send(Frames.close(errorCode));
        closeSeen = true;
        terminateUnlessClosing(errorCode);
    }

    /**
     * 지금까지 쌓인, 상대에게 보낼 바이트를 꺼낸다.
     *
     * @return 보낼 바이트. 없으면 빈 배열
     */
    public byte[] drainOutput() {
        byte[] bytes = output.toByteArray();
        output.reset();
        return bytes;
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
        int compared = Math.min(inbound.length, PREFACE_MAGIC_LENGTH);
        for (int i = 0; i < compared; i++) {
            if (inbound[i] != preface[i]) {
                terminate();
                return false;
            }
        }
        if (inbound.length < preface.length) {
            return false;
        }
        inbound = Arrays.copyOfRange(inbound, preface.length, inbound.length);
        prefaceChecked = true;
        sendHello();
        return true;
    }

    /** 프레임 하나를 읽어 처리한다. 더 읽을 수 있으면 true. */
    private boolean readFrame() {
        if (inbound.length < LENGTH_SIZE) {
            return false;
        }
        long length = Frames.readUnsignedInt(inbound, 0);
        if (!firstFrameChecked) {
            // 첫 프레임 검사(규격 3.2.4절): 헤더를 읽을 수 없거나 HELLO가 아니면 아무것도 보내지 않고 닫는다.
            if (length < MIN_FRAME_LENGTH) {
                terminate();
                return false;
            }
            if (inbound.length < Frames.HEADER_SIZE) {
                return false;
            }
            if (Frames.typeOf(inbound) != Frames.HELLO
                    || Frames.streamIdOf(inbound) != 0
                    || Frames.flagsOf(inbound) != 0) {
                terminate();
                return false;
            }
            firstFrameChecked = true;
        }
        if (length < MIN_FRAME_LENGTH || length > config.maxFrameSize() - LENGTH_SIZE) {
            connectionError();
            return false;
        }
        int frameSize = (int) (LENGTH_SIZE + length);
        if (inbound.length < frameSize) {
            return false;
        }
        byte[] frame = Arrays.copyOfRange(inbound, 0, frameSize);
        inbound = Arrays.copyOfRange(inbound, frameSize, inbound.length);
        handle(frame);
        return true;
    }

    private void handle(byte[] frame) {
        int type = Frames.typeOf(frame);
        int extensions = Frames.extensionsLengthOf(frame);
        long streamId = Frames.streamIdOf(frame);
        if (type == Frames.CLOSE) {
            handleClose(frame, extensions, streamId);
            return;
        }
        if (extensions > frame.length - LENGTH_SIZE - MIN_FRAME_LENGTH) {
            connectionError();
            return;
        }
        if (type >= Frames.FIRST_APPLICATION_TYPE) {
            // 상대 ACK 전의 애플리케이션 프레임은 연결 오류다(규격 3.3절). 그 뒤의 처리는 이후 단계에서 다룬다.
            if (!peerAckReceived) {
                connectionError();
            }
            return;
        }
        if (type != Frames.HELLO && type != Frames.PING && type != Frames.RESET) {
            // 예약 제어 타입은 확장 영역 검사만 받고 무시한다(규격 2.2.2절, 2.4.3절).
            if (extensions != 0) {
                connectionError();
            }
            return;
        }
        if (extensions != 0 || (type != Frames.RESET && streamId != 0)) {
            connectionError();
            return;
        }
        if (!peerAckReceived && (type == Frames.PING || type == Frames.RESET)) {
            connectionError();
            return;
        }
        byte[] payload = Arrays.copyOfRange(frame, Frames.HEADER_SIZE, frame.length);
        boolean ack = (Frames.flagsOf(frame) & Frames.ACK) != 0;
        if (type == Frames.HELLO) {
            handleHello(ack, payload);
        } else if (type == Frames.PING) {
            handlePing(ack, payload);
        }
    }

    private void handleHello(boolean ack, byte[] payload) {
        if (ack) {
            // 합친 프레임(ACK이면서 payload가 있음)과 두 번째 ACK는 연결 오류다(규격 3.2.5절).
            if (payload.length != 0 || peerAckReceived) {
                connectionError();
                return;
            }
            peerAckReceived = true;
            waitingForAck = false;
            return;
        }
        if (helloAcknowledged || !validParameters(payload)) {
            connectionError();
            return;
        }
        helloAcknowledged = true;
        send(Frames.control(Frames.HELLO, Frames.ACK, new byte[0]));
        for (Runnable deferred : deferredSends) {
            if (closed || closeSeen) {
                break;
            }
            deferred.run();
        }
        deferredSends.clear();
    }

    /** HELLO payload 검증(규격 3.2.3절, 3.2.5절): 6의 배수, identifier 중복 없음, max_frame_size 범위. */
    private static boolean validParameters(byte[] payload) {
        if (payload.length % PARAM_SIZE != 0) {
            return false;
        }
        Set<Integer> seen = new HashSet<>();
        for (int at = 0; at < payload.length; at += PARAM_SIZE) {
            int identifier = Frames.readUnsignedShort(payload, at);
            if (!seen.add(identifier)) {
                return false;
            }
            long value = Frames.readUnsignedInt(payload, at + 2);
            if (identifier == MAX_FRAME_SIZE_ID && (value < MIN_MAX_FRAME_SIZE || value > MAX_MAX_FRAME_SIZE)) {
                return false;
            }
        }
        return true;
    }

    private void handlePing(boolean ack, byte[] payload) {
        if (payload.length != PING_PAYLOAD_SIZE) {
            connectionError();
        } else if (!ack) {
            send(Frames.control(Frames.PING, Frames.ACK, payload));
        }
    }

    /** 받은 CLOSE(규격 3.5절). 형태가 잘못돼도 받은 CLOSE로 치고, 그때는 오류 CLOSE처럼 CLOSE 없이 닫는다. */
    private void handleClose(byte[] frame, int extensions, long streamId) {
        closeSeen = true;
        int payloadSize = frame.length - Frames.HEADER_SIZE - extensions;
        if (extensions != 0 || streamId != 0 || payloadSize < 4) {
            terminate();
            return;
        }
        terminateUnlessClosing(Frames.readUnsignedInt(frame, Frames.HEADER_SIZE));
    }

    /** 오류 CLOSE이거나 대기 중인 스트림이 없으면 닫고, 아니면 대기 중인 스트림이 끝날 때까지 종료 중으로 둔다(규격 3.5절). */
    private void terminateUnlessClosing(long errorCode) {
        if (errorCode != Frames.NO_ERROR || pendingStreams.isEmpty()) {
            terminate();
        }
    }

    /** 연결 오류: CLOSE(PROTOCOL_ERROR)를 보내고 닫는다. 이미 CLOSE를 보냈거나 받았다면 보내지 않는다(규격 3.5절, 5.1절). */
    private void connectionError() {
        if (!closeSeen) {
            send(Frames.close(Frames.PROTOCOL_ERROR));
        }
        closeSeen = true;
        terminate();
    }

    /**
     * 연결이 끝난다. 결과를 받지 못한 요청마다 로컬 UNAVAILABLE을 한 번씩 넘긴 뒤 closed를 낸다(규격 3.5절). 이후 받은 바이트와
     * 시간은 처리하지 않는다.
     */
    private void terminate() {
        if (closed) {
            return;
        }
        closed = true;
        inbound = new byte[0];
        waitingForAck = false;
        deferredSends.clear();
        for (int streamId : pendingStreams) {
            events.add(Event.result(streamId, Event.UNAVAILABLE));
        }
        pendingStreams.clear();
        events.add(Event.closed());
    }

    private void sendHello() {
        send(Frames.hello(config));
        waitingForAck = true;
        ackDeadline = now + config.ackTimeoutMs();
    }

    /** 상대 HELLO에 ACK를 보낸 뒤면 바로, 아니면 ACK를 보낼 때까지 미뤄서 보낸다. */
    private void sendAfterAck(Runnable sending) {
        if (helloAcknowledged) {
            sending.run();
        } else {
            deferredSends.add(sending);
        }
    }

    private void sendRequest(int type, byte[] payload) {
        int streamId = nextStreamId;
        nextStreamId += 2;
        pendingStreams.add(streamId);
        send(Frames.request(type, streamId, payload));
    }

    private void send(byte[] frame) {
        output.write(frame, 0, frame.length);
    }
}
