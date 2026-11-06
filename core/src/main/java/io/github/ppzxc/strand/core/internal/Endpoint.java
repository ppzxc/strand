package io.github.ppzxc.strand.core.internal;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 서버 역할 엔드포인트. 입출력을 하지 않는 순수 상태 기계다. 받은 바이트를 {@link #receive}로 넣으면 보낼 바이트와 애플리케이션
 * 이벤트가 쌓이고, {@link #drainOutput}과 {@link #drainEvents}로 한 번씩 꺼낸다. 스레드·소켓·시계에 접근하지 않는다.
 */
public final class Endpoint {

    private static final byte[] PREFACE = {(byte) 0xC0, (byte) 0xDE, 0x53, 0x54, 0x52, 0x30, 0x00, 0x00};
    private static final int PREFACE_MAGIC_LENGTH = 6;
    private static final int MIN_FRAME_LENGTH = 12;
    private static final int PING_PAYLOAD_SIZE = 8;

    private final EndpointConfig config;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final List<Event> events = new ArrayList<>();
    private byte[] inbound = new byte[0];
    private boolean prefaceChecked;
    private boolean closed;

    private Endpoint(EndpointConfig config) {
        this.config = config;
    }

    /**
     * 서버 역할 엔드포인트를 만든다.
     *
     * @param config 이 엔드포인트가 보내는 HELLO의 파라미터
     * @return 연결 서문을 기다리는 엔드포인트
     */
    public static Endpoint server(EndpointConfig config) {
        return new Endpoint(config);
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
        while (!closed && inbound.length >= 4) {
            long length = Frames.readUnsignedInt(inbound, 0);
            if (length < MIN_FRAME_LENGTH || length > config.maxFrameSize() - 4) {
                connectionError();
                return;
            }
            int frameSize = (int) (4 + length);
            if (inbound.length < frameSize) {
                return;
            }
            handle(Arrays.copyOfRange(inbound, 0, frameSize));
            inbound = Arrays.copyOfRange(inbound, frameSize, inbound.length);
        }
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

    /** 서문의 앞 6바이트를 비교한다. 판정할 만큼 모였으면 true. 서문은 8바이트를 모두 받은 뒤 건너뛴다. */
    private boolean checkPreface() {
        int compared = Math.min(inbound.length, PREFACE_MAGIC_LENGTH);
        for (int i = 0; i < compared; i++) {
            if (inbound[i] != PREFACE[i]) {
                close();
                return false;
            }
        }
        if (inbound.length < PREFACE.length) {
            return false;
        }
        inbound = Arrays.copyOfRange(inbound, PREFACE.length, inbound.length);
        prefaceChecked = true;
        send(Frames.hello(config));
        return true;
    }

    private void handle(byte[] frame) {
        int type = ((frame[4] & 0xFF) << 8) | (frame[5] & 0xFF);
        boolean ack = (frame[6] & Frames.ACK) != 0;
        byte[] payload = Arrays.copyOfRange(frame, Frames.HEADER_SIZE, frame.length);
        if (type == Frames.HELLO && !ack) {
            send(Frames.control(Frames.HELLO, Frames.ACK, new byte[0]));
        } else if (type == Frames.PING && !ack) {
            if (payload.length != PING_PAYLOAD_SIZE) {
                connectionError();
                return;
            }
            send(Frames.control(Frames.PING, Frames.ACK, payload));
        }
    }

    /** 연결 오류: CLOSE(PROTOCOL_ERROR)를 보내고 닫는다(규격 5.1절). */
    private void connectionError() {
        send(Frames.close(Frames.PROTOCOL_ERROR));
        close();
    }

    /** 닫는다. 이후 받은 바이트는 처리하지 않고 closed 이벤트는 한 번만 낸다. */
    private void close() {
        closed = true;
        inbound = new byte[0];
        events.add(Event.closed());
    }

    private void send(byte[] frame) {
        output.write(frame, 0, frame.length);
    }
}
