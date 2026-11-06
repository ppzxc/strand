package io.github.ppzxc.strand.core.internal;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/** 프레임 바이트 인코딩(규격 2.1절). */
final class Frames {

    static final int HEADER_SIZE = 16;
    static final int HELLO = 0x0001;
    static final int PING = 0x0002;
    static final int CLOSE = 0x0003;
    static final int ACK = 0x01;
    static final int NO_ERROR = 0;
    static final int PROTOCOL_ERROR = 1;
    static final int RESET = 0x0004;
    /** 이 값부터 애플리케이션 프레임이다(규격 2.2.2절). */
    static final int FIRST_APPLICATION_TYPE = 0x0100;

    private Frames() {}

    /** 확장 영역이 없고 status·stream_id가 0인 제어 프레임. */
    static byte[] control(int type, int flags, byte[] payload) {
        return frame(type, flags, 0, payload);
    }

    /** 확장 영역이 없는 애플리케이션 요청 프레임. */
    static byte[] request(int type, int streamId, byte[] payload) {
        return frame(type, 0, streamId, payload);
    }

    private static byte[] frame(int type, int flags, int streamId, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(HEADER_SIZE + payload.length);
        writeInt(out, 12 + payload.length);
        out.write(type >>> 8);
        out.write(type);
        out.write(flags);
        out.write(0); // status
        writeInt(out, streamId);
        out.write(0); // extensions_length
        out.write(0);
        out.write(0); // reserved
        out.write(0);
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    static byte[] hello(EndpointConfig config) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeParam(payload, 0x0001, config.maxFrameSize());
        writeParam(payload, 0x0002, config.maxConcurrentStreams());
        writeParam(payload, 0x0003, config.capabilities());
        return control(HELLO, 0, payload.toByteArray());
    }

    /** 연결 서문 8바이트(규격 3.1절). */
    static byte[] preface() {
        return new byte[] {(byte) 0xC0, (byte) 0xDE, 0x53, 0x54, 0x52, 0x30, 0x00, 0x00};
    }

    static byte[] close(int errorCode) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeInt(payload, errorCode);
        return control(CLOSE, 0, payload.toByteArray());
    }

    private static void writeParam(ByteArrayOutputStream out, int identifier, long value) {
        out.write(identifier >>> 8);
        out.write(identifier);
        writeInt(out, value);
    }

    private static void writeInt(ByteArrayOutputStream out, long value) {
        out.write((int) (value >>> 24));
        out.write((int) (value >>> 16));
        out.write((int) (value >>> 8));
        out.write((int) value);
    }

    static long readUnsignedInt(byte[] bytes, int at) {
        return ((long) (bytes[at] & 0xFF) << 24)
                | ((bytes[at + 1] & 0xFF) << 16)
                | ((bytes[at + 2] & 0xFF) << 8)
                | (bytes[at + 3] & 0xFF);
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    static int readUnsignedShort(byte[] bytes, int at) {
        return ((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF);
    }

    /** 프레임의 {@code type}(바이트 4~5, 규격 2.1절). */
    static int typeOf(byte[] frame) {
        return readUnsignedShort(frame, 4);
    }

    /** 프레임의 {@code flags}(바이트 6). */
    static int flagsOf(byte[] frame) {
        return frame[6] & 0xFF;
    }

    /** 프레임의 {@code stream_id}(바이트 8~11). */
    static long streamIdOf(byte[] frame) {
        return readUnsignedInt(frame, 8);
    }

    /** 프레임의 {@code extensions_length}(바이트 12~13). */
    static int extensionsLengthOf(byte[] frame) {
        return readUnsignedShort(frame, 12);
    }
}
