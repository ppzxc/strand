package io.github.ppzxc.strand.core.internal;

import java.io.ByteArrayOutputStream;

/** 프레임 바이트 인코딩(규격 2.1절). */
final class Frames {

    static final int HEADER_SIZE = 16;
    static final int HELLO = 0x0001;
    static final int PING = 0x0002;
    static final int CLOSE = 0x0003;
    static final int ACK = 0x01;
    static final int PROTOCOL_ERROR = 1;

    private Frames() {}

    /** 확장 영역이 없고 status·stream_id가 0인 제어 프레임. */
    static byte[] control(int type, int flags, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(HEADER_SIZE + payload.length);
        writeInt(out, 12 + payload.length);
        out.write(type >>> 8);
        out.write(type);
        out.write(flags);
        out.write(0); // status
        writeInt(out, 0); // stream_id
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
}
