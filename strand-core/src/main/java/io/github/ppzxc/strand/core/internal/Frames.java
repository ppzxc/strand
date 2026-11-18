package io.github.ppzxc.strand.core.internal;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

/** 프레임 바이트 인코딩(규격 2.1절). */
final class Frames {

    static final int AUTH_PARAM_CREDENTIAL_LIFETIME = 0x0001;

    /** {@code length} 필드의 크기(규격 2.2.1절). */
    static final int LENGTH_FIELD_SIZE = 4;
    /** 헤더 크기. 확장 영역과 payload가 모두 비면 프레임은 이 크기다(규격 2.1절). */
    static final int HEADER_SIZE = 20;
    /** CLOSE payload의 고정 필드 크기(error_code와 last_stream_id). */
    static final int CLOSE_FIXED_PAYLOAD_SIZE = Integer.BYTES + Long.BYTES;
    /** {@code length}의 최솟값: 헤더에서 {@code length} 필드를 뺀 크기(규격 2.2.1절). */
    static final int MIN_LENGTH = HEADER_SIZE - LENGTH_FIELD_SIZE;
    /** {@code type}이 끝나는 위치. 길이가 어긋난 프레임도 여기까지는 읽어 type을 본다. */
    static final int TYPE_END = 6;

    private static final byte[] NO_EXTENSIONS = new byte[0];

    private Frames() {}

    /**
     * 쓸 수 있는 {@code stream_id}인가: {@code 1}~{@code 2^63-1}(규격 2.2.5절). 최상위 비트가 1이면 {@code long}으로 음수라서 걸러지고,
     * 다음 번호를 2씩 더하다 {@code 2^63-1}을 넘어 넘친 값도 음수라서 걸러진다(번호 소진).
     */
    static boolean isValidStreamId(long streamId) {
        return streamId > 0;
    }

    /** 확장 영역이 없고 status·stream_id가 0인 제어 프레임. */
    static byte[] control(int type, int flags, byte[] payload) {
        return frame(type, flags, 0, 0, payload);
    }

    /** 확장 영역이 없는 애플리케이션 요청 프레임. */
    static byte[] request(int type, long streamId, byte[] payload) {
        return request(type, streamId, NO_EXTENSIONS, payload);
    }

    /**
     * 확장 영역이 있는 애플리케이션 요청 프레임(규격 2.4절). 확장 영역은 이미 항목(tag, length, value)으로 인코딩된 바이트이고 본문 앞에 놓인다.
     */
    static byte[] request(int type, long streamId, byte[] extensions, byte[] payload) {
        return frame(type, 0, 0, streamId, extensions, payload);
    }

    /** 확장 영역과 payload가 없는 응답 프레임. 오류 응답에 쓴다(규격 4.1절). */
    static byte[] response(int type, long streamId, int status) {
        return response(type, streamId, status, new byte[0]);
    }

    /** 확장 영역이 없는 응답 프레임(규격 4.1절). */
    static byte[] response(int type, long streamId, int status, byte[] payload) {
        return frame(type, 0, status, streamId, payload);
    }

    /** 스트림 하나를 중단하는 RESET(규격 4.4절). */
    static byte[] reset(long streamId, int errorCode) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeInt(payload, errorCode);
        return frame(FrameType.RESET, 0, 0, streamId, payload.toByteArray());
    }

    private static byte[] frame(int type, int flags, int status, long streamId, byte[] payload) {
        return frame(type, flags, status, streamId, NO_EXTENSIONS, payload);
    }

    private static byte[] frame(int type, int flags, int status, long streamId, byte[] extensions, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(HEADER_SIZE + extensions.length + payload.length);
        writeInt(out, MIN_LENGTH + extensions.length + payload.length);
        out.write(type >>> 8);
        out.write(type);
        out.write(flags);
        out.write(status);
        writeLong(out, streamId);
        out.write(extensions.length >>> 8); // extensions_length
        out.write(extensions.length);
        out.write(0); // reserved
        out.write(0);
        out.write(extensions, 0, extensions.length);
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    static byte[] hello(EndpointConfig config) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeParam(payload, HelloParams.MAX_FRAME_SIZE_ID, config.maxFrameSize());
        writeParam(payload, HelloParams.MAX_CONCURRENT_STREAMS_ID, config.maxConcurrentStreams());
        writeParam(payload, HelloParams.CAPABILITIES_ID, config.capabilities());
        return control(FrameType.HELLO, 0, payload.toByteArray());
    }

    /** 클라이언트가 보내는 {@code AUTH}: {@code mechanism_length} u8 + {@code mechanism} + {@code credentials}(규격 3.3.1절). */
    static byte[] auth(String mechanism, byte[] credentials) {
        byte[] name = mechanism.getBytes(StandardCharsets.US_ASCII);
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(name.length);
        payload.write(name, 0, name.length);
        payload.write(credentials, 0, credentials.length);
        return control(FrameType.AUTH, 0, payload.toByteArray());
    }

    /** 서버가 알리는 자격 증명 수명(규격 3.3.1절). */
    static byte[] authAck(OptionalLong credentialLifetimeSeconds) {
        if (!credentialLifetimeSeconds.isPresent()) {
            return control(FrameType.AUTH, Flags.ACK, new byte[0]);
        }
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeParam(payload, AUTH_PARAM_CREDENTIAL_LIFETIME, credentialLifetimeSeconds.getAsLong());
        return control(FrameType.AUTH, Flags.ACK, payload.toByteArray());
    }

    /** 연결 서문 8바이트(규격 3.1절). */
    static byte[] preface() {
        return new byte[] {(byte) 0xC0, (byte) 0xDE, 0x53, 0x54, 0x52, 0x30, 0x00, 0x00};
    }

    /** CLOSE. {@code last_stream_id}는 보내는 순간의 {@code highest_peer_stream_id}다(규격 3.5절). */
    static byte[] close(int errorCode, long lastStreamId) {
        return close(errorCode, lastStreamId, new byte[0]);
    }

    static byte[] close(int errorCode, long lastStreamId, byte[] reason) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeInt(payload, errorCode);
        writeLong(payload, lastStreamId);
        payload.write(reason, 0, reason.length);
        return control(FrameType.CLOSE, 0, payload.toByteArray());
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

    private static void writeLong(ByteArrayOutputStream out, long value) {
        writeInt(out, value >>> 32);
        writeInt(out, value);
    }

    static long readUnsignedInt(byte[] bytes, int at) {
        return ((long) (bytes[at] & 0xFF) << 24)
                | ((bytes[at + 1] & 0xFF) << 16)
                | ((bytes[at + 2] & 0xFF) << 8)
                | (bytes[at + 3] & 0xFF);
    }

    /** 8바이트 빅 엔디언 정수. 최상위 비트가 1이면 음수로 읽힌다(규격 2.2.5절은 그런 {@code stream_id}를 오류로 본다). */
    static long readLong(byte[] bytes, int at) {
        return (readUnsignedInt(bytes, at) << 32) | readUnsignedInt(bytes, at + 4);
    }

    static int readUnsignedShort(byte[] bytes, int at) {
        return ((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF);
    }

    /** {@code off}에서 시작하는 프레임의 {@code type}(바이트 4~5, 규격 2.1절). */
    static int typeOf(byte[] bytes, int off) {
        return readUnsignedShort(bytes, off + 4);
    }

    /** {@code off}에서 시작하는 프레임의 {@code flags}(바이트 6). */
    static int flagsOf(byte[] bytes, int off) {
        return bytes[off + 6] & 0xFF;
    }

    /** {@code off}에서 시작하는 응답 프레임의 {@code status}(바이트 7). */
    static int statusOf(byte[] bytes, int off) {
        return bytes[off + 7] & 0xFF;
    }

    /** {@code off}에서 시작하는 프레임의 {@code stream_id}(바이트 8~15). 최상위 비트가 1이면 음수다. */
    static long streamIdOf(byte[] bytes, int off) {
        return readLong(bytes, off + 8);
    }

    /** {@code off}에서 시작하는 프레임의 {@code extensions_length}(바이트 16~17). */
    static int extensionsLengthOf(byte[] bytes, int off) {
        return readUnsignedShort(bytes, off + 16);
    }
}
