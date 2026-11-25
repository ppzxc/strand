package io.github.ppzxc.strand.core.internal;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

final class Frames {

    static final int AUTH_PARAM_CREDENTIAL_LIFETIME = 0x0001;

    static final int LENGTH_FIELD_SIZE = 4;

    static final int HEADER_SIZE = 20;

    static final int CLOSE_FIXED_PAYLOAD_SIZE = Integer.BYTES + Long.BYTES;

    static final int MIN_LENGTH = HEADER_SIZE - LENGTH_FIELD_SIZE;

    static final int TYPE_END = 6;

    private static final byte[] NO_EXTENSIONS = new byte[0];

    private Frames() {}

    static boolean isValidStreamId(long streamId) {
        return streamId > 0;
    }

    static byte[] control(int type, int flags, byte[] payload) {
        return frame(type, flags, 0, 0, payload);
    }

    static byte[] request(int type, long streamId, byte[] payload) {
        return request(type, streamId, NO_EXTENSIONS, payload);
    }

    static byte[] request(int type, long streamId, byte[] extensions, byte[] payload) {
        return frame(type, 0, 0, streamId, extensions, payload);
    }

    static byte[] response(int type, long streamId, int status) {
        return response(type, streamId, status, new byte[0]);
    }

    static byte[] response(int type, long streamId, int status, byte[] payload) {
        return frame(type, 0, status, streamId, payload);
    }

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
        out.write(extensions.length >>> 8);
        out.write(extensions.length);
        out.write(0);
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

    static byte[] auth(String mechanism, byte[] credentials) {
        byte[] name = mechanism.getBytes(StandardCharsets.US_ASCII);
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(name.length);
        payload.write(name, 0, name.length);
        payload.write(credentials, 0, credentials.length);
        return control(FrameType.AUTH, 0, payload.toByteArray());
    }

    static byte[] authAck(OptionalLong credentialLifetimeSeconds) {
        if (!credentialLifetimeSeconds.isPresent()) {
            return control(FrameType.AUTH, Flags.ACK, new byte[0]);
        }
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeParam(payload, AUTH_PARAM_CREDENTIAL_LIFETIME, credentialLifetimeSeconds.getAsLong());
        return control(FrameType.AUTH, Flags.ACK, payload.toByteArray());
    }

    static byte[] preface() {
        return new byte[] {(byte) 0xC0, (byte) 0xDE, 0x53, 0x54, 0x52, 0x30, 0x00, 0x00};
    }

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

    static long readLong(byte[] bytes, int at) {
        return (readUnsignedInt(bytes, at) << 32) | readUnsignedInt(bytes, at + 4);
    }

    static int readUnsignedShort(byte[] bytes, int at) {
        return ((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF);
    }

    static int typeOf(byte[] bytes, int off) {
        return readUnsignedShort(bytes, off + 4);
    }

    static int flagsOf(byte[] bytes, int off) {
        return bytes[off + 6] & 0xFF;
    }

    static int statusOf(byte[] bytes, int off) {
        return bytes[off + 7] & 0xFF;
    }

    static long streamIdOf(byte[] bytes, int off) {
        return readLong(bytes, off + 8);
    }

    static int extensionsLengthOf(byte[] bytes, int off) {
        return readUnsignedShort(bytes, off + 16);
    }
}
