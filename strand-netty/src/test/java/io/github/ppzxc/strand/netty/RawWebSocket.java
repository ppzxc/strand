package io.github.ppzxc.strand.netty;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Locale;

final class RawWebSocket {
    static final int CONTINUATION = 0x0;
    static final int TEXT = 0x1;
    static final int BINARY = 0x2;
    static final int CLOSE = 0x8;
    static final int PING = 0x9;
    static final int PONG = 0xA;

    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private static final SecureRandom RANDOM = new SecureRandom();

    private RawWebSocket() {}

    static final class Frame {
        final int opcode;
        final boolean fin;
        final byte[] payload;

        Frame(int opcode, boolean fin, byte[] payload) {
            this.opcode = opcode;
            this.fin = fin;
            this.payload = payload;
        }

        int closeStatus() {
            return payload.length >= 2 ? ((payload[0] & 0xff) << 8) | (payload[1] & 0xff) : -1;
        }

        String closeReason() {
            return payload.length > 2 ? new String(payload, 2, payload.length - 2, StandardCharsets.UTF_8) : "";
        }
    }

    static String upgradeRequest(String path, String host, String key, String subprotocol) {
        return "GET " + path + " HTTP/1.1\r\n"
                + "Host: " + host + "\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Key: " + key + "\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + (subprotocol == null ? "" : "Sec-WebSocket-Protocol: " + subprotocol + "\r\n")
                + "\r\n";
    }

    static String newKey() {
        byte[] nonce = new byte[16];
        RANDOM.nextBytes(nonce);
        return Base64.getEncoder().encodeToString(nonce);
    }

    static String acceptFor(String key) {
        try {
            return Base64.getEncoder()
                    .encodeToString(MessageDigest.getInstance("SHA-1")
                            .digest((key + GUID).getBytes(StandardCharsets.US_ASCII)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static String readHeaders(InputStream in) throws IOException {
        StringBuilder text = new StringBuilder();
        while (!text.toString().endsWith("\r\n\r\n")) {
            int next = in.read();
            if (next < 0) {
                throw new EOFException("헤더를 마치기 전에 끊겼다: " + text);
            }
            text.append((char) next);
        }
        return text.toString();
    }

    static String header(String headers, String name) {
        for (String line : headers.split("\r\n", -1)) {
            int colon = line.indexOf(':');
            if (colon > 0
                    && line.substring(0, colon).trim().toLowerCase(Locale.ROOT).equals(name.toLowerCase(Locale.ROOT))) {
                return line.substring(colon + 1).trim();
            }
        }
        return null;
    }

    static byte[] encode(int opcode, boolean fin, byte[] payload, boolean mask) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((fin ? 0x80 : 0) | opcode);
        int maskBit = mask ? 0x80 : 0;
        if (payload.length < 126) {
            out.write(maskBit | payload.length);
        } else if (payload.length <= 0xffff) {
            out.write(maskBit | 126);
            out.write(payload.length >> 8);
            out.write(payload.length);
        } else {
            out.write(maskBit | 127);
            for (int shift = 56; shift >= 0; shift -= 8) {
                out.write((int) (((long) payload.length >> shift) & 0xff));
            }
        }
        if (mask) {
            byte[] key = new byte[4];
            RANDOM.nextBytes(key);
            out.write(key, 0, 4);
            for (int i = 0; i < payload.length; i++) {
                out.write(payload[i] ^ key[i % 4]);
            }
        } else {
            out.write(payload, 0, payload.length);
        }
        return out.toByteArray();
    }

    static byte[] headerClaiming(int opcode, boolean fin, long claimedLength, boolean mask) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((fin ? 0x80 : 0) | opcode);
        int maskBit = mask ? 0x80 : 0;
        out.write(maskBit | 127);
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) ((claimedLength >> shift) & 0xff));
        }
        if (mask) {
            out.write(new byte[4], 0, 4);
        }
        return out.toByteArray();
    }

    static byte[] closePayload(int status) {
        return new byte[] {(byte) (status >> 8), (byte) status};
    }

    static void write(OutputStream out, byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    static Frame read(InputStream in) throws IOException {
        int first = in.read();
        if (first < 0) {
            throw new EOFException();
        }
        int second = in.read();
        if (second < 0) {
            throw new EOFException();
        }
        boolean masked = (second & 0x80) != 0;
        long length = second & 0x7f;
        if (length == 126) {
            length = ((long) readByte(in) << 8) | readByte(in);
        } else if (length == 127) {
            length = 0;
            for (int i = 0; i < 8; i++) {
                length = (length << 8) | readByte(in);
            }
        }
        byte[] key = new byte[4];
        if (masked) {
            readFully(in, key);
        }
        byte[] payload = new byte[(int) length];
        readFully(in, payload);
        if (masked) {
            for (int i = 0; i < payload.length; i++) {
                payload[i] ^= key[i % 4];
            }
        }
        return new Frame(first & 0x0f, (first & 0x80) != 0, payload);
    }

    private static int readByte(InputStream in) throws IOException {
        int value = in.read();
        if (value < 0) {
            throw new EOFException();
        }
        return value;
    }

    private static void readFully(InputStream in, byte[] into) throws IOException {
        int at = 0;
        while (at < into.length) {
            int count = in.read(into, at, into.length - at);
            if (count < 0) {
                throw new EOFException();
            }
            at += count;
        }
    }
}
