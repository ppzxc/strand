package io.github.ppzxc.strand.conformance;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

final class Bytes {

    private Bytes() {}

    static String compactHex(String hex) {
        return hex.replaceAll("\\s", "");
    }

    static byte[] fromHex(String hex) {
        String clean = compactHex(hex);
        if (clean.length() % 2 != 0) {
            throw new IllegalArgumentException("hex 자릿수가 홀수다: " + clean.length());
        }
        byte[] out = new byte[clean.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(clean.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    static int readUnsignedShort(byte[] bytes, int at) {
        return ((bytes[at] & 0xFF) << 8) | (bytes[at + 1] & 0xFF);
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

    static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    static void writeShort(ByteArrayOutputStream out, int value) {
        out.write(value >>> 8);
        out.write(value);
    }

    static void writeInt(ByteArrayOutputStream out, long value) {
        for (int shift = 24; shift >= 0; shift -= 8) {
            out.write((int) (value >>> shift));
        }
    }

    static void writeLong(ByteArrayOutputStream out, long value) {
        writeInt(out, value >>> 32);
        writeInt(out, value);
    }
}
