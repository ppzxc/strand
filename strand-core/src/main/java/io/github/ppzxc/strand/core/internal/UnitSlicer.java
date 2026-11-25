package io.github.ppzxc.strand.core.internal;

import java.util.Arrays;

final class UnitSlicer {

    private static final int PREFACE_PREFIX_LENGTH = 6;

    private static final byte[] NO_BYTES = new byte[0];

    private static final byte[] PREFACE = Frames.preface();

    private static final int PENDING_MIN_CAPACITY = 64;

    private static final int PENDING_RELEASE_ABOVE = 65_536;

    enum Stage {
        PREFACE,
        FIRST_FRAME,
        FRAME
    }

    interface Sink {

        boolean isEnded();

        Stage stage();

        boolean hasSpace();

        void prefaceReceived();

        void firstFrameVerified();

        void frame(byte[] bytes, int off, int size);

        void closeWithoutClose();

        void badLength(byte[] bytes, int off);

        void connectionError();
    }

    private final long maxFrameSize;
    private byte[] pending = NO_BYTES;

    private int pendingLength;

    private int wantedLength;

    UnitSlicer(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    boolean hasBuffered() {
        return pendingLength > 0;
    }

    void discard() {
        pending = NO_BYTES;
        pendingLength = 0;
    }

    ReceiveProgress slice(byte[] bytes, int offset, int length, int maximumUnits, Sink sink) {
        int at = offset;
        int end = offset + length;
        int units = 0;
        boolean collectionRequired = false;
        while (!sink.isEnded() && units < maximumUnits) {
            if (pendingLength == 0 && at == end) {
                break;
            }
            if (!sink.hasSpace()) {
                collectionRequired = true;
                break;
            }
            if (pendingLength > 0) {
                int consumed = consumeUnit(pending, 0, pendingLength, sink);
                if (consumed > 0) {
                    release();
                    units++;
                } else if (sink.isEnded() || at == end) {
                    break;
                } else {
                    int take = Math.min(wantedLength - pendingLength, end - at);
                    if (take <= 0) {
                        throw new IllegalStateException("더 필요한 바이트 수가 정해지지 않았다");
                    }
                    append(bytes, at, take);
                    at += take;
                }
            } else {
                int consumed = consumeUnit(bytes, at, end - at, sink);
                if (consumed > 0) {
                    at += consumed;
                    units++;
                } else {
                    if (!sink.isEnded()) {
                        append(bytes, at, end - at);
                        at = end;
                    }
                    break;
                }
            }
        }
        return new ReceiveProgress(at - offset, collectionRequired);
    }

    void sliceWhole(byte[] bytes, int off, int length, boolean supported, Sink sink) {
        Stage stage = sink.stage();
        if (stage == Stage.PREFACE) {
            if (supported && length == PREFACE.length && matchesPrefacePrefix(bytes, off, length)) {
                sink.prefaceReceived();
            } else {
                sink.closeWithoutClose();
            }
            return;
        }
        if (stage == Stage.FIRST_FRAME) {
            boolean valid = supported
                    && length >= Frames.HEADER_SIZE
                    && Frames.readUnsignedInt(bytes, off) >= Frames.MIN_LENGTH
                    && isFirstFrameHeader(bytes, off);
            if (!valid) {
                sink.closeWithoutClose();
                return;
            }
            sink.firstFrameVerified();
        }
        if (!supported || length < Frames.TYPE_END) {
            sink.connectionError();
            return;
        }
        long declared = Frames.readUnsignedInt(bytes, off);
        if (declared < Frames.MIN_LENGTH
                || declared > maxFrameSize - Frames.LENGTH_FIELD_SIZE
                || length != Frames.LENGTH_FIELD_SIZE + declared) {
            sink.badLength(bytes, off);
            return;
        }
        sink.frame(bytes, off, length);
    }

    private int consumeUnit(byte[] bytes, int off, int available, Sink sink) {
        Stage stage = sink.stage();
        if (stage == Stage.PREFACE) {
            return consumePreface(bytes, off, available, sink);
        }
        if (available < Frames.LENGTH_FIELD_SIZE) {
            wantedLength = Frames.LENGTH_FIELD_SIZE;
            return 0;
        }
        long length = Frames.readUnsignedInt(bytes, off);
        if (stage == Stage.FIRST_FRAME) {

            if (length < Frames.MIN_LENGTH) {
                sink.closeWithoutClose();
                return 0;
            }
            if (available < Frames.HEADER_SIZE) {
                wantedLength = Frames.HEADER_SIZE;
                return 0;
            }
            if (!isFirstFrameHeader(bytes, off)) {
                sink.closeWithoutClose();
                return 0;
            }
            sink.firstFrameVerified();
        }
        if (length < Frames.MIN_LENGTH || length > maxFrameSize - Frames.LENGTH_FIELD_SIZE) {
            if (available < Frames.TYPE_END) {

                wantedLength = Frames.TYPE_END;
                return 0;
            }
            sink.badLength(bytes, off);
            return 0;
        }
        int frameSize = (int) (Frames.LENGTH_FIELD_SIZE + length);
        if (available < frameSize) {
            wantedLength = frameSize;
            return 0;
        }
        sink.frame(bytes, off, frameSize);
        return frameSize;
    }

    private int consumePreface(byte[] bytes, int off, int available, Sink sink) {
        int prefaceSize = PREFACE.length;
        if (!matchesPrefacePrefix(bytes, off, available)) {
            sink.closeWithoutClose();
            return 0;
        }
        if (available < prefaceSize) {
            wantedLength = prefaceSize;
            return 0;
        }
        sink.prefaceReceived();
        return prefaceSize;
    }

    private void append(byte[] bytes, int at, int length) {
        int needed = pendingLength + length;
        if (needed > pending.length) {

            int doubled = Math.max(PENDING_MIN_CAPACITY, pending.length * 2);
            pending = Arrays.copyOf(pending, Math.max(needed, Math.min(doubled, wantedLength)));
        }
        System.arraycopy(bytes, at, pending, pendingLength, length);
        pendingLength = needed;
    }

    private void release() {
        pendingLength = 0;
        if (pending.length > PENDING_RELEASE_ABOVE) {
            pending = NO_BYTES;
        }
    }

    private static boolean matchesPrefacePrefix(byte[] bytes, int off, int available) {
        byte[] preface = PREFACE;
        int compared = Math.min(available, PREFACE_PREFIX_LENGTH);
        for (int i = 0; i < compared; i++) {
            if (bytes[off + i] != preface[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean isFirstFrameHeader(byte[] bytes, int off) {
        return Frames.typeOf(bytes, off) == FrameType.HELLO
                && Frames.streamIdOf(bytes, off) == 0
                && Frames.flagsOf(bytes, off) == 0;
    }
}
