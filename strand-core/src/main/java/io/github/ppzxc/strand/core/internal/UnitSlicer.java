package io.github.ppzxc.strand.core.internal;

import java.util.Arrays;

/**
 * 받은 바이트를 단위(서문 또는 프레임 하나)로 자르는 모듈. 스트림 입력에서 경계에 걸친 단위 하나를 모으는 재조립 버퍼와, 단위 입력의 크기 검사를
 * 맡는다. 상태 기계({@link Endpoint})에는 프레임 하나와 경계 위반만 넘긴다. 입력을 처리할 수 있는지, 수거 공간이 있는지는 {@link Sink}가 정한다.
 *
 * <p>모으는 크기는 처리 중인 단위의 크기를 넘지 않고, 그 크기는 {@code max_frame_size} 안이다. 한 청크 안에 통째로 든 단위는 들어온 배열에서 바로
 * 처리해 복사하지 않는다.
 */
final class UnitSlicer {
    /** 서문의 앞 6바이트만 비교한다(규격 3.1절). 나머지 2바이트는 모두 받은 뒤 건너뛴다. */
    private static final int PREFACE_PREFIX_LENGTH = 6;

    private static final byte[] NO_BYTES = new byte[0];
    /** 연결 서문. 비교에만 쓰고 보내거나 바꾸지 않는다(보내는 쪽은 {@link Frames#preface()}가 복사본을 준다). */
    private static final byte[] PREFACE = Frames.preface();
    /** 경계에 걸친 단위를 모으는 버퍼의 처음 크기. */
    private static final int PENDING_MIN_CAPACITY = 64;
    /** 다 비웠을 때 이 크기를 넘는 버퍼는 놓는다. 큰 프레임 하나 때문에 연결이 그 크기를 계속 붙잡지 않게 한다. */
    private static final int PENDING_RELEASE_ABOVE = 65_536;

    /** 다음에 올 단위의 종류. 연결 단계가 정한다. */
    enum Stage {
        PREFACE,
        FIRST_FRAME,
        FRAME
    }

    /** 자른 결과를 받는 쪽. 상태 기계다. */
    interface Sink {
        /** 연결이 끝났는가. 끝났으면 더 자르지 않는다. */
        boolean isEnded();

        /** 다음에 올 단위의 종류. */
        Stage stage();

        /**
         * 단위 하나를 처리할 만큼 수거 공간이 있는가. 거짓이면 그 단위 앞에서 멈추고 수거를 요청한다.
         *
         * @throws IllegalStateException 수거할 것이 없는데도 공간이 모자랄 때
         */
        boolean hasSpace();

        /** 서문을 모두 받았다. */
        void prefaceReceived();

        /** 첫 프레임의 헤더를 검사했다(규격 3.2.4절). 같은 프레임이 이어서 {@link #frame}으로 넘어온다. */
        void firstFrameVerified();

        /** 프레임 하나. 호출이 돌아온 뒤에는 읽지 않는다. */
        void frame(byte[] bytes, int off, int size);

        /** 서문 또는 첫 프레임 검사 위반: 아무것도 보내지 않고 닫는다(규격 3.1절, 3.2.4절). */
        void closeWithoutClose();

        /** 길이 필드가 범위를 벗어났거나 단위 크기와 어긋난 프레임: {@code type}으로 CLOSE 여부를 가른다(규격 3.5절, 5.1절). */
        void badLength(byte[] bytes, int off);

        /** 프레임의 모양을 읽을 수 없는 단위(너무 짧거나 바이트가 아님): 연결 오류다. */
        void connectionError();
    }

    private final long maxFrameSize;
    private byte[] pending = NO_BYTES;
    /** {@link #pending}에서 쓰인 바이트 수. 0이면 모은 것이 없다. */
    private int pendingLength;
    /** {@link #slice}가 더 모아야 할 때, 진행하려면 단위의 앞에서부터 있어야 하는 총 바이트 수. */
    private int wantedLength;

    UnitSlicer(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    /** 스트림으로 받아 두고 아직 단위가 되지 못한 바이트가 있는가. */
    boolean hasBuffered() {
        return pendingLength > 0;
    }

    /** 모은 바이트를 버린다. 큰 버퍼까지 놓는다. 연결이 끝났을 때 부른다. */
    void discard() {
        pending = NO_BYTES;
        pendingLength = 0;
    }

    /**
     * 스트림 입력을 최대 {@code maximumUnits}개 단위로 잘라 넘긴다. 프레임 경계와 무관하게 어디서 잘라도 결과가 같다.
     *
     * @return 소비한 바이트 수와 수거 필요 여부
     */
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

    /**
     * 정확히 한 단위라고 주장하는 입력을 넘긴다. 크기가 어긋나면 연결 단계에 맞게 {@link Sink}에 위반을 알린다.
     *
     * @param supported 바이트로 넘길 수 있는 입력인가(텍스트 메시지 같은 입력은 아니다)
     */
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

    /**
     * {@code off}에서 시작하는 {@code available}바이트에서 한 단위를 넘긴다. 넘겼으면 소비한 바이트 수를 돌려준다. 모자라면 0을 돌려주고
     * {@link #wantedLength}에 진행하려면 있어야 하는 총 바이트 수를 둔다. 판정이 나서 닫았으면 0을 돌려준다. 도착 단위에 따라 결과가 달라지면
     * 안 되므로, 판정에 필요한 바이트가 모이기 전에는 아무것도 하지 않는다.
     */
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
            // 첫 프레임 검사(규격 3.2.4절): 헤더를 읽을 수 없거나 HELLO가 아니면 아무것도 보내지 않고 닫는다.
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
                // type을 읽을 때까지 기다린다. CLOSE이면 받은 CLOSE로 보고 CLOSE 없이 닫아야 하므로 도착 단위에 따라 결과가 달라지면 안 된다.
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

    /** 서문의 앞 6바이트를 비교한다(규격 3.1절). 판정할 만큼 모였으면 서문 8바이트를 건너뛴다. 서문은 8바이트를 모두 받은 뒤 건너뛴다. */
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
            // 받은 만큼에 맞춰 두 배씩 키운다. 상한은 처리 중인 단위의 크기(wantedLength, 최대 max_frame_size)다.
            int doubled = Math.max(PENDING_MIN_CAPACITY, pending.length * 2);
            pending = Arrays.copyOf(pending, Math.max(needed, Math.min(doubled, wantedLength)));
        }
        System.arraycopy(bytes, at, pending, pendingLength, length);
        pendingLength = needed;
    }

    /** 모은 바이트를 모두 처리했다. 큰 버퍼는 놓는다. */
    private void release() {
        pendingLength = 0;
        if (pending.length > PENDING_RELEASE_ABOVE) {
            pending = NO_BYTES;
        }
    }

    /** 서문의 앞 6바이트와, 받은 만큼의 앞부분이 일치하는가. */
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

    /** 첫 프레임의 헤더가 첫 프레임 검사(규격 3.2.4절)를 통과하는가: HELLO이고 stream_id 0, flags 0. */
    private static boolean isFirstFrameHeader(byte[] bytes, int off) {
        return Frames.typeOf(bytes, off) == FrameType.HELLO
                && Frames.streamIdOf(bytes, off) == 0
                && Frames.flagsOf(bytes, off) == 0;
    }
}
