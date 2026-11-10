package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * 벡터의 `recv`·`recv_message` 단계로 들어온 바이트를 프레임 단위로 읽는다. 바이트는 `recv` 단계에 걸쳐 이어 읽고(프레임 경계와
 * 무관하게 잘려 올 수 있다), 완결된 프레임만 돌려준다. `recv_message`는 binary 메시지 하나를 따로 읽고 text 메시지는 읽지 않는다.
 * 프레임 길이가 16 미만(규격 2.2.1절의 최솟값)이면 헤더를 읽을 수 없는 깨진 바이트이므로 그 연결의 나머지는 읽지 않는다.
 */
final class InboundFrames {

    private static final int PREFACE_HEX_LENGTH = Protocol.PREFACE_HEX.length();
    private static final int PING_PAYLOAD_HEX = Protocol.PING_PAYLOAD_SIZE * 2;

    private final StringBuilder buffer = new StringBuilder();
    private boolean prefaceChecked;
    private boolean broken;

    /** 이 단계로 들어온 바이트에서 새로 완결된 프레임들. 연결 하나의 흐름은 `stream`이 이어 읽는다. */
    static List<Frame> receivedBy(JsonObject step, InboundFrames stream) {
        String kind = Steps.kindOf(step);
        if (kind.equals("recv")) {
            return stream.feed(hexOf(step));
        }
        if (kind.equals("recv_message") && step.get("opcode").getAsString().equals("binary")) {
            return new InboundFrames().feed(hexOf(step));
        }
        return new ArrayList<>();
    }

    private static String hexOf(JsonObject step) {
        return Steps.normalizedHex(step.get("hex").getAsString());
    }

    /** 완결된 프레임 하나. */
    static final class Frame {
        final int type;
        final int flags;
        private final String hex;

        private Frame(String hex) {
            this.hex = hex;
            this.type = Integer.parseInt(hex.substring(Protocol.TYPE_HEX_AT, Protocol.TYPE_HEX_AT + 4), 16);
            this.flags = Integer.parseInt(hex.substring(Protocol.FLAGS_HEX_AT, Protocol.FLAGS_HEX_AT + 2), 16);
        }

        boolean hasAckFlag() {
            return (flags & Protocol.ACK_FLAG) != 0;
        }

        /** 헤더 20바이트와 확장 영역 뒤의 payload. 정확히 8바이트가 아니면 null이다(PING 말고는 payload를 맞춰 보지 않는다). */
        String pingSizedPayload() {
            int extensionsHex = Integer.parseInt(
                            hex.substring(Protocol.EXTENSIONS_LENGTH_HEX_AT, Protocol.EXTENSIONS_LENGTH_HEX_AT + 4), 16)
                    * 2;
            int from = Protocol.HEADER_HEX + extensionsHex;
            return hex.length() - from == PING_PAYLOAD_HEX ? hex.substring(from) : null;
        }
    }

    private List<Frame> feed(String hex) {
        List<Frame> frames = new ArrayList<>();
        if (broken) {
            return frames;
        }
        buffer.append(hex);
        if (!prefaceChecked && !skipPreface()) {
            return frames;
        }
        while (!broken && buffer.length() >= Protocol.LENGTH_HEX) {
            long length = Long.parseLong(buffer.substring(0, Protocol.LENGTH_HEX), 16);
            if (length < Protocol.MIN_FRAME_LENGTH) {
                broken = true;
                break;
            }
            // length는 최대 2^32 - 1이라 long에서 넘치지 않는다. 프레임이 다 오지 않았으면 다음 바이트를 기다린다.
            long frameHex = (Protocol.LENGTH_SIZE + length) * 2;
            if (buffer.length() < frameHex) {
                break;
            }
            String frame = buffer.substring(0, (int) frameHex);
            buffer.delete(0, (int) frameHex);
            frames.add(new Frame(frame));
        }
        return frames;
    }

    /** 흐름 맨 앞이 연결 서문이면 8바이트를 건너뛴다. 서문인지 알 만큼 바이트가 모였으면 true. */
    private boolean skipPreface() {
        int compared = Math.min(buffer.length(), Protocol.PREFACE_PREFIX_HEX.length());
        if (!buffer.substring(0, compared).equals(Protocol.PREFACE_PREFIX_HEX.substring(0, compared))) {
            prefaceChecked = true;
            return true;
        }
        if (buffer.length() < PREFACE_HEX_LENGTH) {
            return false;
        }
        buffer.delete(0, PREFACE_HEX_LENGTH);
        prefaceChecked = true;
        return true;
    }
}
