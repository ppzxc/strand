package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;

final class InboundFrames {

    private static final int PREFACE_HEX_LENGTH = Protocol.PREFACE_HEX.length();
    private static final int PING_PAYLOAD_HEX = Protocol.PING_PAYLOAD_SIZE * 2;

    private final StringBuilder buffer = new StringBuilder();
    private boolean prefaceChecked;
    private boolean broken;

    static List<Frame> receivedBy(JsonObject step, InboundFrames stream) {
        StepKind.Inbound inbound = StepKind.of(step).inbound();
        if (inbound == StepKind.Inbound.STREAM) {
            return stream.feed(hexOf(step));
        }
        if (inbound == StepKind.Inbound.MESSAGE
                && step.get("opcode").getAsString().equals("binary")) {
            return new InboundFrames().feed(hexOf(step));
        }
        return new ArrayList<>();
    }

    private static String hexOf(JsonObject step) {
        return Steps.normalizedHex(step.get("hex").getAsString());
    }

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
