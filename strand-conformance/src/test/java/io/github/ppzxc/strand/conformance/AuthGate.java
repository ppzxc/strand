package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

final class AuthGate {

    private AuthGate() {}

    static boolean isAuthAck(InboundFrames.Frame frame) {
        return frame.type == Protocol.TYPE_AUTH && frame.hasAckFlag();
    }

    static boolean isAuthRequest(InboundFrames.Frame frame) {
        return frame.type == Protocol.TYPE_AUTH && !frame.hasAckFlag();
    }

    static boolean[] ackReceivedThrough(JsonObject vector) {
        JsonArray steps = vector.getAsJsonArray("steps");
        int count = steps.size();
        boolean[] result = new boolean[count];
        InboundFrames stream = new InboundFrames();
        boolean received = false;
        for (int i = 0; i < count; i++) {
            JsonObject step = steps.get(i).getAsJsonObject();
            for (InboundFrames.Frame frame : InboundFrames.receivedBy(step, stream)) {
                received |= isAuthAck(frame);
            }
            result[i] = received;
            if (Steps.endsConnection(step)) {
                stream = new InboundFrames();
                received = false;
            }
        }
        return result;
    }
}
