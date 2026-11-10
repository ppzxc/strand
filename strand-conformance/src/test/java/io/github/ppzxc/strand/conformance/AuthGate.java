package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;

/**
 * AUTH(ACK)를 받았는가의 판정(규격 3.3절). 클라이언트 역할의 AUTH 관문이 열렸는지 보는 적재기의 교차 검사와, AUTH(ACK)가 확인 대기를
 * 끝내는지 보는 확인 대기 해석(AckWaits)이 같은 판정을 쓴다. 바이트를 읽는 방법은 {@link InboundFrames}를 따른다.
 */
final class AuthGate {

    private AuthGate() {}

    /** 받은 프레임이 AUTH(ACK)인가. ACK가 없는 AUTH는 서버가 받는 AUTH 요청이다. */
    static boolean isAuthAck(InboundFrames.Frame frame) {
        return frame.type == Protocol.TYPE_AUTH && frame.hasAckFlag();
    }

    /** 각 단계가 끝난 시점까지 AUTH(ACK)를 받았는가. 연결이 끝나면 새 연결은 다시 받지 않은 상태에서 시작한다. */
    static boolean[] ackReceivedThrough(JsonObject vector) {
        int count = vector.getAsJsonArray("steps").size();
        boolean[] result = new boolean[count];
        InboundFrames stream = new InboundFrames();
        boolean received = false;
        for (int i = 0; i < count; i++) {
            JsonObject step = vector.getAsJsonArray("steps").get(i).getAsJsonObject();
            for (InboundFrames.Frame frame : InboundFrames.receivedBy(step, stream)) {
                received |= isAuthAck(frame);
            }
            result[i] = received;
            if (InboundFrames.endsConnection(step)) {
                stream = new InboundFrames();
                received = false;
            }
        }
        return result;
    }
}
