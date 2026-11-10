package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

/** AUTH(ACK) 수신 판정의 내부 불변식. 판정은 확인 대기 해석(AckWaits)과 무관하게 선다. */
class AuthGateTest {

    private static final String ZERO_STREAM = "0000000000000000";
    private static final String AUTH_ACK = "00000010" + "0005" + "01" + "00" + ZERO_STREAM + "0000" + "0000";
    private static final String AUTH_REQUEST = "00000010" + "0005" + "00" + "00" + ZERO_STREAM + "0000" + "0000";

    private static String recv(String hex) {
        return "{\"kind\":\"recv\",\"hex\":\"" + hex + "\",\"send\":[],\"events\":[]}";
    }

    private static JsonObject vector(String... steps) {
        return JsonParser.parseString("{\"setup\":{},\"steps\":[" + String.join(",", steps) + "]}")
                .getAsJsonObject();
    }

    @Test
    void anAuthAckIsKnownOnceItArrivesSplitAcrossRecvSteps() {
        JsonObject v = vector(recv(AUTH_ACK.substring(0, 10)), recv(AUTH_ACK.substring(10)));

        assertThat(AuthGate.ackReceivedThrough(v)).containsExactly(false, true);
    }

    @Test
    void anAuthRequestIsNotAnAuthAck() {
        assertThat(AuthGate.ackReceivedThrough(vector(recv(AUTH_REQUEST)))).containsExactly(false);
    }

    @Test
    void aDisconnectForgetsTheAuthAckOfTheEndedConnection() {
        String disconnect = "{\"kind\":\"disconnect\",\"send\":[],\"events\":[]}";
        JsonObject v = vector(recv(AUTH_ACK), disconnect, recv(AUTH_REQUEST));

        assertThat(AuthGate.ackReceivedThrough(v)).containsExactly(true, true, false);
    }
}
