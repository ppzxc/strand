package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

class AckWaitsTest {

    private static final String PREFACE = "c0de535452300000";
    private static final String PING_ACK_0102 = "000000140002010000000000000000000102030405060708";

    private static JsonObject parseVector(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    private static String pingStart(String payload) {
        return "{\"kind\":\"ping\",\"payload\":\"" + payload + "\"," + "\"send\":[{\"type\":\"PING\"}],\"events\":[]}";
    }

    private static final String PING_START = pingStart("0102030405060708");

    private static String advance(long ms) {
        return "{\"kind\":\"advance\",\"ms\":" + ms + ",\"send\":[],\"events\":[]}";
    }

    private static String recv(String hex) {
        return "{\"kind\":\"recv\",\"hex\":\"" + hex + "\",\"send\":[],\"events\":[]}";
    }

    private static String recvMessage(String opcode, String hex) {
        return "{\"kind\":\"recv_message\",\"opcode\":\"" + opcode + "\",\"hex\":\"" + hex
                + "\",\"send\":[],\"events\":[]}";
    }

    private static JsonObject limit1000(String... steps) {
        return parseVector(
                "{\"setup\":{\"config\":{\"ack_timeout_ms\":1000}},\"steps\":[" + String.join(",", steps) + "]}");
    }

    private static JsonObject websocketLimit1000(String... steps) {
        return parseVector("{\"setup\":{\"transport\":\"websocket\",\"config\":{\"ack_timeout_ms\":1000}},"
                + "\"steps\":[" + String.join(",", steps) + "]}");
    }

    @Test
    void ackWaitIsCountedFromTheStepThatStartsIt() {
        // 대기 시작(PING 송신) 전의 advance 500은 세지 않는다. 구간은 500뿐이므로 한도 1000에 닿지 않는다.
        JsonObject v = limit1000(advance(500), PING_START, advance(500));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(500L);
        assertThat(AckWaits.reachesLimitExactly(v)).isFalse();
    }

    @Test
    void ackWaitEndsWhenTheAckIsReceived() {
        // ACK 수신 뒤의 advance는 같은 대기가 아니다.
        JsonObject v = limit1000(PING_START, advance(999), recv(PING_ACK_0102), advance(1));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(999L);
        assertThat(AckWaits.reachesLimitExactly(v)).isFalse();
    }

    @Test
    void ackWaitReachingTheLimitExactlyIsDetected() {
        assertThat(AckWaits.reachesLimitExactly(limit1000(PING_START, advance(600), advance(400))))
                .isTrue();
    }

    @Test
    void defaultLimitIsUsedWhenConfigIsOmitted() {
        JsonObject v = parseVector("{\"setup\":{},\"steps\":[" + PING_START + "," + advance(5000) + "]}");

        assertThat(AckWaits.reachesLimitExactly(v)).isTrue();
    }

    @Test
    void aFrameLengthThatOverflowsAnIntDoesNotCrashOrHang() {
        // 길이 0xC0DE5354(서문 예약 바이트가 ffff인 경우를 프레임으로 읽은 것)와 0x7FFFFFFC. 한 번 읽은 뒤 멈춰야 한다.
        for (String hex : new String[] {"c0de5354" + "5230ffff", "7ffffffc" + "00020100"}) {
            JsonObject v = limit1000(PING_START, recv(hex), advance(1000));

            assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1000L);
        }
    }

    @Test
    void aPrefaceWithReservedBytesIsSkippedAndTheFramesAfterItAreRead() {
        String helloAck = "00000003" + "000101";
        String helloStart = "{\"kind\":\"recv\",\"hex\":\"\",\"send\":[{\"type\":\"HELLO\"}],\"events\":[]}";
        JsonObject v = limit1000(helloStart, advance(999), recv("c0de53545230ffff" + helloAck), advance(1));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(999L);
    }

    @Test
    void anAckSplitAcrossRecvStepsEndsTheWaitOnceItIsComplete() {
        String head = PING_ACK_0102.substring(0, 10);
        String tail = PING_ACK_0102.substring(10);
        JsonObject v = limit1000(PING_START, recv(head), advance(500), recv(tail), advance(500));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(500L);
        assertThat(AckWaits.reachesLimitExactly(v)).isFalse();
    }

    @Test
    void bytesInsideAPayloadAreNotReadAsAFrameHeader() {
        // 첫 recv는 프레임 하나의 앞부분이고, 둘째 recv의 앞 14자리는 ACK PING 모양이지만 같은 프레임의 payload다.
        String fakeAck = "0002010000000000";
        String frame = "0000000c" + "0063" + "00" + "00" + fakeAck + "0000";
        JsonObject v = limit1000(PING_START, recv(frame.substring(0, 12)), recv(frame.substring(12)), advance(1000));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1000L);
    }

    @Test
    void anAckDeliveredInABinaryMessageEndsTheWait() {
        JsonObject v = websocketLimit1000(PING_START, advance(999), recvMessage("binary", PING_ACK_0102), advance(1));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(999L);
    }

    @Test
    void aTextMessageDoesNotEndTheWait() {
        JsonObject v = websocketLimit1000(PING_START, recvMessage("text", PING_ACK_0102), advance(1000));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1000L);
    }

    @Test
    void aSecondPingDoesNotResetTheFirstPingsWait() {
        JsonObject v = limit1000(PING_START, advance(600), pingStart("1111111111111111"), advance(400));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(600L, 1000L, 400L);
        assertThat(AckWaits.reachesLimitExactly(v)).isTrue();
    }

    @Test
    void aPingAckEndsOnlyTheWaitWithTheSamePayload() {
        JsonObject v = limit1000(PING_START, pingStart("1111111111111111"), recv(PING_ACK_0102), advance(1000));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1000L);
    }
}
