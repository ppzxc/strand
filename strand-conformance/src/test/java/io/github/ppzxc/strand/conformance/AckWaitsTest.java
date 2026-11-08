package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

class AckWaitsTest {

    private static final String PREFACE = "c0de535452300000";
    private static final String PING_ACK_0102 = "00000018000201000000000000000000" + "00000000" + "0102030405060708";

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
        // 길이 0xC1DE5354와 0x7FFFFFFC(서문이 아닌 바이트를 프레임으로 읽은 것). 완결되지 않은 프레임으로 기다려야 한다.
        for (String hex : new String[] {"c1de5354" + "5230ffff", "7ffffffc" + "00020100"}) {
            JsonObject v = limit1000(PING_START, recv(hex), advance(1000));

            assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1000L);
        }
    }

    @Test
    void aPrefaceWithReservedBytesIsSkippedAndTheFramesAfterItAreRead() {
        String helloAck = "00000010" + "0001" + "01" + "00" + "0000000000000000" + "0000" + "0000";
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
        String frame = "00000018" + "0063" + "00" + "00" + "0000000000000000" + "0000" + "0000" + fakeAck;
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

    @Test
    void aPingAckShorterThanARealPingFrameDoesNotEndTheWait() {
        // length 0x14인 짧은 프레임은 payload가 8바이트가 아니라 payload를 맞춰 볼 수 없다.
        String shortAck = "00000014" + "0002" + "01" + "00" + "0000000000000000" + "0000" + "0000" + "01020304";
        JsonObject v = limit1000(PING_START, recv(shortAck), advance(1000));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1000L);
    }

    @Test
    void aDisconnectCancelsTheOpenWaits() {
        String disconnect = "{\"kind\":\"disconnect\",\"send\":[],\"events\":[]}";
        JsonObject v = limit1000(PING_START, advance(600), disconnect, advance(400));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(600L);
        assertThat(AckWaits.reachesLimitExactly(v)).isFalse();
    }

    @Test
    void aCloseStepDoesNotCancelTheOpenWaits() {
        // CLOSE를 보낸 뒤에도 확인 대기는 이어져 시간 초과로 닫힌다(규격 3.3절).
        String close = "{\"kind\":\"close\",\"error_code\":\"NO_ERROR\",\"send\":[],\"events\":[]}";
        JsonObject v = limit1000(PING_START, advance(600), close, advance(400));

        assertThat(AckWaits.reachesLimitExactly(v)).isTrue();
    }

    @Test
    void aPingWithAnUnknownPayloadIsEndedByAnyPingAck() {
        String keepalive = "{\"kind\":\"advance\",\"ms\":1,\"send\":[{\"type\":\"PING\"}],\"events\":[]}";
        JsonObject v = limit1000(keepalive, recv(PING_ACK_0102), advance(1000));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1L);
    }

    @Test
    void anAckReceivedInAStepDoesNotEndTheWaitThatTheSameStepStarts() {
        String ackThenPing =
                "{\"kind\":\"recv\",\"hex\":\"" + PING_ACK_0102 + "\",\"send\":[{\"type\":\"PING\"}],\"events\":[]}";
        JsonObject v = limit1000(ackThenPing, advance(1000));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(1000L);
    }

    @Test
    void aClosedEventEndsTheOpenWaitsAfterItsStep() {
        String closing = "{\"kind\":\"advance\",\"ms\":500,\"send\":[],\"events\":[{\"event\":\"closed\"}]}";
        JsonObject v = limit1000(PING_START, closing, advance(500));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).containsExactly(500L);
        assertThat(AckWaits.reachesLimitExactly(v)).isFalse();
    }

    @Test
    void aDisconnectStartsANewByteStream() {
        String disconnect = "{\"kind\":\"disconnect\",\"send\":[],\"events\":[]}";
        String head = PING_ACK_0102.substring(0, 10);
        JsonObject v = limit1000(recv(head), disconnect, PING_START, recv(PING_ACK_0102), advance(1000));

        assertThat(AckWaits.elapsedAtEachAdvance(v)).isEmpty();
    }

    // ---- AUTH 대기(규격 3.3절, 3.3.1절) ----

    private static final String ZERO_STREAM = "0000000000000000";
    /** 20바이트 헤더의 AUTH(ACK). payload가 비어 있다. */
    private static final String AUTH_ACK = "00000010" + "0005" + "01" + "00" + ZERO_STREAM + "0000" + "0000";
    /** 20바이트 헤더의 AUTH. mechanism `A`, credentials 없음. */
    private static final String AUTH_FRAME = "00000012" + "0005" + "00" + "00" + ZERO_STREAM + "0000" + "0000" + "0141";

    private static final String CLIENT_AUTH_START =
            "{\"kind\":\"recv\",\"hex\":\"\",\"send\":[{\"type\":\"AUTH\",\"mechanism\":\"A\"}],\"events\":[]}";
    private static final String SERVER_HELLO_ACK_START =
            "{\"kind\":\"recv\",\"hex\":\"\",\"send\":[{\"type\":\"HELLO\",\"flags\":[\"ACK\"]}],\"events\":[]}";

    private static JsonObject authLimit(long ackMs, long authMs, String... steps) {
        return parseVector("{\"setup\":{\"config\":{\"ack_timeout_ms\":" + ackMs + ",\"auth_timeout_ms\":" + authMs
                + "}},\"steps\":[" + String.join(",", steps) + "]}");
    }

    @Test
    void aClientAuthStartsAnAckWaitThatAuthAckEnds() {
        JsonObject open = authLimit(1000, 99999, CLIENT_AUTH_START, advance(999));
        JsonObject ended = authLimit(1000, 99999, CLIENT_AUTH_START, advance(500), recv(AUTH_ACK), advance(500));

        assertThat(AckWaits.startsAnyWait(open)).isTrue();
        assertThat(AckWaits.elapsedAtEachAdvance(open)).containsExactly(999L);
        assertThat(AckWaits.elapsedAtEachAdvance(ended)).containsExactly(500L);
    }

    @Test
    void aClientAuthWaitUsesTheAckTimeoutLimit() {
        assertThat(AckWaits.reachesLimitExactly(authLimit(1000, 99999, CLIENT_AUTH_START, advance(1000))))
                .isTrue();
        assertThat(AckWaits.reachesLimitExactly(authLimit(99999, 1000, CLIENT_AUTH_START, advance(1000))))
                .isFalse();
    }

    @Test
    void aServerHelloAckStartsAnAuthWaitWithItsOwnLimitThatAnAuthFrameEnds() {
        JsonObject open = authLimit(99999, 1000, SERVER_HELLO_ACK_START, advance(600), advance(400));
        JsonObject ended = authLimit(99999, 1000, SERVER_HELLO_ACK_START, advance(600), recv(AUTH_FRAME), advance(400));

        assertThat(AckWaits.startsAuthWait(open)).isTrue();
        assertThat(AckWaits.reachesLimitExactly(open)).isTrue();
        assertThat(AckWaits.elapsedAtEachAdvance(ended)).containsExactly(600L);
        assertThat(AckWaits.reachesLimitExactly(authLimit(1000, 99999, SERVER_HELLO_ACK_START, advance(1000))))
                .isFalse();
    }

    @Test
    void theAuthWaitDefaultLimitDiffersFromTheAckWaitDefault() {
        JsonObject v = parseVector("{\"setup\":{},\"steps\":[" + SERVER_HELLO_ACK_START + "," + advance(5000) + "]}");

        assertThat(AckWaits.reachesLimitExactly(v)).isFalse();
        assertThat(AckWaits.reachesLimitExactly(parseVector(
                        "{\"setup\":{},\"steps\":[" + SERVER_HELLO_ACK_START + "," + advance(10000) + "]}")))
                .isTrue();
    }

    @Test
    void twoWaitsCrossingTheirLimitsInOneAdvanceAreDetected() {
        JsonObject both = authLimit(1000, 1000, helloAckAndHelloStart(), advance(1500));
        JsonObject apart = authLimit(1000, 3000, helloAckAndHelloStart(), advance(1500));

        assertThat(AckWaits.maxLimitsCrossedInOneAdvance(both)).isEqualTo(2);
        assertThat(AckWaits.maxLimitsCrossedInOneAdvance(apart)).isEqualTo(1);
    }

    @Test
    void twoWaitsWithTheSameCloseReasonCrossingTogetherCountOnce() {
        JsonObject v = limit1000(PING_START, pingStart("1111111111111111"), advance(1500));

        assertThat(AckWaits.maxLimitsCrossedInOneAdvance(v)).isEqualTo(1);
    }

    private static String helloAckAndHelloStart() {
        return "{\"kind\":\"recv\",\"hex\":\"\",\"send\":[{\"type\":\"HELLO\",\"flags\":[\"ACK\"]},"
                + "{\"type\":\"HELLO\"}],\"events\":[]}";
    }

    @Test
    void authAckReceivedThroughMarksTheStepsFromTheOneThatCarriesIt() {
        JsonObject v = limit1000(recv(PREFACE), recv(AUTH_ACK), advance(1));

        assertThat(AckWaits.authAckReceivedThrough(v)).containsExactly(false, true, true);
    }

    @Test
    void anAuthFrameWithoutTheAckFlagIsNotAnAuthAck() {
        JsonObject v = limit1000(recv(AUTH_FRAME), advance(1));

        assertThat(AckWaits.authAckReceivedThrough(v)).containsExactly(false, false);
    }
}
