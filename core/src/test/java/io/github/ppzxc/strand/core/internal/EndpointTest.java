package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 엔드포인트의 외부 동작만 본다: 받은 바이트와 애플리케이션 호출에 대해 보낸 바이트와 애플리케이션 이벤트. */
class EndpointTest {

    private static final String PREFACE = "c0de535452300000";
    private static final String HELLO = "0000000c000100000000000000000000";
    private static final String HELLO_ACK = "0000000c000101000000000000000000";
    private static final String PING = "00000014 00020000 00000000 00000000 0102030405060708";
    private static final String PING_ACK = "00000014000201000000000000000000" + "0102030405060708";
    private static final String CLOSE_PROTOCOL_ERROR = "00000010000300000000000000000000" + "00000001";
    private static final String CLOSE_NO_ERROR = "00000010000300000000000000000000" + "00000000";
    private static final String PING_PAYLOAD = "0102030405060708";
    private static final EndpointConfig ONE_SECOND = EndpointConfig.defaults().ackTimeoutMs(1000);
    private static final String DEFAULT_HELLO_PARAMS = "0001 00400000 0002 00000064 0003 00000000".replace(" ", "");

    static byte[] bytes(String hex) {
        String clean = hex.replaceAll("\\s", "");
        byte[] out = new byte[clean.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(clean.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /** 파라미터 HELLO 프레임. payload는 hex. */
    private static String helloWith(String payloadHex) {
        return String.format("%08x", 12 + payloadHex.length() / 2) + "000100000000000000000000" + payloadHex;
    }

    private static String sent(Endpoint endpoint) {
        return hex(endpoint.drainOutput());
    }

    private static Endpoint server() {
        return Endpoint.server(EndpointConfig.defaults());
    }

    private static Endpoint serverAfterPreface(EndpointConfig config) {
        Endpoint server = Endpoint.server(config);
        server.receive(bytes(PREFACE));
        server.drainOutput();
        return server;
    }

    private static Endpoint serverAfterPreface() {
        return serverAfterPreface(EndpointConfig.defaults());
    }

    /** 서버: 상대 HELLO에 ACK를 보냈고 상대 ACK도 받은 상태. */
    private static Endpoint established() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainOutput();
        assertThat(server.drainEvents()).isEmpty();
        return server;
    }

    private static Endpoint establishedWith(EndpointConfig config) {
        Endpoint server = serverAfterPreface(config);
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainOutput();
        return server;
    }

    private static Endpoint client() {
        return Endpoint.client(EndpointConfig.defaults());
    }

    /** 클라이언트: 서버 HELLO에 ACK를 보냈고 서버 ACK도 받은 상태. */
    private static Endpoint establishedClient() {
        Endpoint client = client();
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.drainOutput();
        assertThat(client.drainEvents()).isEmpty();
        return client;
    }

    private static List<Event.Kind> kinds(Endpoint endpoint) {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : endpoint.drainEvents()) {
            kinds.add(event.kind());
        }
        return kinds;
    }

    private static void assertClosedWithProtocolError(Endpoint endpoint) {
        assertThat(sent(endpoint)).isEqualTo(CLOSE_PROTOCOL_ERROR);
        assertThat(kinds(endpoint)).containsExactly(Event.Kind.CLOSED);
    }

    private static void assertClosedSilently(Endpoint endpoint) {
        assertThat(endpoint.drainOutput()).isEmpty();
        assertThat(kinds(endpoint)).containsExactly(Event.Kind.CLOSED);
    }

    private static void assertOpenAndSilent(Endpoint endpoint) {
        assertThat(endpoint.drainOutput()).isEmpty();
        assertThat(endpoint.drainEvents()).isEmpty();
    }

    // ---- 서버: 연결 서문과 HELLO ----

    @Test
    void serverSendsItsHelloRightAfterThePreface() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().maxFrameSize(16384));

        server.receive(bytes(PREFACE));

        assertThat(sent(server))
                .isEqualTo("0000001e000100000000000000000000" + "0001000040000002000000640003" + "00000000");
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void prefaceReservedBytesAreIgnored() {
        Endpoint server = server();

        server.receive(bytes("c0de53545230ffff"));

        assertThat(sent(server)).isEqualTo("0000001e000100000000000000000000" + DEFAULT_HELLO_PARAMS);
    }

    @Test
    void wrongPrefaceClosesSilently() {
        Endpoint server = server();

        server.receive(bytes("c0de535452310000"));

        assertClosedSilently(server);
    }

    @Test
    void anHttpRequestInPlaceOfThePrefaceClosesSilently() {
        Endpoint server = server();

        server.receive(bytes("474554202f204854"));

        assertClosedSilently(server);
    }

    @Test
    void aPartialPrefaceWaitsForTheRest() {
        Endpoint server = server();

        server.receive(bytes("c0de5354"));

        assertOpenAndSilent(server);
    }

    @Test
    void helloIsAcknowledgedWithAnEmptyAckHello() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes(HELLO));

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
    }

    @Test
    void ackHelloProducesNoOutput() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainOutput();

        server.receive(bytes(HELLO_ACK));

        assertOpenAndSilent(server);
    }

    @Test
    void pingIsAnsweredWithTheSameEightBytesAndAckFlag() {
        Endpoint server = established();

        server.receive(bytes(PING));

        assertThat(sent(server)).isEqualTo(PING_ACK);
    }

    @Test
    void anAckPingIsNotAnswered() {
        Endpoint server = established();

        server.receive(bytes(PING_ACK));

        assertOpenAndSilent(server);
    }

    @Test
    void bytesMaySplitAtAnyPosition() {
        Endpoint server = serverAfterPreface();

        for (byte b : bytes(HELLO)) {
            assertThat(server.drainOutput()).isEmpty();
            server.receive(new byte[] {b});
        }

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
    }

    @Test
    void drainReturnsEachOutputOnce() {
        Endpoint server = server();
        server.receive(bytes(PREFACE));

        assertThat(server.drainOutput()).isNotEmpty();
        assertThat(server.drainOutput()).isEmpty();
    }

    // ---- 첫 프레임 검사(3.2.4절) ----

    @Test
    void firstFrameThatIsNotHelloClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes(PING));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithAnApplicationTypeClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000000c010100000000000100000000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithAReservedControlTypeClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000000c000500000000000000000000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithANonzeroStreamIdClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000000c000100000000000100000000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithTheAckFlagClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes(HELLO_ACK));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithAnUndefinedFlagClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000000c000180000000000000000000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithLengthBelowTheMinimumClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("00000004"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameStatusAndReservedAreNotChecked() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000000c000100ff000000000000ffff"));

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
    }

    @Test
    void firstFrameHelloAboveTheOwnLimitClosesWithProtocolError() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().maxFrameSize(16384));

        // 헤더 16바이트만으로 판정한다. length = 16381 = 알린 한도 − 4 + 1.
        server.receive(bytes("00003ffd 0001 00 00 00000000 0000 0000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void firstFrameOfTheWrongTypeAboveTheOwnLimitClosesSilently() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().maxFrameSize(16384));

        server.receive(bytes("00003ffd 0002 00 00 00000000 0000 0000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameHelloWithExtensionsClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000001000010000000000000004000000090000"));

        assertClosedWithProtocolError(server);
    }

    // ---- HELLO 값과 형태(3.2.3절, 3.2.5절) ----

    @Test
    void maxFrameSizeAtTheRangeBoundsIsAccepted() {
        for (String size : new String[] {"00004000", "01000000"}) {
            Endpoint server = serverAfterPreface();

            server.receive(bytes(helloWith("0001" + size)));

            assertThat(sent(server)).isEqualTo(HELLO_ACK);
        }
    }

    @Test
    void maxFrameSizeOutsideTheRangeClosesWithProtocolError() {
        for (String size : new String[] {"00003fff", "01000001"}) {
            Endpoint server = serverAfterPreface();

            server.receive(bytes(helloWith("0001" + size)));

            assertClosedWithProtocolError(server);
        }
    }

    @Test
    void helloPayloadThatIsNotAMultipleOfSixClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes(helloWith("0000000000")));

        assertClosedWithProtocolError(server);
    }

    @Test
    void duplicateKnownOrUnknownIdentifiersCloseWithProtocolError() {
        for (String payload : new String[] {"000200000064" + "000200000032", "00ff00000001" + "00ff00000002"}) {
            Endpoint server = serverAfterPreface();

            server.receive(bytes(helloWith(payload)));

            assertClosedWithProtocolError(server);
        }
    }

    @Test
    void unknownIdentifiersAreIgnored() {
        Endpoint server = serverAfterPreface();
        StringBuilder payload = new StringBuilder();
        for (int id = 0x1000; id < 0x1000 + 63; id++) {
            payload.append(String.format("%04x00000000", id));
        }

        server.receive(bytes(helloWith(payload + "000200000002")));

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
    }

    @Test
    void ackHelloWithAPayloadClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainOutput();

        server.receive(bytes("00000012000101000000000000000000000200000064"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void ackHelloWithANonzeroStreamIdClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainOutput();

        server.receive(bytes("0000000c000101000000000100000000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void aSecondParameterHelloClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainOutput();

        server.receive(bytes(HELLO));

        assertClosedWithProtocolError(server);
    }

    @Test
    void aSecondAckClosesWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes(HELLO_ACK));

        assertClosedWithProtocolError(server);
    }

    // ---- 프레임 길이 범위(2.2.1절) ----

    @Test
    void frameLengthAboveTheAnnouncedLimitClosesWithProtocolError() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().maxFrameSize(16384));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainOutput();

        // 본문이 다 오기 전에 헤더 앞 4바이트만으로 판정한다.
        server.receive(bytes("00003ffd"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void frameLengthAtTheAnnouncedLimitIsAccepted() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().maxFrameSize(16384));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainOutput();

        server.receive(bytes("00003ffc"));

        assertOpenAndSilent(server);
    }

    @Test
    void frameLengthBelowTheMinimumClosesWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("0000000b"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void nothingIsHandledOnceClosed() {
        Endpoint server = established();
        server.receive(bytes("0000000b"));
        server.drainOutput();
        server.drainEvents();

        server.receive(bytes(PING));

        assertOpenAndSilent(server);
    }

    @Test
    void controlFramesWithExtensionsCloseWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("00000010 0002 00 00 00000000 0004 0000 00000000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void extensionsLongerThanTheFrameCloseWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("0000000c000500000000000000050000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void controlFramesWithANonzeroStreamIdCloseWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("00000014000200000000000100000000" + "0102030405060708"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void pingPayloadMustBeExactlyEightBytesEvenForAnAck() {
        for (String frame : new String[] {
            "0000000c000200000000000000000000",
            "0000000c000201000000000000000000",
            "0000001500020100000000000000000001020304050607" + "0809"
        }) {
            Endpoint server = established();

            server.receive(bytes(frame));

            assertClosedWithProtocolError(server);
        }
    }

    // ---- ACK 관문(3.3절) ----

    @Test
    void pingResetAndApplicationFramesBeforeThePeerAckCloseWithProtocolError() {
        for (String frame :
                new String[] {PING, "0000001000040000000000010000000000000004", "0000000c010100000000000100000000"}) {
            Endpoint server = serverAfterPreface();
            server.receive(bytes(HELLO));
            server.drainOutput();

            server.receive(bytes(frame));

            assertClosedWithProtocolError(server);
        }
    }

    @Test
    void reservedControlTypesAreIgnoredBeforeThePeerAck() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainOutput();

        server.receive(bytes("0000000c000500000000000000000000"));
        assertOpenAndSilent(server);
        server.receive(bytes(HELLO_ACK));
        server.receive(bytes(PING));

        assertThat(sent(server)).isEqualTo(PING_ACK);
    }

    @Test
    void closeIsAcceptedBeforeThePeerAck() {
        Endpoint noError = serverAfterPreface();
        noError.receive(bytes(HELLO));
        noError.drainOutput();
        Endpoint error = serverAfterPreface();
        error.receive(bytes(HELLO));
        error.drainOutput();

        noError.receive(bytes(CLOSE_NO_ERROR));
        error.receive(bytes(CLOSE_PROTOCOL_ERROR));

        assertClosedSilently(noError);
        assertClosedSilently(error);
    }

    @Test
    void aMalformedCloseIsTreatedAsAReceivedCloseAndClosesWithoutSending() {
        Endpoint server = established();

        server.receive(bytes("0000000f 0003 00 00 00000000 0000 0000 000000".replace(" ", "")));

        assertClosedSilently(server);
    }

    @Test
    void anErrorCloseClosesImmediatelyEvenWithPendingRequests() {
        Endpoint server = established();
        server.request(257, new byte[0]);
        server.drainOutput();

        server.receive(bytes(CLOSE_PROTOCOL_ERROR));

        assertThat(server.drainOutput()).isEmpty();
        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(2);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(events.get(0).streamId()).isEqualTo(2);
        assertThat(events.get(0).status()).isEqualTo(Event.UNAVAILABLE);
        assertThat(events.get(1).kind()).isEqualTo(Event.Kind.CLOSED);
    }

    @Test
    void connectionErrorAfterCloseWasReceivedSendsNoClose() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainOutput();
        server.request(257, new byte[0]);
        server.drainOutput();
        server.receive(bytes(CLOSE_NO_ERROR));
        assertOpenAndSilent(server);

        // 상대 ACK 전의 PING은 연결 오류지만 CLOSE를 이미 받았으므로 CLOSE를 보내지 않는다.
        server.receive(bytes(PING));

        assertThat(server.drainOutput()).isEmpty();
        assertThat(kinds(server)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void noErrorCloseWithoutPendingRequestsClosesAtOnce() {
        Endpoint server = established();

        server.receive(bytes(CLOSE_NO_ERROR));

        assertClosedSilently(server);
    }

    // ---- 확인 대기(3.3절) ----

    @Test
    void serverClosesWhenTheAckDoesNotComeInTime() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.receive(bytes(HELLO));
        server.drainOutput();

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(2);

        assertClosedWithProtocolError(server);
    }

    @Test
    void theAckCancelsTheWait() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.receive(bytes(HELLO));
        server.advance(999);
        server.receive(bytes(HELLO_ACK));
        server.drainOutput();

        server.advance(10000);

        assertOpenAndSilent(server);
    }

    @Test
    void theWaitStartsWhenTheHelloIsSent() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.advance(5000);
        server.receive(bytes(PREFACE));
        server.drainOutput();

        server.advance(999);

        assertOpenAndSilent(server);
    }

    @Test
    void noClockEffectOnceClosed() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.receive(bytes(CLOSE_NO_ERROR));
        server.drainOutput();
        server.drainEvents();

        server.advance(5000);

        assertOpenAndSilent(server);
    }

    @Test
    void theWaitIsDroppedWhenCloseWasReceivedEarlier() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.receive(bytes(HELLO));
        server.drainOutput();
        server.request(257, new byte[0]);
        server.receive(bytes(CLOSE_NO_ERROR));
        server.drainOutput();
        server.drainEvents();

        server.advance(2000);

        // CLOSE를 이미 받았으므로 확인 대기 초과에도 CLOSE를 보내지 않고 닫는다.
        assertThat(server.drainOutput()).isEmpty();
        assertThat(kinds(server)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    // ---- 애플리케이션 호출 ----

    @Test
    void requestUsesTheOwnNumberSpaceInOrder() {
        Endpoint server = established();

        server.request(257, new byte[] {1, 2});
        server.request(258, new byte[0]);

        assertThat(sent(server))
                .isEqualTo("0000000e010100000000000200000000" + "0102" + "0000000c010200000000000400000000");
    }

    @Test
    void clientRequestsStartAtOne() {
        Endpoint client = establishedClient();

        client.request(257, new byte[0]);

        assertThat(sent(client)).isEqualTo("0000000c010100000000000100000000");
    }

    @Test
    void requestBeforeAckingThePeerHelloWaitsForTheAck() {
        Endpoint server = serverAfterPreface();

        server.request(257, new byte[0]);
        assertOpenAndSilent(server);
        server.receive(bytes(HELLO));

        assertThat(sent(server)).isEqualTo(HELLO_ACK + "0000000c010100000000000200000000");
    }

    @Test
    void requestAfterCloseIsFailedLocallyWithoutAFrame() {
        Endpoint server = established();
        server.close(Frames.NO_ERROR);
        server.drainOutput();
        server.drainEvents();

        server.request(257, new byte[0]);

        assertOpenAndSilent(server);
    }

    @Test
    void pingSendsTheEightBytes() {
        Endpoint server = established();

        server.ping(bytes("0102030405060708"));

        assertThat(sent(server)).isEqualTo("00000014000200000000000000000000" + "0102030405060708");
    }

    @Test
    void pingBeforeAckingThePeerHelloWaitsForTheAck() {
        Endpoint server = serverAfterPreface();

        server.ping(bytes("0102030405060708"));
        assertOpenAndSilent(server);
        server.receive(bytes(HELLO));

        assertThat(sent(server)).isEqualTo(HELLO_ACK + "00000014000200000000000000000000" + "0102030405060708");
    }

    @Test
    void closeSendsCloseAndClosesWhenNothingIsPending() {
        Endpoint server = serverAfterPreface();

        server.close(Frames.NO_ERROR);

        assertThat(sent(server)).isEqualTo(CLOSE_NO_ERROR);
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void closeDoesNotCloseWhileRequestsArePending() {
        Endpoint server = established();
        server.request(257, new byte[0]);
        server.drainOutput();

        server.close(Frames.NO_ERROR);

        assertThat(sent(server)).isEqualTo(CLOSE_NO_ERROR);
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void errorCloseClosesAtOnceAndNoSecondCloseIsSent() {
        Endpoint server = established();

        server.close(Frames.PROTOCOL_ERROR);
        server.close(Frames.NO_ERROR);

        assertThat(sent(server)).isEqualTo(CLOSE_PROTOCOL_ERROR);
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void closeAfterCloseWasReceivedSendsNothing() {
        Endpoint server = established();
        server.request(257, new byte[0]);
        server.receive(bytes(CLOSE_NO_ERROR));
        server.drainOutput();
        server.drainEvents();

        server.close(Frames.NO_ERROR);

        assertOpenAndSilent(server);
    }

    // ---- 클라이언트 ----

    @Test
    void clientSendsThePrefaceAndItsHelloAtOnce() {
        Endpoint client = client();

        assertThat(sent(client)).isEqualTo(PREFACE + "0000001e000100000000000000000000" + DEFAULT_HELLO_PARAMS);
        assertThat(client.drainEvents()).isEmpty();
    }

    @Test
    void clientAcknowledgesAValidServerHello() {
        Endpoint client = client();
        client.drainOutput();

        client.receive(bytes(helloWith("000200000064")));

        assertThat(sent(client)).isEqualTo(HELLO_ACK);
    }

    @Test
    void clientFirstFrameChecksCloseSilently() {
        for (String frame : new String[] {
            "0000001000030000000000000000000000000001", HELLO_ACK, "00000012000100000000000100000000000200000064"
        }) {
            Endpoint client = client();
            client.drainOutput();

            client.receive(bytes(frame));

            assertClosedSilently(client);
        }
    }

    @Test
    void clientRejectsAServerHelloOutsideTheRangeWithProtocolError() {
        Endpoint client = client();
        client.drainOutput();

        client.receive(bytes(helloWith("000100003fff")));

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientFirstFrameHelloAboveItsOwnLimitClosesWithProtocolError() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().maxFrameSize(16384));
        client.drainOutput();

        client.receive(bytes("00003ffd 0001 00 00 00000000 0000 0000"));

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientPingBeforeTheServerAckClosesWithProtocolError() {
        Endpoint client = client();
        client.receive(bytes(HELLO));
        client.drainOutput();

        client.receive(bytes(PING));

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientWaitIsNotResetByTheServerHello() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().ackTimeoutMs(1000));
        client.drainOutput();
        client.advance(999);
        client.receive(bytes(HELLO));
        client.drainOutput();
        assertThat(client.drainEvents()).isEmpty();

        client.advance(2);

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientClosesWhenTheAckDoesNotComeInTime() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().ackTimeoutMs(1000));
        client.drainOutput();

        client.advance(999);
        assertOpenAndSilent(client);
        client.advance(2);

        assertClosedWithProtocolError(client);
    }

    @Test
    void closeIsSendableBeforeAckingThePeerHello() {
        Endpoint client = client();
        client.drainOutput();

        client.close(Frames.NO_ERROR);

        assertThat(sent(client)).isEqualTo(CLOSE_NO_ERROR);
    }

    // ---- PING의 확인 대기(3.3절, 3.4절) ----

    @Test
    void pingWithoutAnAckClosesWithProtocolErrorAfterTheWait() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.drainOutput();

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(2);

        assertClosedWithProtocolError(server);
    }

    @Test
    void clientPingWithoutAnAckClosesWithProtocolErrorAfterTheWait() {
        Endpoint client = Endpoint.client(ONE_SECOND);
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.ping(bytes(PING_PAYLOAD));
        client.drainOutput();

        client.advance(1001);

        assertClosedWithProtocolError(client);
    }

    @Test
    void anAckPingWithTheSamePayloadCancelsTheWait() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.advance(999);
        server.receive(bytes(PING_ACK));
        server.drainOutput();

        server.advance(5000);

        assertOpenAndSilent(server);
    }

    @Test
    void anAckPingWithAnotherPayloadIsIgnoredAndTheWaitContinues() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes("ffffffffffffffff"));
        server.drainOutput();

        server.receive(bytes(PING_ACK));
        assertOpenAndSilent(server);
        server.advance(1001);

        assertClosedWithProtocolError(server);
    }

    @Test
    void anUnsolicitedAckPingIsIgnored() {
        Endpoint server = establishedWith(ONE_SECOND);

        server.receive(bytes(PING_ACK));
        server.advance(5000);

        assertOpenAndSilent(server);
    }

    @Test
    void overlappingPingsAreWaitedOnSeparately() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.advance(600);
        server.ping(bytes("ffffffffffffffff"));
        server.receive(bytes(PING_ACK));
        server.drainOutput();

        server.advance(500);
        assertOpenAndSilent(server);
        server.advance(600);

        assertClosedWithProtocolError(server);
    }

    @Test
    void oneAckEndsOnlyOneOfTwoPingsWithTheSamePayload() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.advance(500);
        server.ping(bytes(PING_PAYLOAD));
        server.receive(bytes(PING_ACK));
        server.drainOutput();

        server.advance(501);
        assertOpenAndSilent(server);
        server.advance(500);

        assertClosedWithProtocolError(server);
    }

    @Test
    void pingWaitExpiringWhileClosingClosesWithoutAClosePendingRequestsFail() {
        Endpoint client = Endpoint.client(ONE_SECOND);
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.request(257, new byte[0]);
        client.ping(bytes(PING_PAYLOAD));
        client.receive(bytes(CLOSE_NO_ERROR));
        client.drainOutput();
        assertThat(client.drainEvents()).isEmpty();

        client.advance(1001);

        assertThat(client.drainOutput()).isEmpty();
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void helloAndPingWaitsAreIndependent() {
        Endpoint server = serverAfterPreface(ONE_SECOND);
        server.receive(bytes(HELLO));
        server.advance(500);
        server.ping(bytes(PING_PAYLOAD));
        server.receive(bytes(HELLO_ACK));
        server.drainOutput();

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(2);

        assertClosedWithProtocolError(server);
    }

    @Test
    void firstFrameOfAPeerStreamIsReportedAsAReceivedRequestOnce() {
        Endpoint server = established();

        server.receive(bytes("0000000c010100000000000100000000"));
        server.receive(bytes("0000000c010100000000000100000000"));

        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.REQUEST_RECEIVED);
        assertThat(events.get(0).streamId()).isEqualTo(1);
        assertThat(events.get(0).type()).isEqualTo(257);
    }

    @Test
    void resetWithAnyFlagsFreesThePeerStreamSoItCanBeOpenedAgain() {
        Endpoint server = established();
        server.receive(bytes("0000000c010100000000000100000000"));
        server.drainEvents();

        server.receive(bytes("000000100004ff00000000010000000000000004"));
        assertThat(server.drainEvents()).isEmpty();
        server.receive(bytes("0000000c010100000000000100000000"));

        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }
}
