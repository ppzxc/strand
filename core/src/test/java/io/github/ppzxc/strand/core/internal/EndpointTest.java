package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** 엔드포인트의 외부 동작만 본다: 받은 바이트와 애플리케이션 호출에 대해 보낸 바이트와 애플리케이션 이벤트. */
class EndpointTest {

    private static final String PREFACE = "c0de535452300000";
    private static final String HELLO = "0000000c000100000000000000000000";
    /** extensions_length의 한도(규격 2.4절). */
    private static final int MAX_EXTENSIONS = 8192;

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
        return hex(concat(endpoint.drainMessages()));
    }

    /** 보낸 메시지를 이어 붙인 바이트. 바이트 스트림 전송이 보내는 바이트와 같다. */
    private static byte[] concat(List<OutboundMessage> messages) {
        java.io.ByteArrayOutputStream joined = new java.io.ByteArrayOutputStream();
        for (OutboundMessage message : messages) {
            assertThat(message.kind()).isEqualTo(OutboundMessage.Kind.BINARY);
            joined.write(message.bytes(), 0, message.bytes().length);
        }
        return joined.toByteArray();
    }

    private static Endpoint server() {
        return Endpoint.server(EndpointConfig.defaults());
    }

    private static Endpoint serverAfterPreface(EndpointConfig config) {
        Endpoint server = Endpoint.server(config);
        server.receive(bytes(PREFACE));
        server.drainMessages();
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
        server.drainMessages();
        assertThat(server.drainEvents()).isEmpty();
        return server;
    }

    private static Endpoint establishedWith(EndpointConfig config) {
        Endpoint server = serverAfterPreface(config);
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainMessages();
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
        client.drainMessages();
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
        assertThat(endpoint.drainMessages()).isEmpty();
        assertThat(kinds(endpoint)).containsExactly(Event.Kind.CLOSED);
    }

    private static void assertOpenAndSilent(Endpoint endpoint) {
        assertThat(endpoint.drainMessages()).isEmpty();
        assertThat(endpoint.drainEvents()).isEmpty();
    }

    /** 로컬에서 실패한 요청: 프레임 없이 stream id 없는 RESULT(UNAVAILABLE)가 정확히 한 번 나온다(규격 2.2.5절, 3.2.1절, 3.5절). */
    private static void assertFailedLocally(Endpoint endpoint) {
        assertThat(endpoint.drainMessages()).isEmpty();
        assertLocalUnavailableEvents(endpoint.drainEvents());
    }

    private static void assertLocalUnavailableEvents(List<Event> events) {
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(events.get(0).status()).isEqualTo(Event.UNAVAILABLE);
        assertThat(events.get(0).streamId()).isZero();
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
        server.drainMessages();

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
            assertThat(server.drainMessages()).isEmpty();
            server.receive(new byte[] {b});
        }

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
    }

    @Test
    void drainReturnsEachOutputOnce() {
        Endpoint server = server();
        server.receive(bytes(PREFACE));

        assertThat(server.drainMessages()).isNotEmpty();
        assertThat(server.drainMessages()).isEmpty();
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
        server.drainMessages();

        server.receive(bytes("00000012000101000000000000000000000200000064"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void ackHelloWithANonzeroStreamIdClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainMessages();

        server.receive(bytes("0000000c000101000000000100000000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void aSecondParameterHelloClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainMessages();

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
        server.drainMessages();

        // 본문이 다 오기 전에 length와 type까지만으로 판정한다.
        server.receive(bytes("00003ffd0002"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void frameLengthAtTheAnnouncedLimitIsAccepted() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().maxFrameSize(16384));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainMessages();

        server.receive(bytes("00003ffc"));

        assertOpenAndSilent(server);
    }

    @Test
    void frameLengthBelowTheMinimumClosesWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("0000000b0002"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void nothingIsHandledOnceClosed() {
        Endpoint server = established();
        server.receive(bytes("0000000b0002"));
        server.drainMessages();
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
            server.drainMessages();

            server.receive(bytes(frame));

            assertClosedWithProtocolError(server);
        }
    }

    @Test
    void reservedControlTypesAreIgnoredBeforeThePeerAck() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainMessages();

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
        noError.drainMessages();
        Endpoint error = serverAfterPreface();
        error.receive(bytes(HELLO));
        error.drainMessages();

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
        server.drainMessages();

        server.receive(bytes(CLOSE_PROTOCOL_ERROR));

        assertThat(server.drainMessages()).isEmpty();
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
        server.drainMessages();
        server.request(257, new byte[0]);
        server.drainMessages();
        server.receive(bytes(CLOSE_NO_ERROR));
        assertOpenAndSilent(server);

        // 상대 ACK 전의 PING은 연결 오류지만 CLOSE를 이미 받았으므로 CLOSE를 보내지 않는다.
        server.receive(bytes(PING));

        assertThat(server.drainMessages()).isEmpty();
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
        server.drainMessages();

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
        server.drainMessages();

        server.advance(10000);

        assertOpenAndSilent(server);
    }

    @Test
    void theWaitStartsWhenTheHelloIsSent() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.advance(5000);
        server.receive(bytes(PREFACE));
        server.drainMessages();

        server.advance(999);

        assertOpenAndSilent(server);
    }

    @Test
    void noClockEffectOnceClosed() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.receive(bytes(CLOSE_NO_ERROR));
        server.drainMessages();
        server.drainEvents();

        server.advance(5000);

        assertOpenAndSilent(server);
    }

    @Test
    void theWaitIsDroppedWhenCloseWasReceivedEarlier() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().ackTimeoutMs(1000));
        server.receive(bytes(HELLO));
        server.drainMessages();
        server.request(257, new byte[0]);
        server.receive(bytes(CLOSE_NO_ERROR));
        server.drainMessages();
        server.drainEvents();

        server.advance(2000);

        // CLOSE를 이미 받았으므로 확인 대기 초과에도 CLOSE를 보내지 않고 닫는다.
        assertThat(server.drainMessages()).isEmpty();
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
        server.drainMessages();
        server.drainEvents();

        server.request(257, new byte[0]);

        assertFailedLocally(server);
    }

    @Test
    void requestAfterCloseWasReceivedWhileAStreamIsPendingIsFailedLocally() {
        Endpoint server = established();
        server.request(257, new byte[0]);
        server.receive(bytes(CLOSE_NO_ERROR));
        server.drainMessages();
        server.drainEvents();

        server.request(257, new byte[0]);

        assertFailedLocally(server);
    }

    @Test
    void requestAfterTheConnectionClosedIsFailedLocally() {
        Endpoint server = established();
        server.close(Frames.PROTOCOL_ERROR);
        server.drainMessages();
        server.drainEvents();

        server.request(257, new byte[0]);

        assertFailedLocally(server);
    }

    @Test
    void requestDeferredBeforeTheAckIsFailedLocallyWhenCloseArrivesFirst() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults());
        server.request(257, new byte[0]);
        server.drainMessages();
        server.drainEvents();

        server.receive(bytes(CLOSE_NO_ERROR));

        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(2);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(events.get(0).status()).isEqualTo(Event.UNAVAILABLE);
        assertThat(events.get(1).kind()).isEqualTo(Event.Kind.CLOSED);
    }

    @Test
    void deferredRequestsAfterNumberExhaustionAreFailedLocallyEachOnce() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(2147483647));
        client.drainMessages();
        client.request(257, new byte[0]);
        client.request(257, new byte[0]);
        client.request(257, new byte[0]);
        client.ping(bytes("0102030405060708"));

        client.receive(bytes(HELLO));

        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(2);
        for (Event event : events) {
            assertThat(event.kind()).isEqualTo(Event.Kind.RESULT);
            assertThat(event.status()).isEqualTo(Event.UNAVAILABLE);
            assertThat(event.streamId()).isZero();
        }
        assertThat(sent(client)).isEqualTo(HELLO_ACK + requestFrame(257, 2147483647, "") + CLOSE_NO_ERROR);
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
        server.drainMessages();

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
        server.drainMessages();
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
        client.drainMessages();

        client.receive(bytes(helloWith("000200000064")));

        assertThat(sent(client)).isEqualTo(HELLO_ACK);
    }

    @Test
    void clientFirstFrameChecksCloseSilently() {
        for (String frame : new String[] {
            "0000001000030000000000000000000000000001", HELLO_ACK, "00000012000100000000000100000000000200000064"
        }) {
            Endpoint client = client();
            client.drainMessages();

            client.receive(bytes(frame));

            assertClosedSilently(client);
        }
    }

    @Test
    void clientRejectsAServerHelloOutsideTheRangeWithProtocolError() {
        Endpoint client = client();
        client.drainMessages();

        client.receive(bytes(helloWith("000100003fff")));

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientFirstFrameHelloAboveItsOwnLimitClosesWithProtocolError() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().maxFrameSize(16384));
        client.drainMessages();

        client.receive(bytes("00003ffd 0001 00 00 00000000 0000 0000"));

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientPingBeforeTheServerAckClosesWithProtocolError() {
        Endpoint client = client();
        client.receive(bytes(HELLO));
        client.drainMessages();

        client.receive(bytes(PING));

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientWaitIsNotResetByTheServerHello() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().ackTimeoutMs(1000));
        client.drainMessages();
        client.advance(999);
        client.receive(bytes(HELLO));
        client.drainMessages();
        assertThat(client.drainEvents()).isEmpty();

        client.advance(2);

        assertClosedWithProtocolError(client);
    }

    @Test
    void clientClosesWhenTheAckDoesNotComeInTime() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().ackTimeoutMs(1000));
        client.drainMessages();

        client.advance(999);
        assertOpenAndSilent(client);
        client.advance(2);

        assertClosedWithProtocolError(client);
    }

    @Test
    void closeIsSendableBeforeAckingThePeerHello() {
        Endpoint client = client();
        client.drainMessages();

        client.close(Frames.NO_ERROR);

        assertThat(sent(client)).isEqualTo(CLOSE_NO_ERROR);
    }

    // ---- PING의 확인 대기(3.3절, 3.4절) ----

    @Test
    void pingWithoutAnAckClosesWithProtocolErrorAfterTheWait() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.drainMessages();

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
        client.drainMessages();

        client.advance(1001);

        assertClosedWithProtocolError(client);
    }

    @Test
    void anAckPingWithTheSamePayloadCancelsTheWait() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.advance(999);
        server.receive(bytes(PING_ACK));
        server.drainMessages();

        server.advance(5000);

        assertOpenAndSilent(server);
    }

    @Test
    void anAckPingWithAnotherPayloadIsIgnoredAndTheWaitContinues() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes("ffffffffffffffff"));
        server.drainMessages();

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
        server.drainMessages();

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
        server.drainMessages();

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
        client.drainMessages();
        assertThat(client.drainEvents()).isEmpty();

        client.advance(1001);

        assertThat(client.drainMessages()).isEmpty();
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void helloAndPingWaitsAreIndependent() {
        Endpoint server = serverAfterPreface(ONE_SECOND);
        server.receive(bytes(HELLO));
        server.advance(500);
        server.ping(bytes(PING_PAYLOAD));
        server.receive(bytes(HELLO_ACK));
        server.drainMessages();

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(2);

        assertClosedWithProtocolError(server);
    }

    @Test
    void firstFrameOfAPeerStreamIsReportedAsAReceivedRequest() {
        Endpoint server = established();

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
        server.receive(bytes("0000000c010100000000000300000000"));

        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void resetWithStreamIdZeroOrTopBitSetClosesWithProtocolError() {
        for (String streamId : new String[] {"00000000", "80000001"}) {
            Endpoint server = established();

            server.receive(bytes("00000010000400" + "00" + streamId + "0000" + "0000" + "00000004"));

            assertClosedWithProtocolError(server);
        }
    }

    /** 확장 영역이 {@code extHex}이고 payload가 없는 type 0x0101 프레임 hex. */
    private static String appFrame(int streamId, int status, String extHex) {
        return String.format(
                "%08x0101%02x%02x%08x%04x0000%s",
                12 + extHex.length() / 2, 0, status, streamId, extHex.length() / 2, extHex);
    }

    private static String unknownTlv(int valueLength) {
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < valueLength; i++) {
            value.append("00");
        }
        return String.format("7777%04x%s", valueLength, value);
    }

    @Test
    void clientDeliversTheResponseStatusAsAResult() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.drainMessages();

        client.receive(bytes(appFrame(1, 14, "")));

        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(events.get(0).streamId()).isEqualTo(1);
        assertThat(events.get(0).status()).isEqualTo(14);
    }

    @Test
    void reservedResponseStatusIsDeliveredAsUnknown() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);

        client.receive(bytes(appFrame(1, 200, "")));

        assertThat(client.drainEvents().get(0).status()).isEqualTo(2);
    }

    @Test
    void responseExtensionsAreOnlyFormatCheckedAndTheirContentIsIgnored() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);

        // timeout의 길이가 4가 아니어도 응답에서는 값을 해석하지 않는다.
        client.receive(bytes(appFrame(1, 0, "00010003000001")));

        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT);
    }

    @Test
    void responseWithMalformedExtensionsClosesAndFailsThePendingRequest() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.drainMessages();

        client.receive(bytes(appFrame(1, 0, "00090001610009000162")));

        assertThat(sent(client)).isEqualTo(CLOSE_PROTOCOL_ERROR);
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void lastResponseWhileClosingCompletesTheClose() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.receive(bytes("00000010000300000000000000000000" + "00000000"));
        assertThat(client.drainEvents()).isEmpty();

        client.receive(bytes(appFrame(1, 0, "")));

        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void responseToAStreamThatIsNotPendingIsIgnored() {
        Endpoint client = establishedClient();

        client.receive(bytes(appFrame(1, 0, "")));

        assertOpenAndSilent(client);
    }

    @Test
    void requestWithUnknownExtensionTagIsDeliveredEvenWhenItFillsTheLimit() {
        for (int valueLength : new int[] {3, MAX_EXTENSIONS - 4}) {
            Endpoint server = established();

            server.receive(bytes(appFrame(1, 0, unknownTlv(valueLength))));

            assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
        }
    }

    @Test
    void malformedRequestExtensionsCloseWithProtocolError() {
        String[] malformed = {
            unknownTlv(MAX_EXTENSIONS - 3), // 한도 8192 초과
            "0004000000090008", // 항목이 자기 길이만큼 value를 갖지 못함
            "000900000000", // 항목 뒤에 2바이트가 남음
            "7777000161" + "7777000162", // 모르는 tag 중복
            "00010004000000010001000400000001", // 정의된 tag 중복
            "00" // 항목 헤더보다 짧음
        };
        for (String extensions : malformed) {
            Endpoint server = established();

            server.receive(bytes(appFrame(1, 0, extensions)));

            assertClosedWithProtocolError(server);
        }
    }

    /** 요청 프레임 hex: 확장 영역은 {@code extHex}, payload 없음. */
    private static String requestFrame(int type, int streamId, String extHex) {
        return String.format(
                "%08x%04x0000%08x%04x0000%s", 12 + extHex.length() / 2, type, streamId, extHex.length() / 2, extHex);
    }

    private static final String RESET_REFUSED_1 = "000000100004000000000001" + "0000" + "0000" + "00000003";
    private static final String RESET_REFUSED_3 = "000000100004000000000003" + "0000" + "0000" + "00000003";
    private static final String RESET_REFUSED_5 = "000000100004000000000005" + "0000" + "0000" + "00000003";

    private static String keyBytes(int count) {
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < count; i++) {
            hex.append("6b");
        }
        return hex.toString();
    }

    private static Endpoint establishedKnowing(int... types) {
        Set<Integer> known = new HashSet<>();
        for (int type : types) {
            known.add(type);
        }
        return establishedWith(EndpointConfig.defaults().knownTypes(known));
    }

    @Test
    void requestOfAnUnknownTypeIsAnsweredUnimplementedWithTheSameType() {
        Endpoint server = establishedKnowing(257);

        server.receive(bytes(requestFrame(258, 1, "")));

        assertThat(sent(server)).isEqualTo("0000000c0102000c000000010000" + "0000");
        assertOpenAndSilent(server);
        server.receive(bytes(requestFrame(257, 3, "")));
        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void clientAlsoAnswersAnUnknownTypeRequestFromTheServer() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().knownTypes(Collections.singleton(257)));
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.drainMessages();

        client.receive(bytes(requestFrame(258, 2, "")));

        assertThat(sent(client)).isEqualTo("0000000c0102000c000000020000" + "0000");
    }

    @Test
    void invalidDefinedExtensionValuesAreAnsweredInvalidArgumentAndNotDelivered() {
        String[] invalid = {
            "00010003000001", // timeout 길이 3
            "000100050000000001", // timeout 길이 5
            "00030000", // idempotency-key 길이 0
            "00030041" + keyBytes(65) // idempotency-key 길이 65
        };
        for (String extensions : invalid) {
            Endpoint server = established();

            server.receive(bytes(requestFrame(257, 1, extensions)));

            assertThat(sent(server)).isEqualTo("0000000c0101000300000001" + "0000" + "0000");
            assertOpenAndSilent(server);
        }
    }

    @Test
    void definedExtensionValuesOnTheBoundariesAreDelivered() {
        String[] valid = {"000100040000" + "0001", "00030001" + "6b", "00030040" + keyBytes(64), "0002000161"};
        int streamId = 1;
        Endpoint server = established();
        for (String extensions : valid) {
            server.receive(bytes(requestFrame(257, streamId, extensions)));
            streamId += 2;
        }

        assertThat(kinds(server)).hasSize(valid.length);
    }

    @Test
    void unknownTypeIsCheckedBeforeExtensionValues() {
        Endpoint server = establishedKnowing(257);

        server.receive(bytes(requestFrame(258, 1, "00010003000001")));

        assertThat(sent(server)).isEqualTo("0000000c0102000c000000010000" + "0000");
    }

    @Test
    void requestOverTheConcurrentStreamLimitIsRefusedBeforeItsTypeOrExtensionValuesAreSeen() {
        Endpoint server = establishedWith(
                EndpointConfig.defaults().maxConcurrentStreams(1).knownTypes(Collections.singleton(257)));
        server.receive(bytes(requestFrame(257, 1, "")));
        server.drainEvents();

        server.receive(bytes(requestFrame(258, 3, "")));
        server.receive(bytes(requestFrame(257, 5, "00010003000001")));

        assertThat(sent(server)).isEqualTo(RESET_REFUSED_3 + RESET_REFUSED_5);
        assertOpenAndSilent(server);
    }

    @Test
    void everyRequestIsRefusedWhenTheLimitIsZero() {
        Endpoint server = establishedWith(EndpointConfig.defaults().maxConcurrentStreams(0));

        server.receive(bytes(requestFrame(257, 1, "")));

        assertThat(sent(server)).isEqualTo(RESET_REFUSED_1);
        assertOpenAndSilent(server);
    }

    @Test
    void requestAfterReceivingCloseIsRefusedWhileAPeerStreamKeepsTheConnectionOpen() {
        Endpoint server = established();
        server.receive(bytes(requestFrame(257, 1, "")));
        server.drainEvents();
        server.receive(bytes("00000010000300000000000000000000" + "00000000"));

        server.receive(bytes(requestFrame(258, 3, "00010003000001")));

        assertThat(sent(server)).isEqualTo(RESET_REFUSED_3);
        assertOpenAndSilent(server);
    }

    @Test
    void requestAfterSendingCloseIsRefused() {
        Endpoint server = established();
        server.receive(bytes(requestFrame(257, 1, "")));
        server.drainEvents();
        server.close(0);
        server.drainMessages();

        server.receive(bytes(requestFrame(257, 3, "")));

        assertThat(sent(server)).isEqualTo(RESET_REFUSED_3);
        assertOpenAndSilent(server);
    }

    @Test
    void connectionClosesWhenTheLastPeerStreamEndsWhileClosing() {
        Endpoint server = established();
        server.receive(bytes(requestFrame(257, 1, "")));
        server.drainEvents();
        server.receive(bytes("00000010000300000000000000000000" + "00000000"));
        assertThat(server.drainEvents()).isEmpty();

        server.receive(bytes("000000100004000000000001000000000000" + "0004"));

        assertThat(sent(server)).isEmpty();
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void streamNumberAtOrBelowTheHighestPeerNumberClosesWithProtocolError() {
        // 대기 중인 번호, 응답으로 끝난 번호, 건너뛴 번호, 거절한 번호는 모두 쓰인 것으로 센다(규격 4.2.1절).
        Endpoint pending = established();
        pending.receive(bytes(requestFrame(257, 1, "")));
        pending.drainEvents();
        pending.receive(bytes(requestFrame(257, 1, "")));
        assertClosedWithProtocolError(pending);

        Endpoint ended = established();
        ended.receive(bytes(requestFrame(257, 1, "")));
        ended.respond(1, 0, new byte[0]);
        ended.drainMessages();
        ended.drainEvents();
        ended.receive(bytes(requestFrame(257, 1, "")));
        assertClosedWithProtocolError(ended);

        Endpoint skipped = established();
        skipped.receive(bytes(requestFrame(257, 5, "")));
        skipped.drainEvents();
        skipped.receive(bytes(requestFrame(257, 3, "")));
        assertClosedWithProtocolError(skipped);

        Endpoint refused = establishedWith(EndpointConfig.defaults().maxConcurrentStreams(0));
        refused.receive(bytes(requestFrame(257, 5, "")));
        refused.drainMessages();
        refused.receive(bytes(requestFrame(257, 3, "")));
        assertClosedWithProtocolError(refused);
    }

    @Test
    void repeatedNumberWhileClosingClosesWithoutSendingClose() {
        Endpoint server = established();
        server.receive(bytes(requestFrame(257, 1, "")));
        server.receive(bytes("00000010000300000000000000000000" + "00000000"));
        server.receive(bytes(requestFrame(257, 3, "")));
        server.drainMessages();
        server.drainEvents();

        server.receive(bytes(requestFrame(257, 3, "")));

        assertClosedSilently(server);
    }

    @Test
    void applicationFrameWithStreamIdZeroOrTopBitSetClosesWithProtocolError() {
        for (int streamId : new int[] {0, 0x80000001}) {
            Endpoint server = established();

            server.receive(bytes(requestFrame(257, streamId, "")));

            assertClosedWithProtocolError(server);
        }
    }

    @Test
    void resetPayloadOtherThanFourBytesClosesWithProtocolError() {
        for (String payload : new String[] {"", "000000", "0000000000"}) {
            Endpoint server = established();

            server.receive(bytes(String.format("%08x00040000%08x00000000", 12 + payload.length() / 2, 1) + payload));

            assertClosedWithProtocolError(server);
        }
    }

    @Test
    void respondSendsTheResponseWithTheRequestTypeAndFreesTheSlot() {
        Endpoint server = establishedWith(EndpointConfig.defaults().maxConcurrentStreams(1));
        server.receive(bytes(requestFrame(258, 1, "")));
        server.drainEvents();

        server.respond(1, 0, new byte[0]);
        server.respond(1, 0, new byte[0]);

        assertThat(sent(server)).isEqualTo("0000000c0102000000000001" + "0000" + "0000");
        server.receive(bytes(requestFrame(257, 3, "")));
        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void lastRespondWhileClosingClosesTheConnection() {
        Endpoint server = established();
        server.receive(bytes(requestFrame(257, 1, "")));
        server.drainEvents();
        server.close(0);
        server.drainMessages();

        server.respond(1, 0, new byte[0]);

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void respondToAnUnknownStreamDoesNothing() {
        Endpoint server = established();

        server.respond(1, 0, new byte[0]);

        assertOpenAndSilent(server);
    }

    @Test
    void respondCarriesThePayloadInTheResponseFrame() {
        Endpoint server = established();
        server.receive(bytes(requestFrame(258, 1, "")));
        server.drainEvents();

        server.respond(1, 0, bytes("cafe01"));

        assertThat(sent(server)).isEqualTo("0000000f0102000000000001" + "0000" + "0000" + "cafe01");
    }

    private static Endpoint establishedServerWithPeerFrameSize(int maxFrameSize) {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(helloWith("0001" + String.format("%08x", maxFrameSize))));
        server.receive(bytes(HELLO_ACK));
        server.drainMessages();
        server.receive(bytes(requestFrame(257, 1, "")));
        server.drainEvents();
        return server;
    }

    @Test
    void respondPayloadUpToThePeerMaxFrameSizeIsSent() {
        Endpoint server = establishedServerWithPeerFrameSize(16384);

        server.respond(1, 0, new byte[16384 - 16]);

        assertThat(concat(server.drainMessages())).hasSize(16384);
    }

    @Test
    void respondPayloadOverThePeerMaxFrameSizeIsRejectedWithoutSendingOrFreeingTheStream() {
        Endpoint server = establishedServerWithPeerFrameSize(16384);

        assertThatThrownBy(() -> server.respond(1, 0, new byte[16384 - 16 + 1]))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(sent(server)).isEmpty();
        server.respond(1, 0, new byte[0]);
        assertThat(sent(server)).isEqualTo("0000000c0101000000000001" + "0000" + "0000");
    }

    @Test
    void cancelSendsResetCancelAndReportsCancelledOnce() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.drainMessages();

        client.cancel(1);
        client.cancel(1);

        assertThat(sent(client)).isEqualTo("000000100004000000000001" + "0000" + "0000" + "00000004");
        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).status()).isEqualTo(1);
        client.receive(bytes(appFrame(1, 0, "")));
        assertOpenAndSilent(client);
    }

    @Test
    void lastCancelWhileClosingClosesTheConnection() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.close(0);
        client.drainMessages();

        client.cancel(1);

        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void receivedResetEndsTheRequestWithAStatusFromItsErrorCode() {
        int[][] cases = {{4, 1}, {3, 14}, {0, 13}, {1, 13}, {2, 13}, {5, 13}, {6, 13}};
        for (int[] c : cases) {
            Endpoint client = establishedClient();
            client.request(257, new byte[0]);
            client.drainMessages();

            client.receive(bytes(String.format("000000100004000000000001000000000000%04x", c[0])));

            List<Event> events = client.drainEvents();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).status()).as("error_code %d", c[0]).isEqualTo(c[1]);
            assertThat(client.drainMessages()).isEmpty();
        }
    }

    @Test
    void resetOnAStreamThatIsNotPendingIsDropped() {
        Endpoint server = established();

        server.receive(bytes("000000100004000000000005" + "0000" + "0000" + "00000004"));
        server.receive(bytes("000000100004000000000004" + "0000" + "0000" + "00000004"));
        server.receive(bytes(requestFrame(257, 3, "")));

        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void responseWithADifferentTypeEndsTheRequestWithInternalAndSendsNothing() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.drainMessages();

        client.receive(bytes(requestFrame(258, 1, "")));

        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).status()).isEqualTo(13);
        assertThat(client.drainMessages()).isEmpty();
    }

    @Test
    void streamNumbersAreExhaustedAfterTheLastNumberAndTheConnectionCloses() {
        int[][] roles = {{1, 2147483647}, {0, 2147483646}};
        for (int[] role : roles) {
            EndpointConfig config = EndpointConfig.defaults().nextStreamId(role[1]);
            Endpoint endpoint = role[0] == 1 ? Endpoint.client(config) : serverAfterPreface(config);
            endpoint.receive(bytes(HELLO));
            endpoint.receive(bytes(HELLO_ACK));
            endpoint.drainMessages();
            endpoint.request(257, new byte[0]);
            endpoint.drainMessages();
            endpoint.receive(bytes(appFrame(role[1], 0, "")));
            endpoint.drainEvents();

            endpoint.request(257, new byte[0]);

            assertThat(sent(endpoint)).isEqualTo("00000010000300000000000000000000" + "00000000");
            assertThat(kinds(endpoint)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        }
    }

    @Test
    void exhaustionWithAPendingRequestWaitsForItAndSendsNoSecondClose() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(2147483647));
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.request(257, new byte[0]);
        client.close(0);
        client.drainMessages();

        client.request(257, new byte[0]);

        assertFailedLocally(client);
        client.receive(bytes(appFrame(2147483647, 0, "")));
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void exhaustionTakesPrecedenceOverReachingThePeerConcurrentStreamLimit() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(2147483647));
        client.receive(bytes(helloWith("0002" + String.format("%08x", 1L))));
        client.receive(bytes(HELLO_ACK));
        client.request(257, new byte[0]);
        client.drainMessages();
        client.drainEvents();

        client.request(257, new byte[0]);

        assertThat(sent(client)).isEqualTo("00000010000300000000000000000000" + "00000000");
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT);
    }

    @Test
    void largestStreamNumberIsAccepted() {
        Endpoint server = established();

        server.receive(bytes(requestFrame(257, 2147483647, "")));

        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    private static Endpoint establishedClientWithPeerLimit(long maxConcurrentStreams) {
        Endpoint client = client();
        client.receive(bytes(helloWith("0002" + String.format("%08x", maxConcurrentStreams))));
        client.receive(bytes(HELLO_ACK));
        client.drainMessages();
        return client;
    }

    @Test
    void requestAtThePeerConcurrentStreamLimitIsNotSent() {
        Endpoint client = establishedClientWithPeerLimit(1);
        client.request(257, new byte[0]);
        client.drainMessages();

        client.request(257, new byte[0]);

        assertFailedLocally(client);
    }

    @Test
    void requestBelowThePeerConcurrentStreamLimitIsSent() {
        Endpoint client = establishedClientWithPeerLimit(2);
        client.request(257, new byte[0]);
        client.drainMessages();

        client.request(257, new byte[0]);

        assertThat(sent(client)).isEqualTo(requestFrame(257, 3, ""));
    }

    @Test
    void finishedStreamFreesRoomUnderThePeerLimitAndTheFailedRequestKeepsNoNumber() {
        Endpoint client = establishedClientWithPeerLimit(1);
        client.request(257, new byte[0]);
        client.request(257, new byte[0]);
        client.drainMessages();
        assertLocalUnavailableEvents(client.drainEvents());
        client.receive(bytes(appFrame(1, 0, "")));
        client.drainEvents();

        client.request(257, new byte[0]);

        assertThat(sent(client)).isEqualTo(requestFrame(257, 3, ""));
    }

    @Test
    void peerLimitOfZeroSendsNoRequest() {
        Endpoint client = establishedClientWithPeerLimit(0);

        client.request(257, new byte[0]);

        assertFailedLocally(client);
    }

    @Test
    void closeFrameWithLengthBelowMinimumClosesSilently() {
        Endpoint server = established();

        server.receive(bytes("0000000b0003000000000000000000"));

        assertThat(sent(server)).isEmpty();
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void closeFrameWithLengthAboveMaxFrameSizeClosesSilently() {
        Endpoint server = established();

        server.receive(bytes("00400001" + "0003000000000000" + "00000000"));

        assertThat(sent(server)).isEmpty();
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void nonCloseFrameWithLengthBelowMinimumStillGetsProtocolError() {
        Endpoint server = established();

        server.receive(bytes("0000000b0004000000000000000000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void disconnectFailsPendingRequestsAndClosesOnce() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.drainMessages();

        client.disconnect();
        client.disconnect();

        assertThat(client.drainMessages()).isEmpty();
        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(2);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(events.get(0).status()).isEqualTo(Event.UNAVAILABLE);
        assertThat(events.get(1).kind()).isEqualTo(Event.Kind.CLOSED);
    }

    @Test
    void disconnectAfterClosedEmitsNothing() {
        Endpoint client = establishedClient();
        client.close(1);
        client.drainMessages();
        client.drainEvents();

        client.disconnect();

        assertOpenAndSilent(client);
    }

    @Test
    void closeFrameWithBadLengthIsSilentEvenWhenTheTypeArrivesLater() {
        Endpoint server = established();

        server.receive(bytes("0000000b"));
        assertOpenAndSilent(server);
        server.receive(bytes("0003"));

        assertThat(sent(server)).isEmpty();
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    private static final String PING_MESSAGE = "000000140002000000000000000000000102030405060708";

    private static Endpoint webSocketServer() {
        return Endpoint.server(EndpointConfig.defaults().webSocket());
    }

    private static Endpoint webSocketServerAfterPreface() {
        Endpoint server = webSocketServer();
        server.receiveMessage(MessageKind.BINARY, bytes(PREFACE));
        server.drainMessages();
        return server;
    }

    private static Endpoint establishedWebSocket() {
        return establishedWebSocket(EndpointConfig.defaults());
    }

    private static Endpoint establishedWebSocket(EndpointConfig config) {
        Endpoint server = Endpoint.server(config.webSocket());
        server.receiveMessage(MessageKind.BINARY, bytes(PREFACE));
        server.receiveMessage(MessageKind.BINARY, bytes(HELLO));
        server.receiveMessage(MessageKind.BINARY, bytes(HELLO_ACK));
        server.drainMessages();
        return server;
    }

    @Test
    void drainMessagesReturnsEachSentMessageOnceInTheOrderSent() {
        Endpoint client = client();

        List<OutboundMessage> messages = client.drainMessages();

        assertThat(messages).hasSize(2);
        assertThat(hex(messages.get(0).bytes())).isEqualTo(PREFACE);
        assertThat(hex(messages.get(1).bytes()))
                .isEqualTo("0000001e" + "000100000000000000000000" + DEFAULT_HELLO_PARAMS);
        assertThat(client.drainMessages()).isEmpty();
    }

    private static List<OutboundMessage.Kind> messageKinds(List<OutboundMessage> messages) {
        List<OutboundMessage.Kind> kinds = new ArrayList<>();
        for (OutboundMessage message : messages) {
            kinds.add(message.kind());
        }
        return kinds;
    }

    @Test
    void webSocketNoErrorCloseIsFollowedByWebSocketCloseNormalClosure() {
        Endpoint server = establishedWebSocket();

        server.close(Frames.NO_ERROR);

        List<OutboundMessage> messages = server.drainMessages();
        assertThat(messageKinds(messages))
                .containsExactly(OutboundMessage.Kind.BINARY, OutboundMessage.Kind.WEBSOCKET_CLOSE);
        assertThat(hex(messages.get(0).bytes())).isEqualTo(CLOSE_NO_ERROR);
        assertThat(messages.get(1).statusCode()).isEqualTo(1000);
        assertThat(messages.get(1).bytes()).isEmpty();
    }

    @Test
    void webSocketCloseWaitsForTheLastStreamWhenNoErrorCloseIsDraining() {
        Endpoint server = establishedWebSocket();
        server.receiveMessage(MessageKind.BINARY, bytes("0000000c010100000000000100000000"));
        server.drainEvents();
        server.close(Frames.NO_ERROR);
        assertThat(messageKinds(server.drainMessages())).containsExactly(OutboundMessage.Kind.BINARY);

        server.respond(1, 0, new byte[0]);

        assertThat(messageKinds(server.drainMessages()))
                .containsExactly(OutboundMessage.Kind.BINARY, OutboundMessage.Kind.WEBSOCKET_CLOSE);
    }

    @Test
    void byteStreamNoErrorCloseHasNoWebSocketClose() {
        Endpoint server = established();

        server.close(Frames.NO_ERROR);

        assertThat(messageKinds(server.drainMessages())).containsExactly(OutboundMessage.Kind.BINARY);
    }

    @Test
    void webSocketErrorCloseHasNoWebSocketCloseItem() {
        Endpoint server = establishedWebSocket();

        server.close(Frames.PROTOCOL_ERROR);

        assertThat(messageKinds(server.drainMessages())).containsExactly(OutboundMessage.Kind.BINARY);
    }

    @Test
    void webSocketMessageIsOneFrame() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(MessageKind.BINARY, bytes(PING_MESSAGE));

        assertThat(sent(server)).isEqualTo("000000140002010000000000000000000102030405060708");
    }

    @Test
    void webSocketPrefaceMessageGetsTheServerHello() {
        Endpoint server = webSocketServer();

        server.receiveMessage(MessageKind.BINARY, bytes(PREFACE));

        assertThat(sent(server)).startsWith("00000");
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void webSocketPrefaceOfTheWrongSizeOrOpcodeClosesSilently() {
        for (String hex : new String[] {"c0de5354523000", "c0de53545230000000"}) {
            Endpoint server = webSocketServer();
            server.receiveMessage(MessageKind.BINARY, bytes(hex));
            assertClosedSilently(server);
        }
        Endpoint text = webSocketServer();
        text.receiveMessage(MessageKind.TEXT, bytes(PREFACE));
        assertClosedSilently(text);
    }

    @Test
    void webSocketFirstFrameThatIsTextOrShortClosesSilently() {
        Endpoint text = webSocketServerAfterPreface();
        text.receiveMessage(MessageKind.TEXT, bytes(HELLO));
        assertClosedSilently(text);

        Endpoint shortFrame = webSocketServerAfterPreface();
        shortFrame.receiveMessage(MessageKind.BINARY, bytes("0000000c000100000000"));
        assertClosedSilently(shortFrame);
    }

    @Test
    void webSocketFirstFrameThatIsNotHelloClosesSilently() {
        Endpoint server = webSocketServerAfterPreface();

        server.receiveMessage(MessageKind.BINARY, bytes(PING_MESSAGE));

        assertClosedSilently(server);
    }

    @Test
    void webSocketFirstFrameSizeMismatchIsAProtocolError() {
        Endpoint server = webSocketServerAfterPreface();

        server.receiveMessage(MessageKind.BINARY, bytes(HELLO + "00"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void webSocketTextMessageAfterTheFirstFrameIsAProtocolError() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(MessageKind.TEXT, bytes(PING_MESSAGE));

        assertClosedWithProtocolError(server);
    }

    @Test
    void webSocketCloseCarriedInATextMessageIsAProtocolError() {
        Endpoint noError = establishedWebSocket();
        noError.receiveMessage(MessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertClosedWithProtocolError(noError);

        Endpoint protocolError = establishedWebSocket();
        protocolError.receiveMessage(MessageKind.TEXT, bytes(CLOSE_PROTOCOL_ERROR));
        assertClosedWithProtocolError(protocolError);
    }

    @Test
    void webSocketTextCloseSendsNoSecondCloseAfterOneWasSentOrReceived() {
        Endpoint sentClose = establishedWebSocket();
        sentClose.request(257, new byte[0]);
        sentClose.close(Frames.NO_ERROR);
        sentClose.drainMessages();
        sentClose.drainEvents();
        sentClose.receiveMessage(MessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertThat(sentClose.drainMessages()).isEmpty();
        assertThat(kinds(sentClose)).contains(Event.Kind.CLOSED);

        Endpoint receivedClose = establishedWebSocket();
        receivedClose.request(257, new byte[0]);
        receivedClose.receiveMessage(MessageKind.BINARY, bytes(CLOSE_NO_ERROR));
        receivedClose.drainMessages();
        receivedClose.drainEvents();
        receivedClose.receiveMessage(MessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertThat(receivedClose.drainMessages()).isEmpty();
        assertThat(kinds(receivedClose)).contains(Event.Kind.CLOSED);

        Endpoint closed = establishedWebSocket();
        closed.receiveMessage(MessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        closed.drainMessages();
        closed.drainEvents();
        closed.receiveMessage(MessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertOpenAndSilent(closed);
    }

    @Test
    void webSocketMessageOfTheWrongSizeIsAProtocolError() {
        Endpoint longer = establishedWebSocket();
        longer.receiveMessage(MessageKind.BINARY, bytes(PING_MESSAGE + "00"));
        assertClosedWithProtocolError(longer);

        Endpoint shorter = establishedWebSocket();
        shorter.receiveMessage(MessageKind.BINARY, bytes(PING_MESSAGE.substring(0, PING_MESSAGE.length() - 2)));
        assertClosedWithProtocolError(shorter);

        Endpoint tiny = establishedWebSocket();
        tiny.receiveMessage(MessageKind.BINARY, bytes("0000"));
        assertClosedWithProtocolError(tiny);
    }

    @Test
    void webSocketMessageAboveTheMaxFrameSizeIsAProtocolError() {
        Endpoint server = establishedWebSocket(EndpointConfig.defaults().maxFrameSize(16384));
        String payload = new String(new char[16384 - 12 - 4 + 1]).replace("\0", "00");

        server.receiveMessage(
                MessageKind.BINARY,
                bytes(String.format("%08x", 12 + payload.length() / 2) + "00020000" + "00000000" + "00000000"
                        + payload));

        assertClosedWithProtocolError(server);
    }

    @Test
    void webSocketCloseMessageWithTheWrongSizeClosesSilently() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(MessageKind.BINARY, bytes("000000100003000000000000000000000000000000"));

        assertClosedSilently(server);
    }

    @Test
    void webSocketClientFirstFrameThatIsTextClosesSilently() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().webSocket());
        client.drainMessages();

        client.receiveMessage(MessageKind.TEXT, bytes(HELLO));

        assertClosedSilently(client);
    }

    @Test
    void messagesAfterClosedAreIgnored() {
        Endpoint server = webSocketServer();
        server.receiveMessage(MessageKind.TEXT, bytes(PREFACE));
        server.drainEvents();

        server.receiveMessage(MessageKind.BINARY, bytes(PREFACE));

        assertOpenAndSilent(server);
    }

    @Test
    void webSocketEndpointRejectsByteInputAndStreamEndpointRejectsMessages() {
        assertThatThrownBy(() -> webSocketServer().receive(bytes(PREFACE))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> server().receiveMessage(MessageKind.BINARY, bytes(PREFACE)))
                .isInstanceOf(IllegalStateException.class);
    }
}
