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

    /** 규격 3.2.3절이 허용하는 {@code max_frame_size}의 최솟값(16 KiB). */
    private static final int MIN_MAX_FRAME_SIZE = 16384;
    /** 길이 필드(4B)와 헤더·확장 영역 길이를 포함한, payload 없는 프레임의 크기. */
    private static final int EMPTY_FRAME_SIZE = 20;

    private static final String PREFACE = "c0de535452300000";
    private static final String HELLO = "0000001000010000000000000000000000000000";
    /** extensions_length의 한도(규격 2.4절). */
    private static final int MAX_EXTENSIONS = 8192;

    private static final String HELLO_ACK = "0000001000010100000000000000000000000000";
    /** 메커니즘 {@code ANONYMOUS}에 빈 자격 증명인 {@code AUTH}(규격 3.3.1절). */
    private static final String AUTH_ANONYMOUS = "0000001a0005000000000000000000000000000009414e4f4e594d4f5553";

    private static final String AUTH_ACK = "0000001000050100000000000000000000000000";
    private static final String PING = "00000018 0002 00 00 0000000000000000 0000 0000 0102030405060708";
    private static final String PING_ACK = "0000001800020100000000000000000000000000" + "0102030405060708";
    /** CLOSE의 {@code last_stream_id}가 0인 경우(상대 요청이 아직 없다, 규격 3.5절). */
    private static final String NO_STREAM = "0000000000000000";

    private static final String CLOSE_PROTOCOL_ERROR =
            "0000001c00030000000000000000000000000000" + "00000001" + NO_STREAM;
    private static final String CLOSE_UNRESPONSIVE =
            "0000001c00030000000000000000000000000000" + "00000006" + NO_STREAM;
    private static final String CLOSE_NO_ERROR = "0000001c00030000000000000000000000000000" + "00000000" + NO_STREAM;
    private static final String PING_PAYLOAD = "0102030405060708";
    private static final EndpointConfig ONE_SECOND = EndpointConfig.defaults().ackTimeoutMs(1000);
    private static final EndpointConfig IDLE_ONE_SECOND =
            EndpointConfig.defaults().ackTimeoutMs(5000).idleIntervalMs(1000);
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
        return String.format("%08x", 16 + payloadHex.length() / 2) + "00010000" + "0000000000000000" + "00000000"
                + payloadHex;
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
        server.receive(bytes(AUTH_ANONYMOUS));
        server.drainMessages();
        assertThat(server.drainEvents()).isEmpty();
        return server;
    }

    private static Endpoint establishedWith(EndpointConfig config) {
        Endpoint server = serverAfterPreface(config);
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.receive(bytes(AUTH_ANONYMOUS));
        server.drainMessages();
        return server;
    }

    private static Endpoint client() {
        return Endpoint.client(EndpointConfig.defaults());
    }

    /** 클라이언트가 서버의 {@code AUTH(ACK)}를 받아 AUTH 관문을 통과한다. 알림 이벤트는 비운다. */
    private static void clientPassesAuthGate(Endpoint client) {
        client.receive(bytes(AUTH_ACK));
        assertThat(kinds(client)).containsExactly(Event.Kind.AUTHENTICATED);
    }

    /** 서버가 클라이언트의 {@code ANONYMOUS AUTH}를 통과시킨다. 보낸 {@code AUTH(ACK)}는 비운다. */
    private static void serverPassesAuthGate(Endpoint server) {
        server.receive(bytes(AUTH_ANONYMOUS));
        server.drainMessages();
    }

    /** 클라이언트: 서버 HELLO에 ACK를 보냈고 서버 ACK도 받은 상태. */
    private static Endpoint establishedClient() {
        Endpoint client = client();
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.receive(bytes(AUTH_ACK));
        client.drainMessages();
        assertThat(kinds(client)).containsExactly(Event.Kind.AUTHENTICATED);
        return client;
    }

    private static Endpoint establishedClientWithPeerFrameSize(int maxFrameSize) {
        Endpoint client = client();
        client.receive(bytes(helloWith("0001" + String.format("%08x", maxFrameSize))));
        client.receive(bytes(HELLO_ACK));
        client.receive(bytes(AUTH_ACK));
        client.drainMessages();
        client.drainEvents();
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

    /** 확인 대기 초과(규격 3.3절). */
    private static void assertClosedUnresponsive(Endpoint endpoint) {
        assertThat(sent(endpoint)).isEqualTo(CLOSE_UNRESPONSIVE);
        assertThat(kinds(endpoint)).containsExactly(Event.Kind.CLOSED);
    }

    /** 상대의 요청 번호가 이미 쓰인 뒤라 {@code last_stream_id}가 0이 아닌 CLOSE(PROTOCOL_ERROR)(규격 3.5절). */
    private static void assertClosedWithProtocolError(Endpoint endpoint, long lastStreamId) {
        assertThat(sent(endpoint))
                .isEqualTo(CLOSE_PROTOCOL_ERROR.substring(0, CLOSE_PROTOCOL_ERROR.length() - NO_STREAM.length())
                        + String.format("%016x", lastStreamId));
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
        assertThat(events.get(0).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(events.get(0).streamId()).isZero();
    }

    // ---- 서버: 연결 서문과 HELLO ----

    @Test
    void serverSendsItsHelloRightAfterThePreface() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().maxFrameSize(16384));

        server.receive(bytes(PREFACE));

        assertThat(sent(server))
                .isEqualTo("0000002200010000000000000000000000000000" + "0001000040000002000000640003" + "00000000");
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void prefaceReservedBytesAreIgnored() {
        Endpoint server = server();

        server.receive(bytes("c0de53545230ffff"));

        assertThat(sent(server)).isEqualTo("0000002200010000000000000000000000000000" + DEFAULT_HELLO_PARAMS);
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

        server.receive(bytes("0000001001010000000000000000000100000000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithAReservedControlTypeClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000001000050000000000000000000000000000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameWithANonzeroStreamIdClosesSilently() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("0000001000010000000000000000000100000000"));

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

        server.receive(bytes("0000001000018000000000000000000000000000"));

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

        server.receive(bytes("00000010000100ff00000000000000000000ffff"));

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
    }

    @Test
    void firstFrameHelloAboveTheOwnLimitClosesWithProtocolError() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().maxFrameSize(16384));

        // 헤더 20바이트만으로 판정한다. length = 16381 = 알린 한도 − 4 + 1.
        server.receive(bytes("00003ffd 0001 00 00 0000000000000000 0000 0000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void firstFrameOfTheWrongTypeAboveTheOwnLimitClosesSilently() {
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().maxFrameSize(16384));

        server.receive(bytes("00003ffd 0002 00 00 0000000000000000 0000 0000"));

        assertClosedSilently(server);
    }

    @Test
    void firstFrameHelloWithExtensionsClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();

        server.receive(bytes("000000140001000000000000000000000004000000090000"));

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

        server.receive(bytes("0000001600010100000000000000000000000000000200000064"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void ackHelloWithANonzeroStreamIdClosesWithProtocolError() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainMessages();

        server.receive(bytes("0000001000010100000000000000000100000000"));

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

        server.receive(bytes("0000000f0002"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void nothingIsHandledOnceClosed() {
        Endpoint server = established();
        server.receive(bytes("0000000f0002"));
        server.drainMessages();
        server.drainEvents();

        server.receive(bytes(PING));

        assertOpenAndSilent(server);
    }

    @Test
    void controlFramesWithExtensionsCloseWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("00000014 0002 00 00 0000000000000000 0004 0000 00000000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void extensionsLongerThanTheFrameCloseWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("0000001000050000000000000000000000050000"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void controlFramesWithANonzeroStreamIdCloseWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("0000001800020000000000000000000100000000" + "0102030405060708"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void pingPayloadMustBeExactlyEightBytesEvenForAnAck() {
        for (String frame : new String[] {
            "0000001000020000000000000000000000000000",
            "0000001000020100000000000000000000000000",
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
        for (String frame : new String[] {
            PING, "000000140004000000000000000000010000000000000004", "0000001001010000000000000000000100000000"
        }) {
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

        // 예약 범위는 0x0000과 0x0006~0x00FF다. 0x0005는 AUTH라서 예약이 아니다(규격 2.2.2절).
        for (String type : new String[] {"0000", "0006", "00ff"}) {
            server.receive(bytes("00000010" + type + "0000" + "0000000000000000" + "00000000"));
            assertOpenAndSilent(server);
        }
        server.receive(bytes(HELLO_ACK));
        serverPassesAuthGate(server);
        server.receive(bytes(PING));

        assertThat(sent(server)).isEqualTo(PING_ACK);
    }

    @Test
    void authBeforeThePeerAckClosesWithProtocolErrorBecauseItIsNotReserved() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.drainMessages();

        server.receive(bytes("00000010" + "0005" + "0000" + "0000000000000000" + "00000000"));

        assertClosedWithProtocolError(server);
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

        server.receive(bytes("00000013 0003 00 00 0000000000000000 0000 0000 000000".replace(" ", "")));

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
        assertThat(events.get(0).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(events.get(1).kind()).isEqualTo(Event.Kind.CLOSED);
    }

    @Test
    void connectionErrorAfterCloseWasReceivedSendsNoClose() {
        Endpoint server = established();
        server.request(257, new byte[0]);
        server.drainMessages();
        server.receive(bytes(CLOSE_NO_ERROR));
        assertOpenAndSilent(server);

        // payload가 8바이트가 아닌 PING은 연결 오류지만 CLOSE를 이미 받았으므로 CLOSE를 보내지 않는다.
        server.receive(bytes("00000010 0002 00 00 0000000000000000 0000 0000"));

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

        assertClosedUnresponsive(server);
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
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
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
                .isEqualTo("0000001201010000000000000000000200000000" + "0102"
                        + "0000001001020000000000000000000400000000");
    }

    @Test
    void clientRequestsStartAtOne() {
        Endpoint client = establishedClient();

        client.request(257, new byte[0]);

        assertThat(sent(client)).isEqualTo("0000001001010000000000000000000100000000");
    }

    @Test
    void requestBeforeTheAuthGateWaitsForIt() {
        Endpoint server = serverAfterPreface();

        server.request(257, new byte[0]);
        assertOpenAndSilent(server);
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
        server.receive(bytes(AUTH_ANONYMOUS));

        assertThat(sent(server)).isEqualTo(AUTH_ACK + "0000001001010000000000000000000200000000");
    }

    @Test
    void requestAfterCloseIsFailedLocallyWithoutAFrame() {
        Endpoint server = established();
        server.close(ErrorCode.NO_ERROR);
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
        server.close(ErrorCode.PROTOCOL_ERROR);
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
        assertThat(events.get(0).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(events.get(1).kind()).isEqualTo(Event.Kind.CLOSED);
    }

    @Test
    void deferredRequestsAfterNumberExhaustionAreFailedLocallyEachOnce() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(Long.MAX_VALUE));
        client.drainMessages();
        client.request(257, new byte[0]);
        client.request(257, new byte[0]);
        client.request(257, new byte[0]);
        client.ping(bytes("0102030405060708"));

        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        assertThat(client.drainEvents()).isEmpty();
        client.receive(bytes(AUTH_ACK));

        List<Event> events = client.drainEvents();
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.AUTHENTICATED);
        events = events.subList(1, events.size());
        assertThat(events).hasSize(2);
        for (Event event : events) {
            assertThat(event.kind()).isEqualTo(Event.Kind.RESULT);
            assertThat(event.status()).isEqualTo(Status.UNAVAILABLE);
            assertThat(event.streamId()).isZero();
        }
        assertThat(sent(client))
                .isEqualTo(HELLO_ACK + AUTH_ANONYMOUS + requestFrame(257, Long.MAX_VALUE, "") + CLOSE_NO_ERROR);
    }

    @Test
    void pingSendsTheEightBytes() {
        Endpoint server = established();

        server.ping(bytes("0102030405060708"));

        assertThat(sent(server)).isEqualTo("0000001800020000000000000000000000000000" + "0102030405060708");
    }

    @Test
    void pingBeforeTheAuthGateWaitsForIt() {
        Endpoint server = serverAfterPreface();

        server.ping(bytes("0102030405060708"));
        assertOpenAndSilent(server);
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));

        assertThat(sent(server)).isEqualTo(HELLO_ACK);
        server.receive(bytes(AUTH_ANONYMOUS));

        assertThat(sent(server)).isEqualTo(AUTH_ACK + "0000001800020000000000000000000000000000" + "0102030405060708");
    }

    @Test
    void closeSendsCloseAndClosesWhenNothingIsPending() {
        Endpoint server = serverAfterPreface();

        server.close(ErrorCode.NO_ERROR);

        assertThat(sent(server)).isEqualTo(CLOSE_NO_ERROR);
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void closeDoesNotCloseWhileRequestsArePending() {
        Endpoint server = established();
        server.request(257, new byte[0]);
        server.drainMessages();

        server.close(ErrorCode.NO_ERROR);

        assertThat(sent(server)).isEqualTo(CLOSE_NO_ERROR);
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void errorCloseClosesAtOnceAndNoSecondCloseIsSent() {
        Endpoint server = established();

        server.close(ErrorCode.PROTOCOL_ERROR);
        server.close(ErrorCode.NO_ERROR);

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

        server.close(ErrorCode.NO_ERROR);

        assertOpenAndSilent(server);
    }

    // ---- 클라이언트 ----

    @Test
    void clientSendsThePrefaceAndItsHelloAtOnce() {
        Endpoint client = client();

        assertThat(sent(client)).isEqualTo(PREFACE + "0000002200010000000000000000000000000000" + DEFAULT_HELLO_PARAMS);
        assertThat(client.drainEvents()).isEmpty();
    }

    @Test
    void clientAcknowledgesAValidServerHello() {
        Endpoint client = client();
        client.drainMessages();

        client.receive(bytes(helloWith("000200000064")));

        assertThat(sent(client)).isEqualTo(HELLO_ACK + AUTH_ANONYMOUS);
    }

    @Test
    void clientSendsTheConfiguredMechanismAndCredentialsInAuth() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().auth("PLAIN", bytes("0075736572007077")));
        client.drainMessages();

        client.receive(bytes(HELLO));

        assertThat(sent(client))
                .isEqualTo(HELLO_ACK + "0000001e00050000" + "0000000000000000" + "00000000" + "05" + "504c41494e"
                        + "0075736572007077");
    }

    @Test
    void clientReportsTheCredentialLifetimeAndIgnoresUnknownIdentifiers() {
        Endpoint client = client();
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.drainMessages();

        client.receive(bytes("0000001c0005010000000000000000000000000000010000012c00ff00000001"));

        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.AUTHENTICATED);
        assertThat(events.get(0).credentialLifetime()).hasValue(300);
        assertOpenAndSilent(client);
    }

    @Test
    void authAckWithoutLifetimeReportsNoLifetime() {
        Endpoint client = client();
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));

        client.receive(bytes(AUTH_ACK));

        assertThat(client.drainEvents().get(0).credentialLifetime()).isEmpty();
    }

    @Test
    void serverHandsTheAuthToTheApplicationAndAnswersAuthAckWhenItPasses() {
        List<String> seen = new ArrayList<>();
        Endpoint server = serverAfterPreface(EndpointConfig.defaults().authenticator((mechanism, credentials) -> {
            seen.add(mechanism + ":" + hex(credentials));
            return AuthDecision.PASS;
        }));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainMessages();

        server.receive(
                bytes("0000001e00050000" + "0000000000000000" + "00000000" + "05" + "504c41494e" + "0075736572007077"));

        assertThat(seen).containsExactly("PLAIN:0075736572007077");
        assertThat(sent(server)).isEqualTo(AUTH_ACK);
    }

    @Test
    void applicationFrameBeforeTheAuthGateIsAProtocolError() {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainMessages();

        server.receive(bytes(requestFrame(257, 1, "")));

        assertClosedWithProtocolError(server);
    }

    @Test
    void clientFirstFrameChecksCloseSilently() {
        for (String frame : new String[] {
            "0000001c0003000000000000000000000000000000000001" + NO_STREAM,
            HELLO_ACK,
            "0000001600010000000000000000000100000000000200000064"
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

        client.receive(bytes("00003ffd 0001 00 00 0000000000000000 0000 0000"));

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

        assertClosedUnresponsive(client);
    }

    @Test
    void clientClosesWhenTheAckDoesNotComeInTime() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().ackTimeoutMs(1000));
        client.drainMessages();

        client.advance(999);
        assertOpenAndSilent(client);
        client.advance(2);

        assertClosedUnresponsive(client);
    }

    @Test
    void closeIsSendableBeforeAckingThePeerHello() {
        Endpoint client = client();
        client.drainMessages();

        client.close(ErrorCode.NO_ERROR);

        assertThat(sent(client)).isEqualTo(CLOSE_NO_ERROR);
    }

    // ---- PING의 확인 대기(3.3절, 3.4절) ----

    @Test
    void pingWithoutAnAckClosesWithUnresponsiveAfterTheWait() {
        Endpoint server = establishedWith(ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.drainMessages();

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(2);

        assertClosedUnresponsive(server);
    }

    @Test
    void clientPingWithoutAnAckClosesWithUnresponsiveAfterTheWait() {
        Endpoint client = Endpoint.client(ONE_SECOND);
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.ping(bytes(PING_PAYLOAD));
        client.drainMessages();

        client.advance(1001);

        assertClosedUnresponsive(client);
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

        assertClosedUnresponsive(server);
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

        assertClosedUnresponsive(server);
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

        assertClosedUnresponsive(server);
    }

    @Test
    void pingWaitExpiringWhileClosingClosesWithoutAClosePendingRequestsFail() {
        Endpoint client = Endpoint.client(ONE_SECOND);
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        clientPassesAuthGate(client);
        client.request(257, new byte[0]);
        client.ping(bytes(PING_PAYLOAD));
        client.receive(bytes(CLOSE_NO_ERROR));
        client.drainMessages();
        assertThat(client.drainEvents()).isEmpty();

        client.advance(1001);

        assertThat(client.drainMessages()).isEmpty();
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    // ---- 유휴 시 PING(3.4.1절) ----

    @Test
    void withoutAnIdleIntervalNoIdlePingIsSent() {
        Endpoint server = established();

        server.advance(1_000_000);

        assertOpenAndSilent(server);
    }

    @Test
    void idlePingIsSentWhenNothingWasReceivedForTheIdleInterval() {
        Endpoint server = establishedWith(IDLE_ONE_SECOND);

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(1);

        assertThat(sent(server)).isEqualTo("0000001800020000000000000000000000000000" + "0000000000000000");
    }

    @Test
    void aReceivedFrameRestartsTheIdleInterval() {
        Endpoint server = establishedWith(IDLE_ONE_SECOND);
        server.advance(900);
        server.receive(bytes(PING));
        assertThat(sent(server)).isEqualTo(PING_ACK);

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(1);

        assertThat(sent(server)).hasSize(40 + 16);
    }

    @Test
    void noNewIdlePingIsSentWhileOneIsWaitingForItsAck() {
        Endpoint server = establishedWith(IDLE_ONE_SECOND);
        server.advance(1000);
        assertThat(sent(server)).hasSize(40 + 16);

        server.advance(3000);

        assertOpenAndSilent(server);
    }

    @Test
    void noIdlePingIsSentWhileAnApplicationPingIsWaitingForItsAck() {
        Endpoint server = establishedWith(IDLE_ONE_SECOND);
        server.ping(bytes(PING_PAYLOAD));
        server.drainMessages();

        server.advance(2000);

        assertOpenAndSilent(server);
    }

    @Test
    void anUnansweredIdlePingClosesWithUnresponsiveAfterTheAckWait() {
        Endpoint server = establishedWith(IDLE_ONE_SECOND);
        server.advance(1000);
        server.drainMessages();

        server.advance(4999);
        assertOpenAndSilent(server);
        server.advance(2);

        assertClosedUnresponsive(server);
    }

    @Test
    void anotherIdlePingIsSentAfterTheFirstOneIsAcked() {
        Endpoint server = establishedWith(IDLE_ONE_SECOND);
        server.advance(1000);
        server.drainMessages();
        server.receive(bytes("0000001800020100000000000000000000000000" + "0000000000000000"));
        server.drainMessages();

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(1);

        assertThat(sent(server)).isEqualTo("0000001800020000000000000000000000000000" + "0000000000000001");
    }

    @Test
    void idlePingWaitsForTheAuthGate() {
        Endpoint server = serverAfterPreface(IDLE_ONE_SECOND);
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.drainMessages();

        server.advance(1000);

        assertOpenAndSilent(server);
    }

    @Test
    void idlePingContinuesWhileClosing() {
        Endpoint server = establishedWith(IDLE_ONE_SECOND);
        server.request(257, new byte[0]);
        server.drainMessages();
        server.receive(bytes(CLOSE_NO_ERROR));
        server.drainMessages();

        server.advance(1000);

        assertThat(sent(server)).hasSize(40 + 16);
    }

    @Test
    void helloAndPingWaitsAreIndependent() {
        Endpoint server = serverAfterPreface(ONE_SECOND);
        server.receive(bytes(HELLO));
        server.advance(500);
        server.ping(bytes(PING_PAYLOAD));
        server.receive(bytes(HELLO_ACK));
        serverPassesAuthGate(server);
        server.drainMessages();

        server.advance(999);
        assertOpenAndSilent(server);
        server.advance(2);

        assertClosedUnresponsive(server);
    }

    @Test
    void firstFrameOfAPeerStreamIsReportedAsAReceivedRequest() {
        Endpoint server = established();

        server.receive(bytes("0000001001010000000000000000000100000000"));

        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.REQUEST_RECEIVED);
        assertThat(events.get(0).streamId()).isEqualTo(1);
        assertThat(events.get(0).type()).isEqualTo(257);
    }

    @Test
    void resetWithAnyFlagsFreesThePeerStreamSoItCanBeOpenedAgain() {
        Endpoint server = established();
        server.receive(bytes("0000001001010000000000000000000100000000"));
        server.drainEvents();

        server.receive(bytes("000000140004ff0000000000000000010000000000000004"));
        assertThat(server.drainEvents()).isEmpty();
        server.receive(bytes("0000001001010000000000000000000300000000"));

        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void resetWithStreamIdZeroOrTopBitSetClosesWithProtocolError() {
        for (String streamId : new String[] {"0000000000000000", "8000000000000001"}) {
            Endpoint server = established();

            server.receive(bytes("00000010000400" + "00" + streamId + "0000" + "0000" + "00000004"));

            assertClosedWithProtocolError(server);
        }
    }

    /** 확장 영역이 {@code extHex}이고 payload가 없는 type 0x0101 프레임 hex. */
    private static String appFrame(long streamId, int status, String extHex) {
        return String.format(
                "%08x0101%02x%02x%016x%04x0000%s",
                16 + extHex.length() / 2, 0, status, streamId, extHex.length() / 2, extHex);
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
        client.receive(bytes("0000001c00030000000000000000000000000000" + "00000000" + NO_STREAM));
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
    private static String requestFrame(int type, long streamId, String extHex) {
        return String.format(
                "%08x%04x0000%016x%04x0000%s", 16 + extHex.length() / 2, type, streamId, extHex.length() / 2, extHex);
    }

    private static final String RESET_REFUSED_1 = "00000014000400000000000000000001" + "0000" + "0000" + "00000003";
    private static final String RESET_REFUSED_3 = "00000014000400000000000000000003" + "0000" + "0000" + "00000003";
    private static final String RESET_REFUSED_5 = "00000014000400000000000000000005" + "0000" + "0000" + "00000003";

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

        assertThat(sent(server)).isEqualTo("000000100102000c00000000000000010000" + "0000");
        assertOpenAndSilent(server);
        server.receive(bytes(requestFrame(257, 3, "")));
        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void clientAlsoAnswersAnUnknownTypeRequestFromTheServer() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().knownTypes(Collections.singleton(257)));
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        clientPassesAuthGate(client);
        client.drainMessages();

        client.receive(bytes(requestFrame(258, 2, "")));

        assertThat(sent(client)).isEqualTo("000000100102000c00000000000000020000" + "0000");
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

            assertThat(sent(server)).isEqualTo("00000010010100030000000000000001" + "0000" + "0000");
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

        assertThat(sent(server)).isEqualTo("000000100102000c00000000000000010000" + "0000");
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
        server.receive(bytes("0000001c00030000000000000000000000000000" + "00000000" + NO_STREAM));

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
        server.receive(bytes("0000001c00030000000000000000000000000000" + "00000000" + NO_STREAM));
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
        assertClosedWithProtocolError(pending, 1);

        Endpoint ended = established();
        ended.receive(bytes(requestFrame(257, 1, "")));
        ended.respond(1, 0, new byte[0]);
        ended.drainMessages();
        ended.drainEvents();
        ended.receive(bytes(requestFrame(257, 1, "")));
        assertClosedWithProtocolError(ended, 1);

        Endpoint skipped = established();
        skipped.receive(bytes(requestFrame(257, 5, "")));
        skipped.drainEvents();
        skipped.receive(bytes(requestFrame(257, 3, "")));
        assertClosedWithProtocolError(skipped, 5);

        Endpoint refused = establishedWith(EndpointConfig.defaults().maxConcurrentStreams(0));
        refused.receive(bytes(requestFrame(257, 5, "")));
        refused.drainMessages();
        refused.receive(bytes(requestFrame(257, 3, "")));
        assertClosedWithProtocolError(refused, 5);
    }

    @Test
    void repeatedNumberWhileClosingClosesWithoutSendingClose() {
        Endpoint server = established();
        server.receive(bytes(requestFrame(257, 1, "")));
        server.receive(bytes("0000001c00030000000000000000000000000000" + "00000000" + NO_STREAM));
        server.receive(bytes(requestFrame(257, 3, "")));
        server.drainMessages();
        server.drainEvents();

        server.receive(bytes(requestFrame(257, 3, "")));

        assertClosedSilently(server);
    }

    @Test
    void applicationFrameWithStreamIdZeroOrTopBitSetClosesWithProtocolError() {
        for (long streamId : new long[] {0, 0x8000000000000001L}) {
            Endpoint server = established();

            server.receive(bytes(requestFrame(257, streamId, "")));

            assertClosedWithProtocolError(server);
        }
    }

    @Test
    void resetPayloadOtherThanFourBytesClosesWithProtocolError() {
        for (String payload : new String[] {"", "000000", "0000000000"}) {
            Endpoint server = established();

            server.receive(bytes(String.format("%08x00040000%016x00000000", 16 + payload.length() / 2, 1) + payload));

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

        assertThat(sent(server)).isEqualTo("00000010010200000000000000000001" + "0000" + "0000");
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

        assertThat(sent(server)).isEqualTo("00000013010200000000000000000001" + "0000" + "0000" + "cafe01");
    }

    private static Endpoint establishedServerWithPeerFrameSize(int maxFrameSize) {
        Endpoint server = serverAfterPreface();
        server.receive(bytes(helloWith("0001" + String.format("%08x", maxFrameSize))));
        server.receive(bytes(HELLO_ACK));
        serverPassesAuthGate(server);
        server.receive(bytes(requestFrame(257, 1, "")));
        server.drainEvents();
        return server;
    }

    @Test
    void respondPayloadUpToThePeerMaxFrameSizeIsSent() {
        Endpoint server = establishedServerWithPeerFrameSize(MIN_MAX_FRAME_SIZE);

        server.respond(1, 0, new byte[MIN_MAX_FRAME_SIZE - EMPTY_FRAME_SIZE]);

        assertThat(concat(server.drainMessages())).hasSize(MIN_MAX_FRAME_SIZE);
    }

    @Test
    void respondPayloadOverThePeerMaxFrameSizeIsRejectedWithoutSendingOrFreeingTheStream() {
        Endpoint server = establishedServerWithPeerFrameSize(MIN_MAX_FRAME_SIZE);

        assertThatThrownBy(() -> server.respond(1, 0, new byte[MIN_MAX_FRAME_SIZE - EMPTY_FRAME_SIZE + 1]))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(sent(server)).isEmpty();
        server.respond(1, 0, new byte[0]);
        assertThat(sent(server)).isEqualTo("00000010010100000000000000000001" + "0000" + "0000");
    }

    @Test
    void requestPayloadUpToThePeerMaxFrameSizeIsSent() {
        Endpoint client = establishedClientWithPeerFrameSize(MIN_MAX_FRAME_SIZE);

        client.request(257, new byte[MIN_MAX_FRAME_SIZE - EMPTY_FRAME_SIZE]);

        assertThat(concat(client.drainMessages())).hasSize(MIN_MAX_FRAME_SIZE);
    }

    @Test
    void requestPayloadOverThePeerMaxFrameSizeIsRejectedWithoutSendingOrUsingTheNumber() {
        Endpoint client = establishedClientWithPeerFrameSize(MIN_MAX_FRAME_SIZE);

        assertThatThrownBy(() -> client.request(257, new byte[MIN_MAX_FRAME_SIZE - EMPTY_FRAME_SIZE + 1]))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(sent(client)).isEmpty();
        assertThat(client.drainEvents()).isEmpty();
        client.request(257, new byte[0]);
        assertThat(sent(client)).isEqualTo("00000010010100000000000000000001" + "0000" + "0000");
    }

    @Test
    void requestDeferredBeforeTheAuthGateFailsLocallyWhenItsFrameIsOverThePeerMaxFrameSize() {
        Endpoint client = client();
        client.drainMessages();
        client.request(257, new byte[MIN_MAX_FRAME_SIZE - EMPTY_FRAME_SIZE + 1]);
        assertThat(client.drainEvents()).isEmpty();

        client.receive(bytes(helloWith("0001" + String.format("%08x", MIN_MAX_FRAME_SIZE))));

        assertThat(sent(client)).isEqualTo(HELLO_ACK + AUTH_ANONYMOUS);
        assertThat(client.drainEvents()).isEmpty();
        client.receive(bytes(HELLO_ACK));
        client.receive(bytes(AUTH_ACK));
        List<Event> events = client.drainEvents();
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.AUTHENTICATED);
        assertLocalUnavailableEvents(events.subList(1, events.size()));
        client.request(257, new byte[0]);
        assertThat(sent(client)).isEqualTo("00000010010100000000000000000001" + "0000" + "0000");
    }

    @Test
    void oversizeRequestAfterCloseFailsLocallyInsteadOfThrowing() {
        Endpoint client = establishedClientWithPeerFrameSize(MIN_MAX_FRAME_SIZE);
        client.close(ErrorCode.NO_ERROR);
        client.drainMessages();
        client.drainEvents();

        client.request(257, new byte[MIN_MAX_FRAME_SIZE]);

        assertFailedLocally(client);
    }

    @Test
    void cancelSendsResetCancelAndReportsCancelledOnce() {
        Endpoint client = establishedClient();
        client.request(257, new byte[0]);
        client.drainMessages();

        client.cancel(1);
        client.cancel(1);

        assertThat(sent(client)).isEqualTo("00000014000400000000000000000001" + "0000" + "0000" + "00000004");
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

            client.receive(bytes(String.format("00000014000400000000000000000001" + "00000000" + "0000%04x", c[0])));

            List<Event> events = client.drainEvents();
            assertThat(events).hasSize(1);
            assertThat(events.get(0).status()).as("error_code %d", c[0]).isEqualTo(c[1]);
            assertThat(client.drainMessages()).isEmpty();
        }
    }

    @Test
    void resetOnAStreamThatIsNotPendingIsDropped() {
        Endpoint server = established();

        server.receive(bytes("00000014000400000000000000000005" + "0000" + "0000" + "00000004"));
        server.receive(bytes("00000014000400000000000000000004" + "0000" + "0000" + "00000004"));
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
        long[][] roles = {{1, Long.MAX_VALUE}, {0, (Long.MAX_VALUE - 1)}};
        for (long[] role : roles) {
            EndpointConfig config = EndpointConfig.defaults().nextStreamId(role[1]);
            Endpoint endpoint = role[0] == 1 ? Endpoint.client(config) : serverAfterPreface(config);
            endpoint.receive(bytes(HELLO));
            endpoint.receive(bytes(HELLO_ACK));
            endpoint.receive(bytes(role[0] == 1 ? AUTH_ACK : AUTH_ANONYMOUS));
            endpoint.drainMessages();
            endpoint.drainEvents();
            endpoint.request(257, new byte[0]);
            endpoint.drainMessages();
            endpoint.receive(bytes(appFrame(role[1], 0, "")));
            endpoint.drainEvents();

            endpoint.request(257, new byte[0]);

            assertThat(sent(endpoint)).isEqualTo("0000001c00030000000000000000000000000000" + "00000000" + NO_STREAM);
            assertThat(kinds(endpoint)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        }
    }

    @Test
    void exhaustionWithAPendingRequestWaitsForItAndSendsNoSecondClose() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(Long.MAX_VALUE));
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        clientPassesAuthGate(client);
        client.request(257, new byte[0]);
        client.close(0);
        client.drainMessages();

        client.request(257, new byte[0]);

        assertFailedLocally(client);
        client.receive(bytes(appFrame(Long.MAX_VALUE, 0, "")));
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void exhaustionTakesPrecedenceOverReachingThePeerConcurrentStreamLimit() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(Long.MAX_VALUE));
        client.receive(bytes(helloWith("0002" + String.format("%08x", 1L))));
        client.receive(bytes(HELLO_ACK));
        clientPassesAuthGate(client);
        client.request(257, new byte[0]);
        client.drainMessages();
        client.drainEvents();

        client.request(257, new byte[0]);

        assertThat(sent(client)).isEqualTo("0000001c00030000000000000000000000000000" + "00000000" + NO_STREAM);
        assertThat(kinds(client)).containsExactly(Event.Kind.RESULT);
    }

    @Test
    void largestStreamNumberIsAccepted() {
        Endpoint server = established();

        server.receive(bytes(requestFrame(257, Long.MAX_VALUE, "")));

        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    private static Endpoint establishedClientWithPeerLimit(long maxConcurrentStreams) {
        Endpoint client = client();
        client.receive(bytes(helloWith("0002" + String.format("%08x", maxConcurrentStreams))));
        client.receive(bytes(HELLO_ACK));
        clientPassesAuthGate(client);
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

        server.receive(bytes("0000000f0003" + "00000000000000000000000000"));

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

        server.receive(bytes("0000000f0004" + "00000000000000000000000000"));

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
        assertThat(events.get(0).status()).isEqualTo(Status.UNAVAILABLE);
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

        server.receive(bytes("0000000f"));
        assertOpenAndSilent(server);
        server.receive(bytes("0003"));

        assertThat(sent(server)).isEmpty();
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    private static final String PING_MESSAGE = "00000018000200000000000000000000000000000102030405060708";

    private static Endpoint webSocketServer() {
        return Endpoint.server(EndpointConfig.defaults().webSocket());
    }

    private static Endpoint webSocketServerAfterPreface() {
        Endpoint server = webSocketServer();
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(PREFACE));
        server.drainMessages();
        return server;
    }

    private static Endpoint establishedWebSocket() {
        return establishedWebSocket(EndpointConfig.defaults());
    }

    private static Endpoint establishedWebSocket(EndpointConfig config) {
        Endpoint server = Endpoint.server(config.webSocket());
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(PREFACE));
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(HELLO));
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(HELLO_ACK));
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(AUTH_ANONYMOUS));
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
                .isEqualTo("00000022" + "00010000" + "0000000000000000" + "00000000" + DEFAULT_HELLO_PARAMS);
        assertThat(client.drainMessages()).isEmpty();
    }

    private static List<OutboundMessage.Kind> messageTypes(List<OutboundMessage> messages) {
        List<OutboundMessage.Kind> types = new ArrayList<>();
        for (OutboundMessage message : messages) {
            types.add(message.kind());
        }
        return types;
    }

    @Test
    void webSocketNoErrorCloseIsFollowedByWebSocketCloseNormalClosure() {
        Endpoint server = establishedWebSocket();

        server.close(ErrorCode.NO_ERROR);

        List<OutboundMessage> messages = server.drainMessages();
        assertThat(messageTypes(messages))
                .containsExactly(OutboundMessage.Kind.BINARY, OutboundMessage.Kind.WEBSOCKET_CLOSE);
        assertThat(hex(messages.get(0).bytes())).isEqualTo(CLOSE_NO_ERROR);
        assertThat(messages.get(1).webSocketCloseCode()).isEqualTo(1000);
        assertThat(messages.get(1).bytes()).isEmpty();
    }

    @Test
    void webSocketCloseWaitsForTheLastStreamWhenNoErrorCloseIsDraining() {
        Endpoint server = establishedWebSocket();
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes("0000001001010000000000000000000100000000"));
        server.drainEvents();
        server.close(ErrorCode.NO_ERROR);
        assertThat(messageTypes(server.drainMessages())).containsExactly(OutboundMessage.Kind.BINARY);

        server.respond(1, 0, new byte[0]);

        assertThat(messageTypes(server.drainMessages()))
                .containsExactly(OutboundMessage.Kind.BINARY, OutboundMessage.Kind.WEBSOCKET_CLOSE);
    }

    @Test
    void webSocketReceivedNoErrorCloseIsFollowedByWebSocketCloseNormalClosure() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(CLOSE_NO_ERROR));

        List<OutboundMessage> messages = server.drainMessages();
        assertThat(messageTypes(messages)).containsExactly(OutboundMessage.Kind.WEBSOCKET_CLOSE);
        assertThat(messages.get(0).webSocketCloseCode()).isEqualTo(1000);
    }

    @Test
    void webSocketCloseWaitsForTheLastStreamWhenReceivedNoErrorCloseIsDraining() {
        Endpoint server = establishedWebSocket();
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes("0000001001010000000000000000000100000000"));
        server.drainEvents();
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(CLOSE_NO_ERROR));
        assertThat(server.drainMessages()).isEmpty();

        server.respond(1, 0, new byte[0]);

        assertThat(messageTypes(server.drainMessages()))
                .containsExactly(OutboundMessage.Kind.BINARY, OutboundMessage.Kind.WEBSOCKET_CLOSE);
    }

    @Test
    void byteStreamReceivedNoErrorCloseHasNoWebSocketClose() {
        Endpoint server = established();

        server.receive(bytes(CLOSE_NO_ERROR));

        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void webSocketReceivedErrorCloseHasNoWebSocketCloseItem() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(CLOSE_PROTOCOL_ERROR));

        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void webSocketErrorCloseReceivedWhileDrainingHasNoWebSocketCloseItem() {
        Endpoint server = establishedWebSocket();
        server.receiveMessage(WebSocketMessageKind.BINARY, bytes("0000001001010000000000000000000100000000"));
        server.drainEvents();
        server.close(ErrorCode.NO_ERROR);
        server.drainMessages();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(CLOSE_PROTOCOL_ERROR));

        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void byteStreamNoErrorCloseHasNoWebSocketClose() {
        Endpoint server = established();

        server.close(ErrorCode.NO_ERROR);

        assertThat(messageTypes(server.drainMessages())).containsExactly(OutboundMessage.Kind.BINARY);
    }

    @Test
    void webSocketErrorCloseHasNoWebSocketCloseItem() {
        Endpoint server = establishedWebSocket();

        server.close(ErrorCode.PROTOCOL_ERROR);

        assertThat(messageTypes(server.drainMessages())).containsExactly(OutboundMessage.Kind.BINARY);
    }

    @Test
    void webSocketMessageIsOneFrame() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(PING_MESSAGE));

        assertThat(sent(server)).isEqualTo("00000018000201000000000000000000000000000102030405060708");
    }

    @Test
    void webSocketPrefaceMessageGetsTheServerHello() {
        Endpoint server = webSocketServer();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(PREFACE));

        assertThat(sent(server)).startsWith("00000");
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void webSocketPrefaceOfTheWrongSizeOrOpcodeClosesSilently() {
        for (String hex : new String[] {"c0de5354523000", "c0de53545230000000"}) {
            Endpoint server = webSocketServer();
            server.receiveMessage(WebSocketMessageKind.BINARY, bytes(hex));
            assertClosedSilently(server);
        }
        Endpoint text = webSocketServer();
        text.receiveMessage(WebSocketMessageKind.TEXT, bytes(PREFACE));
        assertClosedSilently(text);
    }

    @Test
    void webSocketFirstFrameThatIsTextOrShortClosesSilently() {
        Endpoint text = webSocketServerAfterPreface();
        text.receiveMessage(WebSocketMessageKind.TEXT, bytes(HELLO));
        assertClosedSilently(text);

        Endpoint shortFrame = webSocketServerAfterPreface();
        shortFrame.receiveMessage(WebSocketMessageKind.BINARY, bytes("0000000c000100000000"));
        assertClosedSilently(shortFrame);
    }

    @Test
    void webSocketFirstFrameThatIsNotHelloClosesSilently() {
        Endpoint server = webSocketServerAfterPreface();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(PING_MESSAGE));

        assertClosedSilently(server);
    }

    @Test
    void webSocketFirstFrameSizeMismatchIsAProtocolError() {
        Endpoint server = webSocketServerAfterPreface();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(HELLO + "00"));

        assertClosedWithProtocolError(server);
    }

    @Test
    void webSocketTextMessageAfterTheFirstFrameIsAProtocolError() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(WebSocketMessageKind.TEXT, bytes(PING_MESSAGE));

        assertClosedWithProtocolError(server);
    }

    @Test
    void webSocketCloseCarriedInATextMessageIsAProtocolError() {
        Endpoint noError = establishedWebSocket();
        noError.receiveMessage(WebSocketMessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertClosedWithProtocolError(noError);

        Endpoint protocolError = establishedWebSocket();
        protocolError.receiveMessage(WebSocketMessageKind.TEXT, bytes(CLOSE_PROTOCOL_ERROR));
        assertClosedWithProtocolError(protocolError);
    }

    @Test
    void webSocketTextCloseSendsNoSecondCloseAfterOneWasSentOrReceived() {
        Endpoint sentClose = establishedWebSocket();
        sentClose.request(257, new byte[0]);
        sentClose.close(ErrorCode.NO_ERROR);
        sentClose.drainMessages();
        sentClose.drainEvents();
        sentClose.receiveMessage(WebSocketMessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertThat(sentClose.drainMessages()).isEmpty();
        assertThat(kinds(sentClose)).contains(Event.Kind.CLOSED);

        Endpoint receivedClose = establishedWebSocket();
        receivedClose.request(257, new byte[0]);
        receivedClose.receiveMessage(WebSocketMessageKind.BINARY, bytes(CLOSE_NO_ERROR));
        receivedClose.drainMessages();
        receivedClose.drainEvents();
        receivedClose.receiveMessage(WebSocketMessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertThat(receivedClose.drainMessages()).isEmpty();
        assertThat(kinds(receivedClose)).contains(Event.Kind.CLOSED);

        Endpoint closed = establishedWebSocket();
        closed.receiveMessage(WebSocketMessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        closed.drainMessages();
        closed.drainEvents();
        closed.receiveMessage(WebSocketMessageKind.TEXT, bytes(CLOSE_NO_ERROR));
        assertOpenAndSilent(closed);
    }

    @Test
    void webSocketMessageOfTheWrongSizeIsAProtocolError() {
        Endpoint longer = establishedWebSocket();
        longer.receiveMessage(WebSocketMessageKind.BINARY, bytes(PING_MESSAGE + "00"));
        assertClosedWithProtocolError(longer);

        Endpoint shorter = establishedWebSocket();
        shorter.receiveMessage(
                WebSocketMessageKind.BINARY, bytes(PING_MESSAGE.substring(0, PING_MESSAGE.length() - 2)));
        assertClosedWithProtocolError(shorter);

        Endpoint tiny = establishedWebSocket();
        tiny.receiveMessage(WebSocketMessageKind.BINARY, bytes("0000"));
        assertClosedWithProtocolError(tiny);
    }

    @Test
    void webSocketMessageAboveTheMaxFrameSizeIsAProtocolError() {
        Endpoint server = establishedWebSocket(EndpointConfig.defaults().maxFrameSize(16384));
        String payload = new String(new char[16384 - 16 - 4 + 1]).replace("\0", "00");

        server.receiveMessage(
                WebSocketMessageKind.BINARY,
                bytes(String.format("%08x", 16 + payload.length() / 2) + "00020000" + "0000000000000000" + "00000000"
                        + payload));

        assertClosedWithProtocolError(server);
    }

    @Test
    void webSocketCloseMessageWithTheWrongSizeClosesSilently() {
        Endpoint server = establishedWebSocket();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes("000000100003000000000000000000000000000000"));

        assertClosedSilently(server);
    }

    @Test
    void webSocketClientFirstFrameThatIsTextClosesSilently() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().webSocket());
        client.drainMessages();

        client.receiveMessage(WebSocketMessageKind.TEXT, bytes(HELLO));

        assertClosedSilently(client);
    }

    @Test
    void messagesAfterClosedAreIgnored() {
        Endpoint server = webSocketServer();
        server.receiveMessage(WebSocketMessageKind.TEXT, bytes(PREFACE));
        server.drainEvents();

        server.receiveMessage(WebSocketMessageKind.BINARY, bytes(PREFACE));

        assertOpenAndSilent(server);
    }

    @Test
    void webSocketEndpointRejectsByteInputAndStreamEndpointRejectsMessages() {
        assertThatThrownBy(() -> webSocketServer().receive(bytes(PREFACE))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> server().receiveMessage(WebSocketMessageKind.BINARY, bytes(PREFACE)))
                .isInstanceOf(IllegalStateException.class);
    }
}
