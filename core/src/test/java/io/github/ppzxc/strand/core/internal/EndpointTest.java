package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** 엔드포인트의 외부 동작만 본다: 받은 바이트에 대해 보낸 바이트와 애플리케이션 이벤트. */
class EndpointTest {

    private static final String PREFACE = "c0de535452300000";
    private static final String CLOSE_PROTOCOL_ERROR = "00000010000300000000000000000000" + "00000001";

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

    private static String sent(Endpoint endpoint) {
        return hex(endpoint.drainOutput());
    }

    @Test
    void serverSendsItsHelloRightAfterThePreface() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().maxFrameSize(16384));

        server.receive(bytes(PREFACE));

        // length 30, HELLO, 파라미터 3개: max_frame_size 16384, max_concurrent_streams 100, capabilities 0
        assertThat(sent(server))
                .isEqualTo("0000001e 0001 00 00 00000000 0000 0000".replace(" ", "")
                        + "0001 00004000 0002 00000064 0003 00000000".replace(" ", ""));
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void helloFromThePeerIsAcknowledgedWithAnEmptyAckHello() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(bytes(PREFACE));
        server.drainOutput();

        server.receive(bytes("0000000c000100000000000000000000"));

        assertThat(sent(server)).isEqualTo("0000000c000101000000000000000000");
    }

    @Test
    void ackHelloFromThePeerProducesNoOutput() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(bytes(PREFACE));
        server.receive(bytes("0000000c000100000000000000000000"));
        server.drainOutput();

        server.receive(bytes("0000000c000101000000000000000000"));

        assertThat(server.drainOutput()).isEmpty();
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void pingIsAnsweredWithTheSameEightBytesAndAckFlag() {
        Endpoint server = established();

        server.receive(bytes("00000014 00020000 00000000 00000000 0102030405060708"));

        assertThat(sent(server)).isEqualTo("00000014000201000000000000000000" + "0102030405060708");
    }

    @Test
    void bytesMaySplitAtAnyPosition() {
        Endpoint server = established();
        byte[] ping = bytes("00000014 00020000 00000000 00000000 0102030405060708");

        for (byte b : ping) {
            assertThat(server.drainOutput()).isEmpty();
            server.receive(new byte[] {b});
        }

        assertThat(sent(server)).isEqualTo("00000014000201000000000000000000" + "0102030405060708");
    }

    @Test
    void frameLengthAboveTheAnnouncedLimitClosesWithProtocolError() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().maxFrameSize(16384));
        server.receive(bytes(PREFACE));
        server.drainOutput();

        // length 16381 = 알린 한도 16384 - 4 + 1. 본문이 다 오기 전에 헤더만으로 판정한다.
        server.receive(bytes("00003ffd"));

        assertThat(sent(server)).isEqualTo(CLOSE_PROTOCOL_ERROR);
        assertThat(server.drainEvents()).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void frameLengthAtTheAnnouncedLimitIsAccepted() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().maxFrameSize(16384));
        server.receive(bytes(PREFACE));
        server.drainOutput();

        server.receive(bytes("00003ffc"));

        assertThat(server.drainOutput()).isEmpty();
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void frameLengthBelowTheMinimumClosesWithProtocolError() {
        Endpoint server = established();

        server.receive(bytes("0000000b"));

        assertThat(sent(server)).isEqualTo(CLOSE_PROTOCOL_ERROR);
        assertThat(server.drainEvents()).hasSize(1);
    }

    @Test
    void nothingIsHandledOnceClosed() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().maxFrameSize(16384));
        server.receive(bytes(PREFACE));
        server.receive(bytes("00003ffd"));
        server.drainOutput();
        server.drainEvents();

        server.receive(bytes("00000014 00020000 00000000 00000000 0102030405060708"));

        assertThat(server.drainOutput()).isEmpty();
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void wrongPrefaceClosesSilently() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());

        server.receive(bytes("c1de535452300000"));

        assertThat(server.drainOutput()).isEmpty();
        assertThat(server.drainEvents()).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void drainReturnsEachOutputOnce() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(bytes(PREFACE));

        assertThat(server.drainOutput()).isNotEmpty();
        assertThat(server.drainOutput()).isEmpty();
    }

    private static Endpoint established() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(bytes(PREFACE));
        server.receive(bytes("0000000c000100000000000000000000"));
        server.receive(bytes("0000000c000101000000000000000000"));
        server.drainOutput();
        List<Event> none = server.drainEvents();
        assertThat(none).isEmpty();
        return server;
    }
}
