package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.netty.internal.MessageWire;
import io.github.ppzxc.strand.netty.internal.WebSocketWire;
import io.github.ppzxc.strand.netty.internal.WireMessage;
import io.github.ppzxc.strand.netty.internal.WireUpgradeException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** 제품 WebSocket 통로를 소켓 없이 직접 구동해 메시지 변환·Close·거절 경로를 확인한다. */
class WebSocketWireTest {
    private static final int MAX = 1024;

    private static byte[] clientFrame(int opcode, boolean fin, byte[] payload) {
        return RawWebSocket.encode(opcode, fin, payload, true);
    }

    private static RawWebSocket.Frame onlyFrame(byte[] wire) throws IOException {
        return RawWebSocket.read(new ByteArrayInputStream(wire));
    }

    @Test
    void aBinaryMessageBecomesAMessageAndOneIsSentAsOneFrame() throws Exception {
        WebSocketWire wire = WebSocketWire.established(false, MAX);

        wire.feed(clientFrame(RawWebSocket.BINARY, true, new byte[] {1, 2, 3}));
        WireMessage message = wire.pollMessage();
        wire.sendMessage(new byte[] {9});

        assertThat(message.kind()).isEqualTo(WireMessage.Kind.BINARY);
        assertThat(message.bytes()).containsExactly(1, 2, 3);
        assertThat(message.toString()).contains("BINARY");
        assertThat(wire.wirePending()).isTrue();
        RawWebSocket.Frame frame = onlyFrame(wire.pollWire());
        assertThat(frame.opcode).isEqualTo(RawWebSocket.BINARY);
        assertThat(frame.payload).containsExactly(9);
        assertThat(wire.pollWire()).isNull();
        assertThat(wire.hasMessages()).isFalse();
        wire.release();
        wire.release();
    }

    @Test
    void aTextMessageAndAnOversizedFrameAreUnsupportedInput() throws Exception {
        WebSocketWire text = WebSocketWire.established(false, MAX);
        text.feed(clientFrame(RawWebSocket.TEXT, true, new byte[] {'a'}));
        WebSocketWire oversized = WebSocketWire.established(false, MAX);
        oversized.feed(RawWebSocket.headerClaiming(RawWebSocket.BINARY, true, MAX + 1, true));
        WebSocketWire unmasked = WebSocketWire.established(false, MAX);
        unmasked.feed(RawWebSocket.encode(RawWebSocket.BINARY, true, new byte[] {1}, false));

        assertThat(text.pollMessage().kind()).isEqualTo(WireMessage.Kind.UNSUPPORTED);
        assertThat(oversized.pollMessage().kind()).isEqualTo(WireMessage.Kind.UNSUPPORTED);
        assertThat(unmasked.pollMessage().kind()).isEqualTo(WireMessage.Kind.UNSUPPORTED);
        // 위반 뒤의 입력은 무시한다.
        text.feed(clientFrame(RawWebSocket.BINARY, true, new byte[] {1}));
        assertThat(text.pollMessage()).isNull();
        assertThat(WireMessage.unsupported().toString()).isEqualTo("UNSUPPORTED");
    }

    @Test
    void aPingIsAnsweredWithAPongAndNeverReachesTheEngine() throws Exception {
        WebSocketWire wire = WebSocketWire.established(false, MAX);

        wire.feed(clientFrame(RawWebSocket.PING, true, new byte[] {5}));
        wire.feed(clientFrame(RawWebSocket.PONG, true, new byte[0]));

        assertThat(wire.pollMessage()).isNull();
        RawWebSocket.Frame pong = onlyFrame(wire.pollWire());
        assertThat(pong.opcode).isEqualTo(RawWebSocket.PONG);
        assertThat(pong.payload).containsExactly(5);
    }

    @Test
    void aPeerCloseIsEchoedWithItsCodeAndReportedOnce() throws Exception {
        WebSocketWire wire = WebSocketWire.established(false, MAX);

        wire.feed(clientFrame(RawWebSocket.CLOSE, true, RawWebSocket.closePayload(1001)));

        assertThat(wire.pollMessage().kind()).isEqualTo(WireMessage.Kind.PEER_CLOSED);
        RawWebSocket.Frame echo = onlyFrame(wire.pollWire());
        assertThat(echo.closeStatus()).isEqualTo(1001);
        assertThat(echo.closeReason()).isEmpty();
        assertThat(wire.closeSent()).isTrue();
        // 이미 닫았으므로 더 보내지 않고, 메시지도 보낼 수 없다.
        wire.sendClose(1000);
        assertThat(wire.pollWire()).isNull();
        assertThatThrownBy(() -> wire.sendMessage(new byte[] {1})).isInstanceOf(IOException.class);
        WireMessage ignored = WireMessage.peerClosed();
        assertThat(ignored.kind()).isEqualTo(WireMessage.Kind.PEER_CLOSED);
    }

    @Test
    void aCloseWithoutAStatusIsEchoedWithoutOne() throws Exception {
        WebSocketWire wire = WebSocketWire.established(false, MAX);

        wire.feed(clientFrame(RawWebSocket.CLOSE, true, new byte[0]));

        assertThat(onlyFrame(wire.pollWire()).closeStatus()).isEqualTo(-1);
    }

    @Test
    void closeCanBeSentOnceAndNotBeforeTheUpgrade() throws Exception {
        WebSocketWire wire = WebSocketWire.established(true, MAX);

        wire.sendClose(1000);
        wire.sendClose(1011);

        RawWebSocket.Frame close = onlyFrame(wire.pollWire());
        assertThat(close.closeStatus()).isEqualTo(1000);
        assertThat(wire.pollWire()).isNull();

        WebSocketWire serverBefore = WebSocketWire.server("/s", MAX);
        serverBefore.sendClose(1000);
        assertThat(serverBefore.closeSent()).isFalse();
        assertThat(serverBefore.established()).isFalse();
        assertThatThrownBy(() -> serverBefore.sendMessage(new byte[1])).isInstanceOf(IOException.class);
        serverBefore.start();
        serverBefore.release();
        serverBefore.feed(new byte[] {1});
    }

    private static String upgrade(String path, String headers) {
        return "GET " + path + " HTTP/1.1\r\nHost: h\r\n" + headers + "\r\n";
    }

    private static final String UPGRADE_HEADERS = "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: strand\r\n";

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    @Test
    void aServerRejectsAnUnsupportedWebSocketVersionWith426() throws Exception {
        MessageWire wire = WebSocketWire.server("/s", MAX);

        assertThatThrownBy(() -> wire.feed(ascii(upgrade("/s", UPGRADE_HEADERS + "Sec-WebSocket-Version: 9\r\n"))))
                .isInstanceOfSatisfying(
                        WireUpgradeException.class,
                        e -> assertThat(e.httpStatus()).isEqualTo(426));
    }

    @Test
    void aServerRejectsGarbageAndAMissingVersionWith400() throws Exception {
        MessageWire garbage = WebSocketWire.server("/s", MAX);
        MessageWire noVersion = WebSocketWire.server("/s", MAX);

        assertThatThrownBy(() -> garbage.feed(ascii("XYZ\r\n\r\n"))).isInstanceOf(WireUpgradeException.class);
        assertThatThrownBy(() -> noVersion.feed(ascii(upgrade("/s", UPGRADE_HEADERS))))
                .isInstanceOf(WireUpgradeException.class);
    }

    @Test
    void aServerRejectsAPost() throws Exception {
        MessageWire post = WebSocketWire.server("/s", MAX);

        assertThatThrownBy(() -> post.feed(ascii("POST /s HTTP/1.1\r\nHost: h\r\n" + UPGRADE_HEADERS + "\r\n")))
                .isInstanceOfSatisfying(
                        WireUpgradeException.class,
                        e -> assertThat(e.httpStatus()).isEqualTo(404));
    }

    @Test
    void aClientRejectsANon101ResponseAndGarbage() throws Exception {
        MessageWire refused = WebSocketWire.client(URI.create("ws://h:1/s"), MAX);
        refused.start();
        assertThat(refused.pollWire()).isNotNull();
        MessageWire garbage = WebSocketWire.client(URI.create("ws://h:1/s"), MAX);
        garbage.start();

        assertThatThrownBy(() -> refused.feed(ascii("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")))
                .isInstanceOfSatisfying(
                        WireUpgradeException.class,
                        e -> assertThat(e.httpStatus()).isEqualTo(404));
        assertThatThrownBy(() -> garbage.feed(ascii("XYZ\r\n\r\n"))).isInstanceOf(WireUpgradeException.class);
    }

    @Test
    void aClientWithoutAnUpgradeRequestIsNotEstablishedAndSendsNothing() throws Exception {
        MessageWire server = WebSocketWire.server("/s", MAX);

        assertThat(server.established()).isFalse();
        assertThat(server.wirePending()).isFalse();
        assertThat(server.pollWire()).isNull();
        assertThat(server.pollMessage()).isNull();
    }
}
