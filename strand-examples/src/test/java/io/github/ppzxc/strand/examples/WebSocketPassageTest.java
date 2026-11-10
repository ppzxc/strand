package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import io.github.ppzxc.strand.nio.internal.Inbound;
import java.io.IOException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** {@link WebSocketPassage}가 JDK WebSocket 콜백을 메시지 통로({@link Inbound#message})로 바꾸는 방식. */
class WebSocketPassageTest {

    private final WebSocketPassage passage = new WebSocketPassage();
    private final FakeWebSocket socket = new FakeWebSocket();
    private final WebSocket.Listener listener = passage.listener();

    private static ByteBuffer buffer(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return ByteBuffer.wrap(bytes);
    }

    private static OutboundMessage firstMessage() {
        return Endpoint.client(EndpointConfig.defaults().webSocket())
                .drainMessages()
                .get(0);
    }

    @Test
    void fragmentsOfOneBinaryMessageAreDeliveredAsOneBinaryMessage() throws Exception {
        listener.onOpen(socket);

        listener.onBinary(socket, buffer(1, 2), false);
        listener.onBinary(socket, buffer(3), true);

        Inbound inbound = passage.await(0);
        assertThat(inbound.kind()).isEqualTo(Inbound.Kind.MESSAGE);
        assertThat(inbound.messageKind()).isEqualTo(WebSocketMessageKind.BINARY);
        assertThat(inbound.data()).containsExactly(1, 2, 3);
        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.NONE);
    }

    @Test
    void aTextMessageIsDeliveredAsATextMessageWithItsUtf8Bytes() throws Exception {
        listener.onOpen(socket);

        listener.onText(socket, "한", false);
        listener.onText(socket, "글", true);

        Inbound inbound = passage.await(0);
        assertThat(inbound.kind()).isEqualTo(Inbound.Kind.MESSAGE);
        assertThat(inbound.messageKind()).isEqualTo(WebSocketMessageKind.TEXT);
        assertThat(inbound.data()).isEqualTo("한글".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void theNextMessageIsRequestedOnlyAfterTheDeliveredOneIsTaken() throws Exception {
        listener.onOpen(socket);
        assertThat(socket.requests).containsExactly(1L);

        listener.onBinary(socket, buffer(1), true);
        assertThat(socket.requests).as("꺼내기 전에는 다음을 받지 않는다").containsExactly(1L);

        passage.await(0);
        assertThat(socket.requests).containsExactly(1L, 1L);
    }

    @Test
    void aNonFinalFragmentRequestsTheNextFragmentAtOnce() {
        listener.onOpen(socket);

        listener.onBinary(socket, buffer(1), false);

        assertThat(socket.requests).containsExactly(1L, 1L);
    }

    @Test
    void theMessageIsCopiedBecauseTheCallerMayReuseItsBuffer() throws Exception {
        listener.onOpen(socket);
        ByteBuffer reused = buffer(7, 8);

        listener.onBinary(socket, reused, true);
        Arrays.fill(reused.array(), (byte) 0);

        assertThat(passage.await(0).data()).containsExactly(7, 8);
    }

    @Test
    void theCloseFromThePeerEndsTheReadingAndStaysEnded() throws Exception {
        listener.onOpen(socket);

        listener.onClose(socket, 1000, "");

        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.END);
        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.END);
    }

    @Test
    void messagesThatArrivedBeforeTheCloseAreReadBeforeTheEnd() throws Exception {
        listener.onOpen(socket);
        listener.onBinary(socket, buffer(5), true);
        listener.onClose(socket, 1000, "");

        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.MESSAGE);
        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.END);
    }

    @Test
    void anErrorMakesTheNextAwaitThrowAnIoException() {
        listener.onOpen(socket);

        listener.onError(socket, new IllegalStateException("깨짐"));

        assertThatThrownBy(() -> passage.await(0))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void writingABinaryMessageSendsItAsOneWholeWebSocketMessage() throws Exception {
        listener.onOpen(socket);
        OutboundMessage message = firstMessage();

        passage.write(message);

        assertThat(socket.sentBinary).hasSize(1);
        assertThat(socket.sentBinary.get(0)).isEqualTo(message.bytes());
        assertThat(socket.sentBinaryLast).containsExactly(true);
    }

    @Test
    void aFailedSendIsAnIoException() {
        listener.onOpen(socket);
        socket.failSends = true;

        assertThatThrownBy(() -> passage.write(firstMessage())).isInstanceOf(IOException.class);
    }

    @Test
    void writingBeforeTheSocketOpenedIsAnIoException() {
        assertThatThrownBy(() -> passage.write(firstMessage())).isInstanceOf(IOException.class);
    }

    @Test
    void shuttingDownTheOutputSendsANormalCloseOnlyOnce() {
        listener.onOpen(socket);

        passage.shutdownOutput();
        passage.shutdownOutput();

        assertThat(socket.sentCloseCodes).containsExactly(1000);
        assertThat(socket.aborted).isFalse();
    }

    @Test
    void closingAbortsTheSocketAndLaterAwaitsThrow() {
        listener.onOpen(socket);

        passage.close();
        passage.close();

        assertThat(socket.aborted).isTrue();
        assertThatThrownBy(() -> passage.await(0)).isInstanceOf(IOException.class);
    }
}
