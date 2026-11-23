package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.TestMessages;
import java.io.IOException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** {@link WebSocketPassage}가 JDK WebSocket 콜백을 단위 입력({@link Inbound#unit})과 잘못된 입력({@link Inbound#unsupported()})으로 바꾸는 방식. */
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
        return TestMessages.firstMessage();
    }

    @Test
    void fragmentsOfOneBinaryMessageAreDeliveredAsOneBinaryMessage() throws Exception {
        listener.onOpen(socket);

        listener.onBinary(socket, buffer(1, 2), false);
        listener.onBinary(socket, buffer(3), true);

        Inbound inbound = passage.await(0);
        assertThat(inbound.kind()).isEqualTo(Inbound.Kind.UNIT);
        assertThat(inbound.data()).containsExactly(1, 2, 3);
        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.NONE);
    }

    @Test
    void aTextMessageIsDeliveredAsAnUnsupportedInput() throws Exception {
        listener.onOpen(socket);

        listener.onText(socket, "한", false);
        listener.onText(socket, "글", true);

        Inbound inbound = passage.await(0);
        assertThat(inbound.kind()).isEqualTo(Inbound.Kind.UNSUPPORTED);
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

        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.UNIT);
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

        passage.write(message, 0);

        assertThat(socket.sentBinary).hasSize(1);
        assertThat(socket.sentBinary.get(0)).isEqualTo(message.bytes());
        assertThat(socket.sentBinaryLast).containsExactly(true);
    }

    @Test
    void aFailedSendIsAnIoException() {
        listener.onOpen(socket);
        socket.failSends = true;

        assertThatThrownBy(() -> passage.write(firstMessage(), 0)).isInstanceOf(IOException.class);
    }

    @Test
    void writingBeforeTheSocketOpenedIsAnIoException() {
        assertThatThrownBy(() -> passage.write(firstMessage(), 0)).isInstanceOf(IOException.class);
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

    @Test
    void aMessageGrowingPastTheLimitAcrossFragmentsIsAnErrorAndAbortsTheSocket() {
        WebSocketPassage limited = new WebSocketPassage(10);
        limited.listener().onOpen(socket);

        limited.listener().onBinary(socket, buffer(1, 2, 3, 4, 5, 6), false);
        limited.listener().onBinary(socket, buffer(7, 8, 9, 10, 11), false);

        assertThat(socket.aborted).isTrue();
        assertThatThrownBy(() -> limited.await(0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("10");
    }

    @Test
    void aSingleMessageOverTheLimitIsAnError() {
        WebSocketPassage limited = new WebSocketPassage(4);
        limited.listener().onOpen(socket);

        limited.listener().onBinary(socket, buffer(1, 2, 3, 4, 5), true);

        assertThat(socket.aborted).isTrue();
        assertThatThrownBy(() -> limited.await(0)).isInstanceOf(IOException.class);
    }

    @Test
    void aTextMessageIsLimitedByItsUtf8Bytes() {
        WebSocketPassage limited = new WebSocketPassage(5);
        limited.listener().onOpen(socket);

        limited.listener().onText(socket, "한글", true);

        assertThat(socket.aborted).isTrue();
        assertThatThrownBy(() -> limited.await(0)).isInstanceOf(IOException.class);
    }

    @Test
    void aMessageExactlyAtTheLimitIsDelivered() throws Exception {
        WebSocketPassage limited = new WebSocketPassage(4);
        limited.listener().onOpen(socket);

        limited.listener().onBinary(socket, buffer(1, 2, 3, 4), true);

        assertThat(limited.await(0).data()).containsExactly(1, 2, 3, 4);
        assertThat(socket.aborted).isFalse();
    }

    @Test
    void fragmentsThatKeepArrivingAfterTheLimitWasHitAreIgnored() throws Exception {
        WebSocketPassage limited = new WebSocketPassage(4);
        limited.listener().onOpen(socket);
        limited.listener().onBinary(socket, buffer(1, 2, 3, 4, 5), false);

        limited.listener().onBinary(socket, buffer(6), true);

        assertThatThrownBy(() -> limited.await(0)).isInstanceOf(IOException.class);
        assertThat(limited.await(0).kind()).as("넘친 메시지의 나머지 조각이 메시지로 올라오지 않는다").isEqualTo(Inbound.Kind.NONE);
    }

    @Test
    void aFailedCloseWhenShuttingDownTheOutputAbortsTheSocket() {
        listener.onOpen(socket);
        socket.failSends = true;

        passage.shutdownOutput();

        assertThat(socket.aborted).isTrue();
    }

    @Test
    void manyOneCharFragmentsAreEncodedOnlyOnceEach() throws Exception {
        listener.onOpen(socket);

        for (int i = 0; i < 999; i++) {
            listener.onText(socket, "a", false);
        }
        listener.onText(socket, "a", true);

        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.UNSUPPORTED);
        assertThat(passage.encodedCharCount())
                .as("조각마다 전체를 다시 인코딩하면 이 값이 제곱으로 는다")
                .isEqualTo(1000);
    }

    @Test
    void theTextLimitCountsMultiByteCharactersAtTheirUtf8Size() throws Exception {
        WebSocketPassage limited = new WebSocketPassage(6);
        limited.listener().onOpen(socket);

        limited.listener().onText(socket, "한", false);
        limited.listener().onText(socket, "글", true);

        assertThat(limited.await(0).kind()).isEqualTo(Inbound.Kind.UNSUPPORTED);
        assertThat(socket.aborted).isFalse();
    }

    @Test
    void oneByteMoreThanTheLimitAcrossTextFragmentsIsAnError() {
        WebSocketPassage limited = new WebSocketPassage(6);
        limited.listener().onOpen(socket);

        limited.listener().onText(socket, "한", false);
        limited.listener().onText(socket, "글", false);
        limited.listener().onText(socket, "a", true);

        assertThat(socket.aborted).isTrue();
        assertThatThrownBy(() -> limited.await(0)).isInstanceOf(IOException.class);
    }

    @Test
    void aSurrogatePairSplitAcrossFragmentsIsEncodedAsOneCharacter() throws Exception {
        WebSocketPassage limited = new WebSocketPassage(4);
        limited.listener().onOpen(socket);

        limited.listener().onText(socket, "\uD83D", false);
        limited.listener().onText(socket, "\uDE00", true);

        assertThat(limited.await(0).kind()).isEqualTo(Inbound.Kind.UNSUPPORTED);
    }

    @Test
    void anOverflowQueuedWhileReadingIsPausedWakesAWriteWaitAndThrows() throws Exception {
        WebSocketPassage limited = new WebSocketPassage(4);
        FakeWebSocket held = new FakeWebSocket();
        limited.listener().onOpen(held);
        held.holdSends = true;
        limited.write(firstMessage(), 0);
        assertThat(limited.write(firstMessage(), 0)).as("앞선 보내기가 끝나지 않아 막힌다").isZero();
        java.util.concurrent.CompletableFuture<Object> waiting = new java.util.concurrent.CompletableFuture<>();
        Thread waiter = new Thread(() -> {
            try {
                waiting.complete(limited.await(-1, false, true));
            } catch (Throwable e) {
                waiting.complete(e);
            }
        });
        waiter.setDaemon(true);
        waiter.start();

        limited.listener().onBinary(held, buffer(1, 2, 3, 4, 5), true);

        assertThat(waiting.get(5, java.util.concurrent.TimeUnit.SECONDS)).isInstanceOf(IOException.class);
    }

    @Test
    void aWakeupWhileReadingIsPausedLeavesNoEmptyWakeupForTheResumedRead() throws Exception {
        listener.onOpen(socket);

        passage.wakeup();
        assertThat(passage.await(-1, false, false).kind()).isEqualTo(Inbound.Kind.NONE);
        listener.onBinary(socket, buffer(1), true);

        assertThat(passage.await(0).kind()).as("읽기를 다시 켜면 쌓인 메시지가 먼저다").isEqualTo(Inbound.Kind.UNIT);
        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.NONE);
    }

    @Test
    void aPollDoesNotConsumeAWakeupMeantForTheNextWait() throws Exception {
        listener.onOpen(socket);

        passage.wakeup();
        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.NONE);

        long start = System.nanoTime();
        assertThat(passage.await(10_000).kind()).isEqualTo(Inbound.Kind.NONE);
        assertThat(java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                .as("기다리기 전에 불린 wakeup이 다음 기다림을 바로 끝낸다")
                .isLessThan(5_000);
    }
}
