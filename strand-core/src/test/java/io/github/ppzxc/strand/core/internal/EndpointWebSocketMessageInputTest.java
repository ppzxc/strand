package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** WebSocket 메시지 입력의 종류·시작·조각·끝 보존과 끝 검증 전 효과 미적용(규격 1.2절, 5.1절 3번, 이슈 #321). */
class EndpointWebSocketMessageInputTest {

    private static final int REQUEST_TYPE = 257;
    private static final long MAX_FRAME = 16_384;

    private static Endpoint webSocketServer() {
        return Endpoint.server(EndpointConfig.defaults().webSocket());
    }

    private static Endpoint establishedServer() {
        Endpoint server = webSocketServer();
        server.receiveMessage(WebSocketMessageKind.BINARY, Frames.preface());
        server.receiveMessage(WebSocketMessageKind.BINARY, Frames.hello(EndpointConfig.defaults()));
        server.receiveMessage(WebSocketMessageKind.BINARY, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        server.receiveMessage(WebSocketMessageKind.BINARY, Frames.auth("ANONYMOUS", new byte[0]));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    private static ReceiveProgress fragment(
            Endpoint endpoint, WebSocketMessageKind kind, boolean start, boolean end, byte[] bytes) {
        return endpoint.receiveMessageFragment(kind, start, end, bytes, 0, bytes.length, CollectionSpace.UNLIMITED);
    }

    /** 메시지를 {@code sizes} 크기의 조각으로 나눠 넣는다. 마지막 조각이 끝이다. */
    private static void feed(Endpoint endpoint, WebSocketMessageKind kind, byte[] message, int... sizes) {
        int at = 0;
        for (int i = 0; i < sizes.length; i++) {
            boolean last = i == sizes.length - 1;
            byte[] part = Arrays.copyOfRange(message, at, at + sizes[i]);
            at += sizes[i];
            ReceiveProgress progress = fragment(endpoint, kind, i == 0, last, part);
            assertThat(progress.collectionRequired()).isFalse();
        }
        assertThat(at).isEqualTo(message.length);
    }

    private static List<Event.Kind> eventKinds(Endpoint endpoint) {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : endpoint.drainEvents()) {
            kinds.add(event.kind());
        }
        return kinds;
    }

    private static boolean sentErrorClose(Endpoint endpoint) {
        for (OutboundMessage message : endpoint.drainMessages()) {
            byte[] bytes = message.bytes();
            if (message.kind() == OutboundMessage.Kind.BINARY
                    && bytes.length >= Frames.HEADER_SIZE
                    && Frames.typeOf(bytes, 0) == FrameType.CLOSE) {
                return true;
            }
        }
        return false;
    }

    // ---- 조각 입력: 끝에서만 효과 ----

    @Test
    void fragmentedFrameDeliversTheRequestOnlyAtTheMessageEnd() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1, 2, 3});

        fragment(server, WebSocketMessageKind.BINARY, true, false, Arrays.copyOfRange(request, 0, 5));
        fragment(server, WebSocketMessageKind.BINARY, false, false, Arrays.copyOfRange(request, 5, 21));
        assertThat(server.drainEvents()).isEmpty();
        fragment(server, WebSocketMessageKind.BINARY, false, true, Arrays.copyOfRange(request, 21, request.length));

        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.REQUEST_RECEIVED);
        assertThat(events.get(0).body()).containsExactly(1, 2, 3);
    }

    @Test
    void everyFragmentationOfAValidFrameHasTheSameEffectAsTheWholeMessage() {
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {9, 8, 7, 6});
        for (int cut = 1; cut < request.length; cut++) {
            Endpoint fragmented = establishedServer();
            feed(fragmented, WebSocketMessageKind.BINARY, request, cut, request.length - cut);
            Endpoint whole = establishedServer();
            whole.receiveMessage(WebSocketMessageKind.BINARY, request);

            assertThat(eventKinds(fragmented)).as("cut=%d", cut).isEqualTo(eventKinds(whole));
        }
    }

    @Test
    void emptyFragmentsKeepTheMessageOpenWithoutEffect() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[0]);

        fragment(server, WebSocketMessageKind.BINARY, true, false, new byte[0]);
        fragment(server, WebSocketMessageKind.BINARY, false, false, request);
        assertThat(server.drainEvents()).isEmpty();
        fragment(server, WebSocketMessageKind.BINARY, false, true, new byte[0]);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    // ---- 여분 바이트·복수 프레임·사이에 나뉜 프레임 ----

    @Test
    void surplusBytesAfterANormalFrameAreAProtocolErrorWithoutApplyingTheFrame() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1});

        feed(server, WebSocketMessageKind.BINARY, concat(request, new byte[] {0}), request.length, 1);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(sentErrorClose(server)).isTrue();
    }

    @Test
    void twoFramesInOneMessageAreAProtocolErrorAndNeitherIsApplied() {
        Endpoint server = establishedServer();
        byte[] first = Frames.request(REQUEST_TYPE, 1, new byte[] {1});
        byte[] second = Frames.request(REQUEST_TYPE, 3, new byte[] {2});

        feed(server, WebSocketMessageKind.BINARY, concat(first, second), first.length, second.length);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void aFrameSplitAcrossTwoMessagesIsAProtocolErrorAndNotApplied() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1, 2});

        fragment(server, WebSocketMessageKind.BINARY, true, true, Arrays.copyOfRange(request, 0, 10));
        fragment(server, WebSocketMessageKind.BINARY, true, true, Arrays.copyOfRange(request, 10, request.length));

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void aMessageShorterThanItsDeclaredFrameIsAProtocolErrorAtTheEnd() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1, 2});

        feed(server, WebSocketMessageKind.BINARY, Arrays.copyOf(request, request.length - 1), 10, request.length - 11);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    // ---- 텍스트 ----

    @Test
    void aTextMessageIsRejectedAtItsStartAndItsLaterFragmentsAreDiscarded() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1});

        ReceiveProgress first = fragment(server, WebSocketMessageKind.TEXT, true, false, request);
        assertThat(first.consumed()).isEqualTo(request.length);
        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        ReceiveProgress rest = fragment(server, WebSocketMessageKind.TEXT, false, true, request);

        assertThat(rest.consumed()).isZero();
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void aTextPrefaceClosesSilentlyAtItsStart() {
        Endpoint server = webSocketServer();

        fragment(server, WebSocketMessageKind.TEXT, true, false, Arrays.copyOf(Frames.preface(), 4));

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainMessages()).isEmpty();
    }

    // ---- 서문 ----

    @Test
    void aFragmentedPrefaceIsAnsweredWithHelloOnlyAtTheMessageEnd() {
        Endpoint server = webSocketServer();
        byte[] preface = Frames.preface();

        fragment(server, WebSocketMessageKind.BINARY, true, false, Arrays.copyOfRange(preface, 0, 3));
        fragment(server, WebSocketMessageKind.BINARY, false, false, Arrays.copyOfRange(preface, 3, 7));
        assertThat(server.drainMessages()).isEmpty();
        fragment(server, WebSocketMessageKind.BINARY, false, true, Arrays.copyOfRange(preface, 7, 8));

        assertThat(server.drainMessages()).hasSize(1);
    }

    @Test
    void aPrefaceLongerThanEightBytesClosesSilentlyBeforeTheMessageEnds() {
        Endpoint server = webSocketServer();

        fragment(server, WebSocketMessageKind.BINARY, true, false, concat(Frames.preface(), new byte[] {0}));

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void aPrefaceShorterThanEightBytesClosesSilentlyAtTheEnd() {
        Endpoint server = webSocketServer();
        byte[] preface = Frames.preface();

        fragment(server, WebSocketMessageKind.BINARY, true, false, Arrays.copyOfRange(preface, 0, 4));
        assertThat(server.drainEvents()).isEmpty();
        fragment(server, WebSocketMessageKind.BINARY, false, true, Arrays.copyOfRange(preface, 4, 7));

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainMessages()).isEmpty();
    }

    // ---- 과대 입력과 유한 조립 공간 ----

    @Test
    void anOversizedFrameIsRejectedFromItsHeaderWithoutAssemblingTheWholeMessage() {
        Endpoint server = establishedServer();
        byte[] header = Frames.request(REQUEST_TYPE, 1, new byte[0]);
        long declared = MAX_FRAME * 4;
        header[0] = (byte) (declared >>> 24);
        header[1] = (byte) (declared >>> 16);
        header[2] = (byte) (declared >>> 8);
        header[3] = (byte) declared;

        fragment(server, WebSocketMessageKind.BINARY, true, false, header);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.messageAssemblyLength()).isZero();
        byte[] chunk = new byte[8192];
        for (int i = 0; i < 64; i++) {
            fragment(server, WebSocketMessageKind.BINARY, false, i == 63, chunk);
            assertThat(server.messageAssemblyLength()).isZero();
        }
    }

    @Test
    void assemblySpaceNeverExceedsTheMaximumFrameSize() {
        Endpoint server = establishedServer();
        byte[] body = new byte[(int) MAX_FRAME - Frames.HEADER_SIZE];
        byte[] request = Frames.request(REQUEST_TYPE, 1, body);
        assertThat(request).hasSize((int) MAX_FRAME);

        fragment(server, WebSocketMessageKind.BINARY, true, false, Arrays.copyOfRange(request, 0, 100));
        assertThat(server.messageAssemblyLength()).isEqualTo(100);
        fragment(server, WebSocketMessageKind.BINARY, false, false, Arrays.copyOfRange(request, 100, request.length));
        assertThat(server.messageAssemblyLength()).isEqualTo(request.length);
        fragment(server, WebSocketMessageKind.BINARY, false, true, new byte[0]);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
        assertThat(server.messageAssemblyLength()).isZero();
    }

    @Test
    void aWholeOversizedMessageIsRejectedWithoutBeingCollected() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[(int) MAX_FRAME]);

        fragment(server, WebSocketMessageKind.BINARY, true, true, request);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.messageAssemblyLength()).isZero();
    }

    // ---- 수거 중단·재개 ----

    @Test
    void stopsBeforeTheMessageEndWhenCollectionIsNeededAndResumesWithoutDuplicateConsumption() {
        Endpoint server = establishedServer();
        CollectionSpace space = CollectionSpace.of(2, 4);
        byte[] first = Frames.request(REQUEST_TYPE, 1, new byte[] {1});
        byte[] second = Frames.request(REQUEST_TYPE, 3, new byte[] {2});
        server.receiveMessageFragment(WebSocketMessageKind.BINARY, true, true, first, 0, first.length, space);

        byte[] head = Arrays.copyOfRange(second, 0, 10);
        byte[] tail = Arrays.copyOfRange(second, 10, second.length);
        ReceiveProgress started =
                server.receiveMessageFragment(WebSocketMessageKind.BINARY, true, false, head, 0, head.length, space);
        assertThat(started.collectionRequired()).isTrue();
        assertThat(started.consumed()).isZero();
        assertThat(server.messageAssemblyLength()).isZero();

        assertThat(server.drainEvents()).hasSize(1);
        started = server.receiveMessageFragment(WebSocketMessageKind.BINARY, true, false, head, 0, head.length, space);
        assertThat(started.consumed()).isEqualTo(head.length);
        ReceiveProgress finished =
                server.receiveMessageFragment(WebSocketMessageKind.BINARY, false, true, tail, 0, tail.length, space);
        assertThat(finished.consumed()).isEqualTo(tail.length);

        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).streamId()).isEqualTo(3);
    }

    @Test
    void stoppingAtTheLastFragmentKeepsTheEarlierFragmentsAndAppliesNoEffectEarly() {
        Endpoint server = establishedServer();
        CollectionSpace space = CollectionSpace.of(2, 4);
        byte[] request = Frames.request(REQUEST_TYPE, 3, new byte[] {5});
        byte[] head = Arrays.copyOfRange(request, 0, 12);
        byte[] tail = Arrays.copyOfRange(request, 12, request.length);
        assertThat(server.receiveMessageFragment(WebSocketMessageKind.BINARY, true, false, head, 0, head.length, space)
                        .consumed())
                .isEqualTo(head.length);
        // 메시지 도중 수거하지 않은 사건이 생긴다.
        server.cancelRequest(server.request(REQUEST_TYPE, new byte[0]));

        ReceiveProgress stopped =
                server.receiveMessageFragment(WebSocketMessageKind.BINARY, false, true, tail, 0, tail.length, space);

        assertThat(stopped.collectionRequired()).isTrue();
        assertThat(stopped.consumed()).isZero();
        assertThat(server.messageAssemblyLength()).isEqualTo(head.length);
        assertThat(eventKinds(server)).containsExactly(Event.Kind.RESULT);
        ReceiveProgress resumed =
                server.receiveMessageFragment(WebSocketMessageKind.BINARY, false, true, tail, 0, tail.length, space);
        assertThat(resumed.consumed()).isEqualTo(tail.length);
        assertThat(eventKinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    // ---- 호출 규약 ----

    @Test
    void aContinuationWithoutAStartIsRejectedWithoutChangingState() {
        Endpoint server = establishedServer();

        assertThatIllegalStateException()
                .isThrownBy(() -> fragment(server, WebSocketMessageKind.BINARY, false, true, new byte[] {1}));
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void aSecondStartInsideAnOpenMessageIsRejected() {
        Endpoint server = establishedServer();
        fragment(server, WebSocketMessageKind.BINARY, true, false, new byte[] {0});

        assertThatIllegalStateException()
                .isThrownBy(() -> fragment(server, WebSocketMessageKind.BINARY, true, false, new byte[] {0}));
    }

    @Test
    void theKindMustNotChangeInsideAMessage() {
        Endpoint server = establishedServer();
        fragment(server, WebSocketMessageKind.BINARY, true, false, new byte[] {0});

        assertThatIllegalArgumentException()
                .isThrownBy(() -> fragment(server, WebSocketMessageKind.TEXT, false, true, new byte[] {0}));
    }

    @Test
    void invalidRangesAreRejected() {
        Endpoint server = establishedServer();

        assertThatIllegalArgumentException()
                .isThrownBy(() -> server.receiveMessageFragment(
                        WebSocketMessageKind.BINARY, true, true, new byte[2], 1, 2, CollectionSpace.UNLIMITED));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> server.receiveMessageFragment(
                        WebSocketMessageKind.BINARY, true, true, new byte[2], -1, 1, CollectionSpace.UNLIMITED));
    }

    @Test
    void fragmentsAreOnlyForWebSocketEndpointsAndByteInputIsOnlyForStreamEndpoints() {
        Endpoint stream = Endpoint.server(EndpointConfig.defaults());

        assertThatIllegalStateException()
                .isThrownBy(() -> fragment(stream, WebSocketMessageKind.BINARY, true, true, Frames.preface()));
        assertThatIllegalStateException().isThrownBy(() -> establishedServer().receive(Frames.preface()));
        assertThatIllegalStateException()
                .isThrownBy(
                        () -> establishedServer().receiveBounded(Frames.preface(), 0, 8, 1, CollectionSpace.UNLIMITED));
    }

    @Test
    void anEndedConnectionConsumesNothingAndIgnoresFragments() {
        Endpoint server = establishedServer();
        server.disconnect();
        server.drainEvents();

        ReceiveProgress progress = fragment(server, WebSocketMessageKind.BINARY, true, true, Frames.preface());

        assertThat(progress.consumed()).isZero();
        assertThat(progress.collectionRequired()).isFalse();
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void borrowedFragmentsAreCopiedSoTheCallerMayReuseThem() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {4, 5});
        byte[] head = Arrays.copyOfRange(request, 0, 9);
        fragment(server, WebSocketMessageKind.BINARY, true, false, head);
        Arrays.fill(head, (byte) 0);

        fragment(server, WebSocketMessageKind.BINARY, false, true, Arrays.copyOfRange(request, 9, request.length));

        assertThat(eventKinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void closeBeforeTheMessageEndsDiscardsThePartialMessageWithoutEffect() {
        Endpoint server = establishedServer();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1});
        fragment(server, WebSocketMessageKind.BINARY, true, false, Arrays.copyOfRange(request, 0, 10));

        server.webSocketClosed(1000);

        assertThat(eventKinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.messageAssemblyLength()).isZero();
    }
}
