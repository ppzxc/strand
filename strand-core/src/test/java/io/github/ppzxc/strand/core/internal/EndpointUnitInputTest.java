package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 단위 입력과 바이트로 넘길 수 없는 입력 신호: 연결 단계별 경계 위반 반응과 스트림 입력과의 동등성(이슈 #367). */
class EndpointUnitInputTest {

    private static final int REQUEST_TYPE = 257;
    private static final CollectionSpace SPACE = CollectionSpace.UNLIMITED;

    private static Endpoint server(boolean webSocket) {
        EndpointConfig config = webSocket ? EndpointConfig.defaults().webSocket() : EndpointConfig.defaults();
        return Endpoint.server(config);
    }

    private static ReceiveProgress unit(Endpoint endpoint, byte[] bytes) {
        return endpoint.receiveUnit(bytes, 0, bytes.length, SPACE);
    }

    private static Endpoint established(boolean webSocket) {
        Endpoint server = server(webSocket);
        unit(server, Frames.preface());
        unit(server, Frames.hello(EndpointConfig.defaults()));
        unit(server, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        unit(server, Frames.auth("ANONYMOUS", new byte[0]));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static List<Event.Kind> kinds(Endpoint endpoint) {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : endpoint.drainEvents()) {
            kinds.add(event.kind());
        }
        return kinds;
    }

    private static boolean sentClose(Endpoint endpoint) {
        for (OutboundMessage message : endpoint.drainMessages()) {
            byte[] bytes = message.bytes();
            if (bytes.length >= Frames.HEADER_SIZE && Frames.typeOf(bytes, 0) == FrameType.CLOSE) {
                return true;
            }
        }
        return false;
    }

    private static byte[] withLength(byte[] frame, int extra) {
        return Arrays.copyOf(frame, frame.length + extra);
    }

    // ---- 크기가 맞는 단위 ----

    @Test
    void aWholePrefaceUnitIsAnsweredWithHello() {
        for (boolean webSocket : new boolean[] {false, true}) {
            Endpoint server = server(webSocket);

            ReceiveProgress progress = unit(server, Frames.preface());

            assertThat(progress.consumed()).isEqualTo(Frames.preface().length);
            assertThat(progress.collectionRequired()).isFalse();
            assertThat(server.drainMessages()).hasSize(1);
        }
    }

    @Test
    void rightSizedUnitsHaveTheSameEventsAndOutputAsTheSameBytesAsAStream() {
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1, 2, 3});
        Endpoint byUnit = established(false);
        Endpoint byStream = established(false);

        unit(byUnit, request);
        byStream.receive(request);

        List<Event> unitEvents = byUnit.drainEvents();
        List<Event> streamEvents = byStream.drainEvents();
        assertThat(unitEvents).hasSize(1);
        assertThat(unitEvents.get(0).kind()).isEqualTo(streamEvents.get(0).kind());
        assertThat(unitEvents.get(0).body()).containsExactly(1, 2, 3);
        assertThat(byUnit.drainMessages()).hasSameSizeAs(byStream.drainMessages());
    }

    @Test
    void aUnitReadsOnlyItsRangeOfTheArray() {
        Endpoint server = established(false);
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {7});
        byte[] padded = new byte[request.length + 6];
        System.arraycopy(request, 0, padded, 3, request.length);

        ReceiveProgress progress = server.receiveUnit(padded, 3, request.length, SPACE);

        assertThat(progress.consumed()).isEqualTo(request.length);
        assertThat(kinds(server)).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    // ---- 서문 단계 ----

    @Test
    void aPrefaceUnitOfTheWrongSizeClosesSilently() {
        byte[] preface = Frames.preface();
        for (byte[] bad : new byte[][] {Arrays.copyOf(preface, 7), Arrays.copyOf(preface, 9), new byte[0]}) {
            Endpoint server = server(false);

            unit(server, bad);

            assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
            assertThat(server.drainMessages()).isEmpty();
        }
    }

    // ---- 첫 프레임 단계 ----

    @Test
    void aFirstFrameUnitTooShortToReadItsHeaderClosesSilently() {
        byte[] hello = Frames.hello(EndpointConfig.defaults());
        Endpoint server = server(false);
        unit(server, Frames.preface());
        server.drainMessages();

        unit(server, Arrays.copyOf(hello, Frames.HEADER_SIZE - 1));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void aHelloUnitOfTheWrongSizeIsAProtocolErrorOnceTheFirstFrameCheckPassed() {
        byte[] hello = Frames.hello(EndpointConfig.defaults());
        for (byte[] bad : new byte[][] {withLength(hello, 1), Arrays.copyOf(hello, hello.length - 1)}) {
            Endpoint server = server(false);
            unit(server, Frames.preface());
            server.drainMessages();

            unit(server, bad);

            assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
            assertThat(sentClose(server)).isTrue();
        }
    }

    @Test
    void aFirstFrameThatIsNotHelloClosesSilently() {
        Endpoint server = server(false);
        unit(server, Frames.preface());
        server.drainMessages();

        unit(server, Frames.request(REQUEST_TYPE, 1, new byte[0]));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainMessages()).isEmpty();
    }

    // ---- 그 뒤 ----

    @Test
    void aLaterUnitOfTheWrongSizeIsAProtocolErrorAndTheFrameIsNotApplied() {
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1});
        for (byte[] bad : new byte[][] {
            withLength(request, 1), Arrays.copyOf(request, request.length - 1), Arrays.copyOf(request, 2)
        }) {
            Endpoint server = established(false);

            unit(server, bad);

            assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
            assertThat(sentClose(server)).isTrue();
        }
    }

    @Test
    void aWrongSizedCloseUnitClosesWithoutSendingClose() {
        byte[] close = Frames.control(FrameType.CLOSE, 0, new byte[12]);
        Endpoint server = established(false);

        unit(server, withLength(close, 3));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(sentClose(server)).isFalse();
    }

    // ---- 바이트로 넘길 수 없는 입력 ----

    @Test
    void unsupportedInputInThePrefaceAndFirstFramePhasesClosesSilently() {
        Endpoint atPreface = server(false);
        atPreface.receiveUnsupportedInput(SPACE);
        assertThat(kinds(atPreface)).containsExactly(Event.Kind.CLOSED);
        assertThat(atPreface.drainMessages()).isEmpty();

        Endpoint atFirstFrame = server(false);
        unit(atFirstFrame, Frames.preface());
        atFirstFrame.drainMessages();
        atFirstFrame.receiveUnsupportedInput(SPACE);
        assertThat(kinds(atFirstFrame)).containsExactly(Event.Kind.CLOSED);
        assertThat(atFirstFrame.drainMessages()).isEmpty();
    }

    @Test
    void unsupportedInputAfterTheFirstFrameIsAProtocolError() {
        for (boolean webSocket : new boolean[] {false, true}) {
            Endpoint server = established(webSocket);

            server.receiveUnsupportedInput(SPACE);

            assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
            assertThat(sentClose(server)).isTrue();
        }
    }

    @Test
    void aUnitAboveTheMaxFrameSizeIsAProtocolError() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().maxFrameSize(16384));
        unit(server, Frames.preface());
        unit(server, Frames.hello(EndpointConfig.defaults()));
        unit(server, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        unit(server, Frames.auth("ANONYMOUS", new byte[0]));
        server.drainMessages();
        server.drainEvents();

        unit(server, Frames.request(REQUEST_TYPE, 1, new byte[16384]));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(sentClose(server)).isTrue();
    }

    @Test
    void unsupportedInputAsAnAlreadySentOrReceivedCloseSendsNoSecondClose() {
        Endpoint closing = established(false);
        closing.close(ErrorCode.NO_ERROR);
        closing.drainMessages();
        closing.drainEvents();

        closing.receiveUnsupportedInput(SPACE);

        assertThat(closing.drainMessages()).isEmpty();
    }

    @Test
    void aClientAlsoClosesSilentlyOnUnsupportedFirstInput() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().webSocket());
        client.drainMessages();

        client.receiveUnsupportedInput(SPACE);

        assertThat(kinds(client)).containsExactly(Event.Kind.CLOSED);
        assertThat(client.drainMessages()).isEmpty();
    }

    // ---- 수거 공간 계약 ----

    @Test
    void stopsWithoutConsumingWhenCollectionIsNeededAndResumesWithTheSameUnit() {
        Endpoint server = established(false);
        CollectionSpace space = CollectionSpace.of(2, 8);
        byte[] first = Frames.request(REQUEST_TYPE, 1, new byte[] {1});
        byte[] second = Frames.request(REQUEST_TYPE, 3, new byte[] {2});
        assertThat(server.receiveUnit(first, 0, first.length, space).consumed()).isEqualTo(first.length);

        ReceiveProgress stopped = server.receiveUnit(second, 0, second.length, space);

        assertThat(stopped.collectionRequired()).isTrue();
        assertThat(stopped.consumed()).isZero();
        assertThat(server.drainEvents()).hasSize(1);
        ReceiveProgress resumed = server.receiveUnit(second, 0, second.length, space);
        assertThat(resumed.consumed()).isEqualTo(second.length);
        assertThat(server.drainEvents()).hasSize(1);
    }

    @Test
    void unsupportedInputAlsoStopsWhenCollectionIsNeeded() {
        Endpoint server = established(false);
        CollectionSpace space = CollectionSpace.of(2, 8);
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[0]);
        server.receiveUnit(request, 0, request.length, space);

        ReceiveProgress stopped = server.receiveUnsupportedInput(space);

        assertThat(stopped.collectionRequired()).isTrue();
        assertThat(server.drainEvents()).hasSize(1);
        assertThat(server.receiveUnsupportedInput(space).collectionRequired()).isFalse();
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    // ---- 호출 규약 ----

    @Test
    void anEndedConnectionConsumesNothing() {
        Endpoint server = established(false);
        server.disconnect();
        server.drainEvents();

        ReceiveProgress progress = unit(server, Frames.preface());
        ReceiveProgress unsupported = server.receiveUnsupportedInput(SPACE);

        assertThat(progress.consumed()).isZero();
        assertThat(unsupported.collectionRequired()).isFalse();
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void invalidRangesAreRejected() {
        Endpoint server = established(false);

        assertThatIllegalArgumentException().isThrownBy(() -> server.receiveUnit(new byte[2], 1, 2, SPACE));
        assertThatIllegalArgumentException().isThrownBy(() -> server.receiveUnit(new byte[2], -1, 1, SPACE));
    }

    @Test
    void aUnitCannotFollowBytesLeftOverFromAStreamInput() {
        Endpoint server = established(false);
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[0]);
        server.receive(Arrays.copyOf(request, 5));

        assertThatIllegalStateException().isThrownBy(() -> unit(server, request));
    }
}
