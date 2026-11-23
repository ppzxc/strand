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
    private static final CollectionSpace SPACE = EndpointDriver.ROOMY;

    private static Endpoint server() {
        return Drive.server(EndpointConfig.defaults());
    }

    private static ReceiveProgress unit(Endpoint endpoint, byte[] bytes) {
        ReceiveProgress progress = endpoint.receiveUnit(bytes, 0, bytes.length, SPACE);
        Drive.settle(endpoint);
        return progress;
    }

    private static Endpoint established() {
        Endpoint server = server();
        unit(server, Frames.preface());
        unit(server, Frames.hello(EndpointConfig.defaults()));
        unit(server, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        unit(server, Frames.auth("ANONYMOUS", new byte[0]));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        return server;
    }

    private static List<Event.Kind> kinds(Endpoint endpoint) {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : Drive.drainEvents(endpoint)) {
            kinds.add(event.kind());
        }
        return kinds;
    }

    private static boolean sentClose(Endpoint endpoint) {
        for (OutboundMessage message : Drive.drainMessages(endpoint)) {
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
        Endpoint server = server();

        ReceiveProgress progress = unit(server, Frames.preface());

        assertThat(progress.consumed()).isEqualTo(Frames.preface().length);
        assertThat(progress.collectionRequired()).isFalse();
        assertThat(Drive.drainMessages(server)).hasSize(1);
    }

    @Test
    void rightSizedUnitsHaveTheSameEventsAndOutputAsTheSameBytesAsAStream() {
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1, 2, 3});
        Endpoint byUnit = established();
        Endpoint byStream = established();

        unit(byUnit, request);
        Drive.receive(byStream, request);

        List<Event> unitEvents = Drive.drainEvents(byUnit);
        List<Event> streamEvents = Drive.drainEvents(byStream);
        assertThat(unitEvents).hasSize(1);
        assertThat(unitEvents.get(0).kind()).isEqualTo(streamEvents.get(0).kind());
        assertThat(unitEvents.get(0).body()).containsExactly(1, 2, 3);
        assertThat(Drive.drainMessages(byUnit)).hasSameSizeAs(Drive.drainMessages(byStream));
    }

    @Test
    void aUnitReadsOnlyItsRangeOfTheArray() {
        Endpoint server = established();
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
            Endpoint server = server();

            unit(server, bad);

            assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
            assertThat(Drive.drainMessages(server)).isEmpty();
        }
    }

    // ---- 첫 프레임 단계 ----

    @Test
    void aFirstFrameUnitTooShortToReadItsHeaderClosesSilently() {
        byte[] hello = Frames.hello(EndpointConfig.defaults());
        Endpoint server = server();
        unit(server, Frames.preface());
        Drive.drainMessages(server);

        unit(server, Arrays.copyOf(hello, Frames.HEADER_SIZE - 1));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(Drive.drainMessages(server)).isEmpty();
    }

    @Test
    void aHelloUnitOfTheWrongSizeIsAProtocolErrorOnceTheFirstFrameCheckPassed() {
        byte[] hello = Frames.hello(EndpointConfig.defaults());
        for (byte[] bad : new byte[][] {withLength(hello, 1), Arrays.copyOf(hello, hello.length - 1)}) {
            Endpoint server = server();
            unit(server, Frames.preface());
            Drive.drainMessages(server);

            unit(server, bad);

            assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
            assertThat(sentClose(server)).isTrue();
        }
    }

    @Test
    void aFirstFrameThatIsNotHelloClosesSilently() {
        Endpoint server = server();
        unit(server, Frames.preface());
        Drive.drainMessages(server);

        unit(server, Frames.request(REQUEST_TYPE, 1, new byte[0]));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(Drive.drainMessages(server)).isEmpty();
    }

    // ---- 그 뒤 ----

    @Test
    void aLaterUnitOfTheWrongSizeIsAProtocolErrorAndTheFrameIsNotApplied() {
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[] {1});
        for (byte[] bad : new byte[][] {
            withLength(request, 1), Arrays.copyOf(request, request.length - 1), Arrays.copyOf(request, 2)
        }) {
            Endpoint server = established();

            unit(server, bad);

            assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
            assertThat(sentClose(server)).isTrue();
        }
    }

    @Test
    void aWrongSizedCloseUnitClosesWithoutSendingClose() {
        byte[] close = Frames.control(FrameType.CLOSE, 0, new byte[12]);
        Endpoint server = established();

        unit(server, withLength(close, 3));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(sentClose(server)).isFalse();
    }

    // ---- 바이트로 넘길 수 없는 입력 ----

    @Test
    void unsupportedInputInThePrefaceAndFirstFramePhasesClosesSilently() {
        Endpoint atPreface = server();
        atPreface.receiveUnsupportedInput(SPACE);
        assertThat(kinds(atPreface)).containsExactly(Event.Kind.CLOSED);
        assertThat(Drive.drainMessages(atPreface)).isEmpty();

        Endpoint atFirstFrame = server();
        unit(atFirstFrame, Frames.preface());
        Drive.drainMessages(atFirstFrame);
        atFirstFrame.receiveUnsupportedInput(SPACE);
        assertThat(kinds(atFirstFrame)).containsExactly(Event.Kind.CLOSED);
        assertThat(Drive.drainMessages(atFirstFrame)).isEmpty();
    }

    @Test
    void unsupportedInputAfterTheFirstFrameIsAProtocolError() {
        Endpoint server = established();

        server.receiveUnsupportedInput(SPACE);

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(sentClose(server)).isTrue();
    }

    @Test
    void aUnitAboveTheMaxFrameSizeIsAProtocolError() {
        Endpoint server = Drive.server(EndpointConfig.defaults().maxFrameSize(16384));
        unit(server, Frames.preface());
        unit(server, Frames.hello(EndpointConfig.defaults()));
        unit(server, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        unit(server, Frames.auth("ANONYMOUS", new byte[0]));
        Drive.drainMessages(server);
        Drive.drainEvents(server);

        unit(server, Frames.request(REQUEST_TYPE, 1, new byte[16384]));

        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
        assertThat(sentClose(server)).isTrue();
    }

    @Test
    void unsupportedInputAsAnAlreadySentOrReceivedCloseSendsNoSecondClose() {
        Endpoint closing = established();
        closing.close(ErrorCode.NO_ERROR);
        Drive.drainMessages(closing);
        Drive.drainEvents(closing);

        closing.receiveUnsupportedInput(SPACE);

        assertThat(Drive.drainMessages(closing)).isEmpty();
    }

    @Test
    void aClientAlsoClosesSilentlyOnUnsupportedFirstInput() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Drive.drainMessages(client);

        client.receiveUnsupportedInput(SPACE);

        assertThat(kinds(client)).containsExactly(Event.Kind.CLOSED);
        assertThat(Drive.drainMessages(client)).isEmpty();
    }

    // ---- 수거 공간 계약 ----

    @Test
    void stopsWithoutConsumingWhenCollectionIsNeededAndResumesWithTheSameUnit() {
        Endpoint server = established();
        CollectionSpace space = CollectionSpace.of(2, 8);
        byte[] first = Frames.request(REQUEST_TYPE, 1, new byte[] {1});
        byte[] second = Frames.request(REQUEST_TYPE, 3, new byte[] {2});
        assertThat(server.receiveUnit(first, 0, first.length, space).consumed()).isEqualTo(first.length);

        ReceiveProgress stopped = server.receiveUnit(second, 0, second.length, space);

        assertThat(stopped.collectionRequired()).isTrue();
        assertThat(stopped.consumed()).isZero();
        assertThat(Drive.drainEvents(server)).hasSize(1);
        ReceiveProgress resumed = server.receiveUnit(second, 0, second.length, space);
        assertThat(resumed.consumed()).isEqualTo(second.length);
        assertThat(Drive.drainEvents(server)).hasSize(1);
    }

    @Test
    void unsupportedInputAlsoStopsWhenCollectionIsNeeded() {
        Endpoint server = established();
        CollectionSpace space = CollectionSpace.of(2, 8);
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[0]);
        server.receiveUnit(request, 0, request.length, space);

        ReceiveProgress stopped = server.receiveUnsupportedInput(space);

        assertThat(stopped.collectionRequired()).isTrue();
        assertThat(Drive.drainEvents(server)).hasSize(1);
        assertThat(server.receiveUnsupportedInput(space).collectionRequired()).isFalse();
        assertThat(kinds(server)).containsExactly(Event.Kind.CLOSED);
    }

    // ---- 호출 규약 ----

    @Test
    void anEndedConnectionConsumesNothing() {
        Endpoint server = established();
        server.disconnect();
        Drive.drainEvents(server);

        ReceiveProgress progress = unit(server, Frames.preface());
        ReceiveProgress unsupported = server.receiveUnsupportedInput(SPACE);

        assertThat(progress.consumed()).isZero();
        assertThat(unsupported.collectionRequired()).isFalse();
        assertThat(Drive.drainEvents(server)).isEmpty();
    }

    @Test
    void invalidRangesAreRejected() {
        Endpoint server = established();

        assertThatIllegalArgumentException().isThrownBy(() -> server.receiveUnit(new byte[2], 1, 2, SPACE));
        assertThatIllegalArgumentException().isThrownBy(() -> server.receiveUnit(new byte[2], -1, 1, SPACE));
    }

    @Test
    void aUnitCannotFollowBytesLeftOverFromAStreamInput() {
        Endpoint server = established();
        byte[] request = Frames.request(REQUEST_TYPE, 1, new byte[0]);
        Drive.receive(server, Arrays.copyOf(request, 5));

        assertThatIllegalStateException().isThrownBy(() -> unit(server, request));
    }
}
