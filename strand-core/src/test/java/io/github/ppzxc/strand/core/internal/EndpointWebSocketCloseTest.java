package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** Strand CLOSE, WebSocket Close, 전송 종료 지시의 구별과 요청 결과 보존 순서(이슈 #308 결정, #321). */
class EndpointWebSocketCloseTest {

    private static final int REQUEST_TYPE = 257;

    private static Endpoint establishedClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().webSocket());
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.hello(EndpointConfig.defaults()));
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.authAck(OptionalLong.empty()));
        acceptAll(client);
        Drive.drainEvents(client);
        return client;
    }

    private static List<OutboundMessage> acceptAll(Endpoint endpoint) {
        List<OutboundMessage> accepted = new ArrayList<>();
        for (EndpointOutput output = endpoint.nextOutput(); output != null; output = endpoint.nextOutput()) {
            accepted.add(output.message());
            endpoint.outputAccepted(output.id());
        }
        return accepted;
    }

    private static List<Event.Kind> kinds(List<Event> events) {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : events) {
            kinds.add(event.kind());
        }
        return kinds;
    }

    // ---- 받은 WebSocket Close ----

    @Test
    void receivedWebSocketCloseNeverCreatesAStrandCloseOrLastStreamId() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        acceptAll(client);

        client.webSocketClosed(1000);

        assertThat(client.nextOutput()).isNull();
        List<Event> events = Drive.drainEvents(client);
        assertThat(kinds(events)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        CloseReason reason = events.get(1).closeReason().get();
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
        assertThat(reason.sender()).isEmpty();
        assertThat(reason.errorCode()).isZero();
    }

    @Test
    void receivedWebSocketClose1000ResolvesPendingRequestsByWhetherTheMessageWasAccepted() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        acceptAll(client);
        client.request(REQUEST_TYPE, new byte[0]);

        client.webSocketClosed(1000);

        List<Event> events = Drive.drainEvents(client);
        assertThat(kinds(events)).containsExactly(Event.Kind.RESULT, Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(events.get(0).certainty()).hasValue(Certainty.UNKNOWN);
        assertThat(events.get(1).certainty()).hasValue(Certainty.NOT_PROCESSED);
    }

    @Test
    void anAlreadyConfirmedResultIsKeptAndNotDuplicatedByWebSocketClose() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        acceptAll(client);
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.response(REQUEST_TYPE, 1, 0));

        client.webSocketClosed(1000);

        assertThat(kinds(Drive.drainEvents(client))).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void webSocketCloseAfterAStrandCloseExchangeAddsNothing() {
        Endpoint client = establishedClient();
        client.close(ErrorCode.NO_ERROR);
        acceptAll(client);
        Drive.drainEvents(client);

        client.webSocketClosed(1000);

        assertThat(Drive.drainEvents(client)).isEmpty();
    }

    @Test
    void webSocketClosedIsOnlyForWebSocketEndpoints() {
        Endpoint stream = Endpoint.client(EndpointConfig.defaults());

        assertThatIllegalStateException().isThrownBy(() -> stream.webSocketClosed(1000));
    }

    @Test
    void writeFailureOrAbortWhileClosingDoesNotWaitForAnUndeliverableClose() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        acceptAll(client);
        client.close(ErrorCode.NO_ERROR);
        assertThat(client.transportClose()).isEqualTo(Endpoint.TransportClose.NONE);

        client.disconnect();

        assertThat(client.nextOutput()).isNull();
        assertThat(kinds(Drive.drainEvents(client))).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(client.transportClose()).isEqualTo(Endpoint.TransportClose.IMMEDIATE);
    }

    // ---- 로컬 종료: Strand CLOSE 다음 WebSocket Close ----

    @Test
    void localNoErrorCloseSendsTheStrandCloseThenWebSocketClose1000() {
        Endpoint client = establishedClient();

        client.close(ErrorCode.NO_ERROR);

        assertThat(client.transportClose()).isEqualTo(Endpoint.TransportClose.AFTER_PENDING_OUTPUT);
        List<OutboundMessage> sent = acceptAll(client);
        assertThat(sent).hasSize(2);
        assertThat(sent.get(0).kind()).isEqualTo(OutboundMessage.Kind.BINARY);
        assertThat(Frames.typeOf(sent.get(0).bytes(), 0)).isEqualTo(FrameType.CLOSE);
        assertThat(sent.get(1).kind()).isEqualTo(OutboundMessage.Kind.WEBSOCKET_CLOSE);
        assertThat(sent.get(1).webSocketCloseCode()).isEqualTo(1000);
        assertThat(client.transportClose()).isEqualTo(Endpoint.TransportClose.IMMEDIATE);
    }

    @Test
    void errorCloseSendsTheStrandCloseWithoutWebSocketClose1000() {
        Endpoint client = establishedClient();

        client.close(ErrorCode.PROTOCOL_ERROR);

        List<OutboundMessage> sent = acceptAll(client);
        assertThat(sent).hasSize(1);
        assertThat(Frames.typeOf(sent.get(0).bytes(), 0)).isEqualTo(FrameType.CLOSE);
    }

    @Test
    void resultsOfPendingRequestsArePreservedBeforeTheClosedEvent() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        acceptAll(client);
        client.close(ErrorCode.PROTOCOL_ERROR);

        assertThat(kinds(Drive.drainEvents(client))).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void receivedStrandCloseGivesAfterPendingOutputThenWebSocketClose() {
        Endpoint client = establishedClient();

        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.close(ErrorCode.NO_ERROR, 0));

        List<OutboundMessage> sent = acceptAll(client);
        assertThat(sent).extracting(OutboundMessage::kind).contains(OutboundMessage.Kind.WEBSOCKET_CLOSE);
        assertThat(Drive.drainEvents(client)).extracting(Event::kind).contains(Event.Kind.CLOSED);
    }

    // ---- 종료 지시 ----

    @Test
    void anOpenConnectionHasNoTransportCloseInstruction() {
        assertThat(establishedClient().transportClose()).isEqualTo(Endpoint.TransportClose.NONE);
    }

    @Test
    void aViolatedPrefaceIsAnImmediateDisconnectWithNothingToSend() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().webSocket());

        server.receiveMessage(WebSocketMessageKind.TEXT, Frames.preface());

        assertThat(server.transportClose()).isEqualTo(Endpoint.TransportClose.IMMEDIATE);
        assertThat(server.nextOutput()).isNull();
    }

    @Test
    void aProtocolErrorWaitsForTheStrandCloseToBeAcceptedBeforeTheInstructionIsImmediate() {
        Endpoint client = establishedClient();

        client.receiveMessage(WebSocketMessageKind.TEXT, new byte[] {0});

        assertThat(client.transportClose()).isEqualTo(Endpoint.TransportClose.AFTER_PENDING_OUTPUT);
        acceptAll(client);
        assertThat(client.transportClose()).isEqualTo(Endpoint.TransportClose.IMMEDIATE);
    }

    @Test
    void aReceivedWebSocketCloseIsAnImmediateInstructionBecauseTheTransportAnswersIt() {
        Endpoint client = establishedClient();

        client.webSocketClosed(1000);

        assertThat(client.transportClose()).isEqualTo(Endpoint.TransportClose.IMMEDIATE);
    }
}
