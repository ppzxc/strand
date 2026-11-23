package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.List;
import org.junit.jupiter.api.Test;

/** WebSocket 출력: 전달 전 철회와 메시지 전체 접수의 구별(이슈 #308 결정, #321). */
class EndpointWebSocketOutputTest {

    private static final int REQUEST_TYPE = 257;

    private static Endpoint establishedClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().webSocket());
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.hello(EndpointConfig.defaults()));
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.authAck(java.util.OptionalLong.empty()));
        while (true) {
            EndpointOutput output = client.nextOutput();
            if (output == null) {
                break;
            }
            client.outputAccepted(output.id());
        }
        Drive.drainEvents(client);
        return client;
    }

    private static Event onlyResult(Endpoint endpoint) {
        List<Event> events = Drive.drainEvents(endpoint);
        assertThat(events).filteredOn(e -> e.kind() == Event.Kind.RESULT).hasSize(1);
        return events.get(0);
    }

    @Test
    void takingTheNextOutputAloneDoesNotStartDelivery() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        assertThat(client.nextOutput()).isNotNull();

        client.webSocketClosed(1006);

        Event result = onlyResult(client);
        assertThat(result.certainty()).hasValue(Certainty.NOT_PROCESSED);
    }

    @Test
    void cancellationBeforeAcceptanceWithdrawsTheMessageWithoutReset() {
        Endpoint client = establishedClient();
        long ticket = client.request(REQUEST_TYPE, new byte[0]);
        EndpointOutput request = client.nextOutput();

        client.cancelRequest(ticket);

        assertThat(client.nextOutput()).isNull();
        assertThatIllegalArgumentException().isThrownBy(() -> client.outputAccepted(request.id()));
        Event result = onlyResult(client);
        assertThat(result.certainty()).hasValue(Certainty.NOT_PROCESSED);
        assertThat(result.status()).isEqualTo(Status.CANCELLED);
    }

    @Test
    void cancellationAfterAcceptanceKeepsTheMessageAndSendsResetAfterIt() {
        Endpoint client = establishedClient();
        long ticket = client.request(REQUEST_TYPE, new byte[0]);
        EndpointOutput request = client.nextOutput();
        client.outputAccepted(request.id());

        client.cancelRequest(ticket);

        EndpointOutput reset = client.nextOutput();
        assertThat(reset.bytes()).isEqualTo(Frames.reset(1, ErrorCode.CANCEL));
        client.outputAccepted(reset.id());
        assertThat(client.nextOutput()).isNull();
        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.CANCELLED);
        client.cancelRequest(ticket);
        assertThat(Drive.drainEvents(client)).isEmpty();
    }

    @Test
    void deadlineBeforeAcceptanceWithdrawsTheMessage() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0], 100);
        client.nextOutput();

        Drive.advance(client, 101);

        assertThat(client.nextOutput()).isNull();
        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(result.certainty()).hasValue(Certainty.NOT_PROCESSED);
    }

    @Test
    void deadlineAfterAcceptanceSendsResetAndConfirmsTheResultOnce() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0], 100);
        client.outputAccepted(client.nextOutput().id());

        Drive.advance(client, 101);

        assertThat(client.nextOutput().bytes()).isEqualTo(Frames.reset(1, ErrorCode.CANCEL));
        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        Drive.advance(client, 1000);
        assertThat(Drive.drainEvents(client)).isEmpty();
    }

    @Test
    void disconnectionBeforeAcceptanceIsNotProcessed() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        client.nextOutput();

        client.disconnect();

        assertThat(onlyResult(client).certainty()).hasValue(Certainty.NOT_PROCESSED);
        assertThat(client.nextOutput()).isNull();
    }

    @Test
    void disconnectionAfterAcceptanceEvenBeforeAnySocketWriteIsUnknown() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        client.outputAccepted(client.nextOutput().id());

        client.disconnect();

        Event result = onlyResult(client);
        assertThat(result.certainty()).hasValue(Certainty.UNKNOWN);
    }

    @Test
    void aLateTransportFailureDoesNotChangeAnAlreadyConfirmedResult() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        client.outputAccepted(client.nextOutput().id());
        client.receiveMessage(WebSocketMessageKind.BINARY, Frames.response(REQUEST_TYPE, 1, 0));
        List<Event> events = Drive.drainEvents(client);
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT);

        client.disconnect();

        assertThat(Drive.drainEvents(client)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void messagesAreAcceptedWholeAndPartialProgressIsRejected() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        EndpointOutput request = client.nextOutput();

        assertThatIllegalStateException().isThrownBy(() -> client.outputProgress(request.id(), 1));
        assertThatIllegalStateException().isThrownBy(() -> client.outputProgress(request.id(), request.length()));
        client.outputProgress(request.id(), 0);

        assertThat(client.nextOutput().id()).isEqualTo(request.id());
    }

    @Test
    void acceptanceMovesToTheNextOutputAndRejectsStaleOrRepeatedIdentifiers() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        client.request(REQUEST_TYPE, new byte[0]);
        EndpointOutput first = client.nextOutput();

        client.outputAccepted(first.id());

        EndpointOutput second = client.nextOutput();
        assertThat(second.id()).isNotEqualTo(first.id());
        assertThatIllegalArgumentException().isThrownBy(() -> client.outputAccepted(first.id()));
        assertThatIllegalArgumentException().isThrownBy(() -> client.outputAccepted(first.id() + 100));
        client.outputAccepted(second.id());
        assertThat(client.nextOutput()).isNull();
    }

    @Test
    void messageAcceptanceIsOnlyForWebSocketEndpoints() {
        Endpoint stream = Endpoint.client(EndpointConfig.defaults());
        EndpointOutput preface = stream.nextOutput();

        assertThatIllegalStateException().isThrownBy(() -> stream.outputAccepted(preface.id()));
    }

    @Test
    void higherPriorityOutputBeforeAcceptanceGetsANewIdentifier() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, new byte[0]);
        EndpointOutput request = client.nextOutput();
        client.ping(new byte[8]);

        EndpointOutput ping = client.nextOutput();

        assertThat(ping.id()).isNotEqualTo(request.id());
        assertThatIllegalArgumentException().isThrownBy(() -> client.outputAccepted(request.id()));
    }

    @Test
    void acknowledgementWaitStartsWhenTheMessageIsAcceptedNotWhenItIsTaken() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().webSocket());
        client.outputAccepted(client.nextOutput().id());
        EndpointOutput hello = client.nextOutput();
        Drive.advance(client, 6000);
        assertThat(Drive.drainEvents(client)).isEmpty();

        client.outputAccepted(hello.id());
        Drive.advance(client, 5001);

        assertThat(Drive.drainEvents(client)).extracting(Event::kind).contains(Event.Kind.CLOSED);
    }
}
