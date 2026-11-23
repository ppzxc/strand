package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EndpointOutputProgressTest {
    @Test
    void partiallyDeliveredRequestIsNotCountedAsFullySentForPeerClose() {
        Endpoint server = openServer();
        server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), 1);
        Drive.receive(server, Frames.close(ErrorCode.NO_ERROR, 2, new byte[0]));
        assertThat(Drive.drainEvents(server))
                .last()
                .satisfies(event -> assertThat(event.closeReason())
                        .get()
                        .extracting(CloseReason::kind)
                        .isEqualTo(CloseReason.Kind.ERROR_CLOSE));
    }

    @Test
    void cancelledPartiallyWrittenRequestStillCountsOnceFullyDelivered() {
        Endpoint server = openServer();
        long ticket = server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), 1);
        server.cancelRequest(ticket);
        Drive.drainEvents(server);
        server.outputProgress(request.id(), request.bytes().length - 1);
        Drive.receive(server, Frames.close(ErrorCode.NO_ERROR, 2, new byte[0]));
        assertThat(Drive.drainEvents(server)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void acknowledgementTimeoutStartsAfterFullOutputDelivery() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        EndpointOutput preface = client.nextOutput();
        client.outputProgress(preface.id(), preface.bytes().length);
        EndpointOutput hello = client.nextOutput();
        client.outputProgress(hello.id(), 1);
        Drive.advance(client, 6000);
        assertThat(Drive.drainEvents(client)).isEmpty();
        client.outputProgress(hello.id(), hello.bytes().length - 1);
        Drive.advance(client, 5001);
        assertThat(Drive.drainEvents(client)).extracting(Event::kind).contains(Event.Kind.CLOSED);
    }

    private Endpoint openServer() {
        Endpoint server = Drive.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        Drive.drainEvents(server);
        Drive.drainMessages(server);
        return server;
    }

    @Test
    void partialRequestRemainsAheadOfCancellationReset() {
        Endpoint server = openServer();
        long ticket = server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), 1);
        server.cancelRequest(ticket);
        assertThat(server.nextOutput().id()).isEqualTo(request.id());
        server.outputProgress(request.id(), request.bytes().length - 1);
        assertThat(server.nextOutput().bytes()).isEqualTo(Frames.reset(2, ErrorCode.CANCEL));
    }

    @Test
    void zeroProgressDoesNotPreventWithdrawal() {
        Endpoint server = openServer();
        long ticket = server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), 0);
        server.cancelRequest(ticket);
        assertThat(server.nextOutput()).isNull();
    }

    @Test
    void disconnectBeforeFullDeliveryIsNotProcessed() {
        Endpoint server = openServer();
        server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), 1);
        server.disconnect();
        assertThat(Drive.drainEvents(server))
                .first()
                .satisfies(e -> assertThat(e.certainty()).hasValue(Certainty.NOT_PROCESSED));
        assertThat(server.nextOutput()).isNull();
    }

    @Test
    void disconnectAfterFullDeliveryIsUnknown() {
        Endpoint server = openServer();
        server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), request.bytes().length);
        server.disconnect();
        assertThat(Drive.drainEvents(server))
                .first()
                .satisfies(e -> assertThat(e.certainty()).hasValue(Certainty.UNKNOWN));
    }

    @Test
    void zeroProgressKeepsOutputAndFullProgressMovesToNext() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        EndpointOutput preface = client.nextOutput();
        assertThat(preface.bytes()).isEqualTo(EndpointTest.bytes("c0de535452300000"));
        client.outputProgress(preface.id(), 0);
        assertThat(client.nextOutput().id()).isEqualTo(preface.id());
        client.outputProgress(preface.id(), preface.bytes().length);
        assertThat(client.nextOutput().id()).isNotEqualTo(preface.id());
    }

    @Test
    void duplicateOrLateRequestCompletionDoesNotProduceASecondResult() {
        Endpoint server = openServer();
        long ticket = server.request(257, new byte[0]);
        Drive.drainMessages(server);
        Drive.receive(server, Frames.response(257, 2, 0));
        assertThat(Drive.drainEvents(server)).extracting(Event::kind).containsExactly(Event.Kind.RESULT);

        Drive.receive(server, Frames.response(257, 2, 0));
        server.cancelRequest(ticket);

        assertThat(Drive.drainEvents(server)).isEmpty();
    }

    @Test
    void staleOutputIdentifierIsRejectedAfterWithdrawal() {
        Endpoint server = openServer();
        long ticket = server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.cancelRequest(ticket);

        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> server.outputProgress(request.id(), 1));
    }

    @Test
    void higherPriorityOutputBeforeAnyProgressGetsANewIdentifier() {
        Endpoint server = openServer();
        server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        EndpointOutput next = server.nextOutput();

        assertThat(next.id()).isNotEqualTo(request.id());
        assertThat(next.bytes()[5]).isEqualTo((byte) 2);
        org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
                .isThrownBy(() -> server.outputProgress(request.id(), 1));
    }
}
