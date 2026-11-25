package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class EndpointPartialRequestCertaintyTest {
    @Test
    void errorCloseAfterPartialProgressIsUnknownAndPeerReceivesTheRequest() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Drive.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        EndpointOutput request = client.nextOutput();
        int firstPart = 5;
        deliver(client, server, request, 0, firstPart);

        client.close(ErrorCode.INTERNAL_ERROR);

        List<Event> clientEvents = Drive.drainEvents(client);
        assertThat(clientEvents).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(clientEvents.get(0).certainty()).hasValue(Certainty.UNKNOWN);

        deliver(client, server, request, firstPart, request.length() - firstPart);
        pump(client, server);

        assertThat(Drive.drainEvents(server)).extracting(Event::kind).contains(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void disconnectAfterPartialProgressStaysNotProcessed() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Drive.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        EndpointOutput request = client.nextOutput();
        deliver(client, server, request, 0, 5);

        client.disconnect();

        List<Event> events = Drive.drainEvents(client);
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(events.get(0).certainty()).hasValue(Certainty.NOT_PROCESSED);
    }

    @Test
    void disconnectAfterGracefulCloseStartedStaysNotProcessedForPartialRequest() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Drive.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        EndpointOutput request = client.nextOutput();
        deliver(client, server, request, 0, 5);

        client.close(ErrorCode.NO_ERROR);
        client.disconnect();

        List<Event> events = Drive.drainEvents(client);
        assertThat(events).extracting(Event::kind).contains(Event.Kind.RESULT);
        assertThat(events.get(0).certainty()).hasValue(Certainty.NOT_PROCESSED);
    }

    @Test
    void errorCloseWithoutAnyWrittenByteIsNotProcessedAndFrameIsNotSent() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Drive.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        client.close(ErrorCode.INTERNAL_ERROR);

        List<Event> events = Drive.drainEvents(client);
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(events.get(0).certainty()).hasValue(Certainty.NOT_PROCESSED);
        pump(client, server);
        assertThat(Drive.drainEvents(server)).extracting(Event::kind).doesNotContain(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void errorCloseAfterFullHandoffIsUnknownExactlyOnce() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Drive.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        EndpointOutput request = client.nextOutput();
        deliver(client, server, request, 0, request.length());

        client.close(ErrorCode.INTERNAL_ERROR);
        client.disconnect();

        List<Event> events = Drive.drainEvents(client);
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(events.get(0).certainty()).hasValue(Certainty.UNKNOWN);
    }

    private static void connect(Endpoint client, Endpoint server) {
        pump(client, server);
        Drive.drainEvents(client);
        Drive.drainEvents(server);
    }

    private static void pump(Endpoint a, Endpoint b) {
        boolean moved = true;
        while (moved) {
            boolean forward = move(a, b);
            boolean backward = move(b, a);
            moved = forward || backward;
        }
    }

    private static boolean move(Endpoint from, Endpoint to) {
        boolean moved = false;
        EndpointOutput output;
        while ((output = from.nextOutput()) != null) {
            deliver(from, to, output, 0, output.length());
            moved = true;
        }
        return moved;
    }

    private static void deliver(Endpoint from, Endpoint to, EndpointOutput output, int offset, int count) {
        from.outputProgress(output.id(), count);
        Drive.receive(to, java.util.Arrays.copyOfRange(output.bytes(), offset, offset + count));
    }
}
