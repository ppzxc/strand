package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 쓰기를 시작했지만 마지막 바이트를 넘기지 못한 요청의 결과 확정성(규격 3.5절). 요청한 쪽이 받는 확정성과, 남은 출력을 상대 엔드포인트에 넣었을 때
 * 상대가 요청을 받는지를 함께 본다.
 */
class EndpointPartialRequestCertaintyTest {
    @Test
    void errorCloseAfterPartialProgressIsUnknownAndPeerReceivesTheRequest() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        EndpointOutput request = client.nextOutput();
        int firstPart = 5;
        deliver(client, server, request, 0, firstPart);

        client.close(ErrorCode.INTERNAL_ERROR);

        List<Event> clientEvents = client.drainEvents();
        assertThat(clientEvents).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(clientEvents.get(0).certainty()).hasValue(Certainty.UNKNOWN);

        // 전송 모듈은 남은 출력(요청의 나머지와 CLOSE)을 마저 쓴다.
        deliver(client, server, request, firstPart, request.length() - firstPart);
        pump(client, server);

        assertThat(server.drainEvents()).extracting(Event::kind).contains(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void disconnectAfterPartialProgressStaysNotProcessed() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        EndpointOutput request = client.nextOutput();
        deliver(client, server, request, 0, 5);

        client.disconnect();

        List<Event> events = client.drainEvents();
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(events.get(0).certainty()).hasValue(Certainty.NOT_PROCESSED);
    }

    @Test
    void errorCloseWithoutAnyWrittenByteIsNotProcessedAndFrameIsNotSent() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        client.close(ErrorCode.INTERNAL_ERROR);

        List<Event> events = client.drainEvents();
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(events.get(0).certainty()).hasValue(Certainty.NOT_PROCESSED);
        pump(client, server);
        assertThat(server.drainEvents()).extracting(Event::kind).doesNotContain(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void errorCloseAfterFullHandoffIsUnknownExactlyOnce() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        connect(client, server);

        client.request(257, new byte[] {1, 2, 3});
        EndpointOutput request = client.nextOutput();
        deliver(client, server, request, 0, request.length());

        client.close(ErrorCode.INTERNAL_ERROR);
        client.disconnect();

        List<Event> events = client.drainEvents();
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
        assertThat(events.get(0).certainty()).hasValue(Certainty.UNKNOWN);
    }

    /** 두 엔드포인트가 서로의 출력을 끝까지 넘겨 연결을 연다. */
    private static void connect(Endpoint client, Endpoint server) {
        pump(client, server);
        client.drainEvents();
        server.drainEvents();
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

    /** 출력의 {@code offset}부터 {@code count} 바이트를 쓴 것으로 보고하고 그 바이트를 상대에 넣는다. */
    private static void deliver(Endpoint from, Endpoint to, EndpointOutput output, int offset, int count) {
        from.outputProgress(output.id(), count);
        to.receive(java.util.Arrays.copyOfRange(output.bytes(), offset, offset + count));
    }
}
