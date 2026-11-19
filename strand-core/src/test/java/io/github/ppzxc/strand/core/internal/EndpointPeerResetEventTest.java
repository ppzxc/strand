package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * 상대가 받은 요청을 RESET하면 그 사실이 오류 코드와 함께 애플리케이션 사건으로 나온다(규격 4.4절 "받은 쪽은 할 수 있으면 처리를 멈추고"). 응답을 이미
 * 보낸 스트림, 거절한 스트림, 대기 중이 아닌 스트림의 RESET은 사건이 아니다(규격 4.3절).
 */
class EndpointPeerResetEventTest {

    private static final int REQUEST_TYPE = 0x0100;
    private static final byte[] NO_BYTES = new byte[0];

    private static Endpoint establishedServer() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().knownTypes(Collections.singleton(REQUEST_TYPE)));
        server.receive(Frames.preface());
        server.receive(Frames.hello(EndpointConfig.defaults()));
        server.receive(Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        server.receive(Frames.auth("ANONYMOUS", NO_BYTES));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static void receiveRequest(Endpoint server, long streamId) {
        server.receive(Frames.request(REQUEST_TYPE, streamId, NO_BYTES));
    }

    private static List<Event.Kind> kinds(List<Event> events) {
        java.util.ArrayList<Event.Kind> kinds = new java.util.ArrayList<>();
        events.forEach(event -> kinds.add(event.kind()));
        return kinds;
    }

    @Test
    void resetOfADeliveredRequestIsReportedWithItsStreamAndErrorCode() {
        Endpoint server = establishedServer();
        receiveRequest(server, 1);
        server.drainEvents();

        server.receive(Frames.reset(1, 4));

        List<Event> events = server.drainEvents();
        assertThat(events).singleElement().satisfies(event -> {
            assertThat(event.kind()).isEqualTo(Event.Kind.REQUEST_RESET);
            assertThat(event.streamId()).isEqualTo(1);
            assertThat(event.resetErrorCode()).isEqualTo(OptionalLong.of(4));
        });
        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void theErrorCodeIsKeptAsReceivedEvenWhenItIsReservedOrCloseOnly() {
        for (long code : new long[] {0, 1, 5, 7, 8, 0xffffffffL}) {
            Endpoint server = establishedServer();
            receiveRequest(server, 1);
            server.drainEvents();

            server.receive(Frames.reset(1, (int) code));

            assertThat(server.drainEvents())
                    .singleElement()
                    .satisfies(event -> assertThat(event.resetErrorCode()).isEqualTo(OptionalLong.of(code)));
        }
    }

    @Test
    void aResetThatArrivesBeforeTheRequestWasDrainedFollowsItsRequestInOrder() {
        Endpoint server = establishedServer();
        receiveRequest(server, 1);

        server.receive(Frames.reset(1, 4));

        assertThat(kinds(server.drainEvents())).containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.REQUEST_RESET);
    }

    @Test
    void anAnsweredStreamAndAnUnknownStreamAreNotReported() {
        Endpoint server = establishedServer();
        receiveRequest(server, 1);
        server.drainEvents();
        server.respond(1, 0, NO_BYTES);

        server.receive(Frames.reset(1, 4));
        server.receive(Frames.reset(3, 4));

        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void aRefusedRequestIsNotReportedWhenThePeerResetsItToo() {
        Endpoint server = establishedServer();
        receiveRequest(server, 1);
        server.drainEvents();
        server.refuseRequest(1);

        server.receive(Frames.reset(1, 4));

        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void theBoundedDrainKeepsTheOrderOfARequestAndItsReset() {
        Endpoint server = establishedServer();
        receiveRequest(server, 1);
        server.receive(Frames.reset(1, 4));

        List<Event> first = server.drainEvents(1);
        List<Event> second = server.drainEvents(1);

        assertThat(kinds(first)).containsExactly(Event.Kind.REQUEST_RECEIVED);
        assertThat(kinds(second)).containsExactly(Event.Kind.REQUEST_RESET);
    }
}
