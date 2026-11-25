package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class EndpointUnsentIdlePingTest {

    private static final EndpointConfig CONFIG =
            EndpointConfig.defaults().idleIntervalMs(500).ackTimeoutMs(1000).authTimeoutMs(Long.MAX_VALUE / 4);

    @Test
    void noOutputProgressClosesUnresponsiveAfterTheAckWaitSinceTheIdlePingWasQueued() {
        Endpoint server = blockedWithIdlePingQueued();

        Drive.advance(server, 1000);
        assertThat(Drive.drainEvents(server)).isEmpty();
        Drive.advance(server, 1);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
        assertThat(sentFrameTypes(server)).containsExactly(FrameType.PING, FrameType.CLOSE);
    }

    @Test
    void theDeadlineIsPartOfTheNextDeadlineQuery() {
        Endpoint server = blockedWithIdlePingQueued();

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(1501));
        Drive.advance(server, 1001);
        assertThat(Drive.drainEvents(server)).isNotEmpty();
    }

    @Test
    void doesNotSendAnotherCloseWhenCloseWasAlreadySent() {
        Endpoint server = blockedWithIdlePingQueuedAndAPeerRequestPending();
        server.close(ErrorCode.NO_ERROR);
        Drive.drainEvents(server);

        Drive.advance(server, 1001);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
        assertThat(sentFrameTypes(server).stream().filter(t -> t == FrameType.CLOSE))
                .hasSize(1);
    }

    @Test
    void doesNotSendCloseWhenCloseWasAlreadyReceived() {
        Endpoint server = blockedWithIdlePingQueuedAndAPeerRequestPending();
        Drive.receive(server, Frames.close(ErrorCode.NO_ERROR, 0));
        Drive.drainEvents(server);

        Drive.advance(server, 1001);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
        assertThat(sentFrameTypes(server)).doesNotContain(FrameType.CLOSE);
    }

    @Test
    void anyOutputProgressCountsTheDeadlineAgain() {
        Endpoint server = blockedWithIdlePingQueued();
        Drive.advance(server, 900);
        EndpointOutput head = server.nextOutput();
        server.outputProgress(head.id(), 1);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(2401));
        Drive.advance(server, 1000);
        assertThat(Drive.drainEvents(server)).isEmpty();
        Drive.advance(server, 1);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
    }

    @Test
    void aBigFrameWrittenBitByBitIsNeverClosed() {
        Endpoint server = blockedWithIdlePingQueued();
        for (int i = 0; i < 20; i++) {
            Drive.advance(server, 900);
            EndpointOutput head = server.nextOutput();
            assertThat(head).isNotNull();
            server.outputProgress(head.id(), 1);
            assertThat(Drive.drainEvents(server)).isEmpty();
        }
    }

    @Test
    void theAckWaitOfAHandedOffIdlePingStartsAtHandOff() {
        Endpoint server = blockedWithIdlePingQueued();
        Drive.advance(server, 900);
        EndpointOutput ping = server.nextOutput();
        server.outputProgress(ping.id(), ping.bytes().length);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(2401));
        Drive.advance(server, 1000);
        assertThat(Drive.drainEvents(server)).isEmpty();
        Drive.advance(server, 1);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
    }

    @Test
    void aDrainedIdlePingKeepsTheAckWaitFromTheHandOff() {
        Endpoint server = Drive.server(CONFIG);
        Drive.receive(server, EndpointTest.bytes(PREFACE));
        Drive.receive(server, EndpointTest.bytes(HELLO));
        Drive.receive(server, EndpointTest.bytes(HELLO_ACK));
        Drive.receive(server, EndpointTest.bytes(AUTH_ANONYMOUS));
        Drive.drainEvents(server);
        Drive.drainMessages(server);
        Drive.advance(server, 500);
        Drive.drainMessages(server);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(1501));
        Drive.advance(server, 1001);
        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
    }

    @Test
    void anAckBeforeTheHandOffEndsTheRule() {
        Endpoint server = blockedWithIdlePingQueued();
        Drive.receive(server, Frames.control(FrameType.PING, 1, new byte[8]));
        Drive.drainEvents(server);

        Drive.advance(server, 5000);

        assertThat(Drive.drainEvents(server)).extracting(Event::kind).doesNotContain(Event.Kind.CLOSED);
    }

    @Test
    void anApplicationPingBlockedInTheQueueIsNotSubjectToTheRule() {
        Endpoint server =
                Drive.server(EndpointConfig.defaults().idleIntervalMs(0).ackTimeoutMs(1000));
        open(server);
        server.ping(new byte[8]);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.empty());
        Drive.advance(server, 100_000);

        assertThat(Drive.drainEvents(server)).isEmpty();
    }

    @Test
    void theBlockedHelloIsNotSubjectToTheRule() {
        Endpoint client =
                Endpoint.client(EndpointConfig.defaults().idleIntervalMs(500).ackTimeoutMs(1000));

        assertThat(client.nextDeadline()).isEqualTo(OptionalLong.empty());
        Drive.advance(client, 100_000);

        assertThat(Drive.drainEvents(client)).isEmpty();
    }

    private static final String PREFACE = "c0de535452300000";
    private static final String HELLO = "0000001000010000000000000000000000000000";
    private static final String HELLO_ACK = "0000001000010100000000000000000000000000";
    private static final String AUTH_ANONYMOUS = "0000001a0005000000000000000000000000000009414e4f4e594d4f5553";

    private static void open(Endpoint server) {
        Drive.receive(server, EndpointTest.bytes(PREFACE));
        Drive.receive(server, EndpointTest.bytes(HELLO));
        Drive.receive(server, EndpointTest.bytes(HELLO_ACK));
        Drive.receive(server, EndpointTest.bytes(AUTH_ANONYMOUS));
        Drive.drainEvents(server);
        EndpointOutput output;
        while ((output = server.nextOutput()) != null) {
            server.outputProgress(output.id(), output.bytes().length);
        }
    }

    private static Endpoint blockedWithIdlePingQueued() {
        Endpoint server = Drive.server(CONFIG);
        open(server);
        Drive.advance(server, 500);
        assertThat(server.nextOutput()).isNotNull();
        return server;
    }

    private static Endpoint blockedWithIdlePingQueuedAndAPeerRequestPending() {
        Endpoint server = Drive.server(CONFIG);
        open(server);
        Drive.receive(server, Frames.request(257, 1, new byte[0]));
        Drive.drainEvents(server);
        Drive.advance(server, 500);
        assertThat(server.nextOutput()).isNotNull();
        return server;
    }

    private static List<Integer> sentFrameTypes(Endpoint endpoint) {
        List<Integer> types = new ArrayList<>();
        EndpointOutput output;
        while ((output = endpoint.nextOutput()) != null) {
            types.add(Frames.typeOf(output.bytes(), 0));
            endpoint.outputProgress(output.id(), output.bytes().length);
        }
        return types;
    }

    private static CloseReason closedReason(Endpoint endpoint) {
        return Drive.drainEvents(endpoint).stream()
                .filter(e -> e.kind() == Event.Kind.CLOSED)
                .findFirst()
                .orElseThrow(AssertionError::new)
                .closeReason()
                .orElseThrow(AssertionError::new);
    }
}
