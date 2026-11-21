package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * 넘기지 못한 유휴 PING(규격 3.3절). 유휴 PING을 보내기로 한 때부터 확인 대기 시간이 지나도록 전송 계층에 넘기지 못했고 그동안 출력이 진행하지
 * 않았으면 확인 대기 초과와 같게 닫는다. 진행 보고 방식에서 쓰기 완료 보고를 하지 않는 것으로 쓰기가 막힌 연결을 만든다. 가상 시계는 0에서 시작하고,
 * 유휴 간격 500, 확인 대기 1000이다.
 */
class EndpointUnsentIdlePingTest {

    private static final EndpointConfig CONFIG = EndpointConfig.defaults()
            .idleIntervalMs(500)
            .ackTimeoutMs(1000)
            .authTimeoutMs(Long.MAX_VALUE / 4);

    @Test
    void noOutputProgressClosesUnresponsiveAfterTheAckWaitSinceTheIdlePingWasQueued() {
        Endpoint server = blockedWithIdlePingQueued();

        server.advance(1000);
        assertThat(server.drainEvents()).isEmpty();
        server.advance(1);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
        assertThat(sentFrameTypes(server)).containsExactly(FrameType.PING, FrameType.CLOSE);
    }

    @Test
    void theDeadlineIsPartOfTheNextDeadlineQuery() {
        Endpoint server = blockedWithIdlePingQueued();

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(1501));
        server.advance(1001);
        assertThat(server.drainEvents()).isNotEmpty();
    }

    @Test
    void doesNotSendAnotherCloseWhenCloseWasAlreadySent() {
        Endpoint server = blockedWithIdlePingQueuedAndAPeerRequestPending();
        server.close(ErrorCode.NO_ERROR);
        server.drainEvents();

        server.advance(1001);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
        assertThat(sentFrameTypes(server).stream().filter(t -> t == FrameType.CLOSE))
                .hasSize(1);
    }

    @Test
    void doesNotSendCloseWhenCloseWasAlreadyReceived() {
        Endpoint server = blockedWithIdlePingQueuedAndAPeerRequestPending();
        server.receive(Frames.close(ErrorCode.NO_ERROR, 0));
        server.drainEvents();

        server.advance(1001);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
        assertThat(sentFrameTypes(server)).doesNotContain(FrameType.CLOSE);
    }

    @Test
    void anyOutputProgressCountsTheDeadlineAgain() {
        Endpoint server = blockedWithIdlePingQueued();
        server.advance(900); // 1400: 기한 직전
        EndpointOutput head = server.nextOutput();
        server.outputProgress(head.id(), 1);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(2401));
        server.advance(1000); // 2400
        assertThat(server.drainEvents()).isEmpty();
        server.advance(1);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
    }

    @Test
    void aBigFrameWrittenBitByBitIsNeverClosed() {
        Endpoint server = blockedWithIdlePingQueued();
        for (int i = 0; i < 20; i++) {
            server.advance(900);
            EndpointOutput head = server.nextOutput();
            assertThat(head).isNotNull();
            server.outputProgress(head.id(), 1);
            assertThat(server.drainEvents()).isEmpty();
        }
    }

    @Test
    void theAckWaitOfAHandedOffIdlePingStartsAtHandOff() {
        Endpoint server = blockedWithIdlePingQueued();
        server.advance(900); // 1400
        EndpointOutput ping = server.nextOutput();
        server.outputProgress(ping.id(), ping.bytes().length); // 넘김: 대기 시작 1400

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(2401));
        server.advance(1000);
        assertThat(server.drainEvents()).isEmpty();
        server.advance(1);

        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
    }

    @Test
    void aDrainedIdlePingKeepsTheAckWaitFromTheHandOff() {
        Endpoint server = Endpoint.server(CONFIG);
        server.receive(EndpointTest.bytes(PREFACE));
        server.receive(EndpointTest.bytes(HELLO));
        server.receive(EndpointTest.bytes(HELLO_ACK));
        server.receive(EndpointTest.bytes(AUTH_ANONYMOUS));
        server.drainEvents();
        server.drainMessages();
        server.advance(500);
        server.drainMessages();

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(1501));
        server.advance(1001);
        assertThat(closedReason(server).errorCode()).isEqualTo(ErrorCode.UNRESPONSIVE);
    }

    @Test
    void anAckBeforeTheHandOffEndsTheRule() {
        Endpoint server = blockedWithIdlePingQueued();
        server.receive(Frames.control(FrameType.PING, 1, new byte[8]));
        server.drainEvents();

        server.advance(5000);

        assertThat(server.drainEvents()).extracting(Event::kind).doesNotContain(Event.Kind.CLOSED);
    }

    @Test
    void anApplicationPingBlockedInTheQueueIsNotSubjectToTheRule() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().idleIntervalMs(0).ackTimeoutMs(1000));
        open(server);
        server.ping(new byte[8]);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.empty());
        server.advance(100_000);

        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void theBlockedHelloIsNotSubjectToTheRule() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().idleIntervalMs(500).ackTimeoutMs(1000));

        assertThat(client.nextDeadline()).isEqualTo(OptionalLong.empty());
        client.advance(100_000);

        assertThat(client.drainEvents()).isEmpty();
    }

    private static final String PREFACE = "c0de535452300000";
    private static final String HELLO = "0000001000010000000000000000000000000000";
    private static final String HELLO_ACK = "0000001000010100000000000000000000000000";
    private static final String AUTH_ANONYMOUS = "0000001a0005000000000000000000000000000009414e4f4e594d4f5553";

    private static void open(Endpoint server) {
        server.receive(EndpointTest.bytes(PREFACE));
        server.receive(EndpointTest.bytes(HELLO));
        server.receive(EndpointTest.bytes(HELLO_ACK));
        server.receive(EndpointTest.bytes(AUTH_ANONYMOUS));
        server.drainEvents();
        EndpointOutput output;
        while ((output = server.nextOutput()) != null) {
            server.outputProgress(output.id(), output.bytes().length);
        }
    }

    /** 진행 보고 방식. 출력을 하나도 진행하지 않은 채 가상 시각 500에 유휴 PING이 쓰기 대기열에 들어갔다. */
    private static Endpoint blockedWithIdlePingQueued() {
        Endpoint server = Endpoint.server(CONFIG);
        open(server);
        server.advance(500);
        assertThat(server.nextOutput()).isNotNull();
        return server;
    }

    /** 위와 같고, 상대 요청이 응답을 기다려서 닫는 중에도 연결이 끝나지 않는다. */
    private static Endpoint blockedWithIdlePingQueuedAndAPeerRequestPending() {
        Endpoint server = Endpoint.server(CONFIG);
        open(server);
        server.receive(Frames.request(257, 1, new byte[0]));
        server.drainEvents();
        server.advance(500);
        assertThat(server.nextOutput()).isNotNull();
        return server;
    }

    /** 남은 출력을 모두 진행 보고로 비우며 프레임 종류를 모은다. */
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
        return endpoint.drainEvents().stream()
                .filter(e -> e.kind() == Event.Kind.CLOSED)
                .findFirst()
                .orElseThrow(AssertionError::new)
                .closeReason()
                .orElseThrow(AssertionError::new);
    }
}
