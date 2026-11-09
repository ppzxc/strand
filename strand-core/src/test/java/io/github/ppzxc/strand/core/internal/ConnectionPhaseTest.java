package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.ConnectionPhase.Admission;
import io.github.ppzxc.strand.core.internal.ConnectionPhase.ApplicationSend;
import io.github.ppzxc.strand.core.internal.ConnectionPhase.Phase;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 연결 단계의 internal seam 테스트. 핸드셰이크 바이트 없이 단계 x 사건 전이 표, 단계별 프레임 타입 허용 표(규격 3.3절 ACK 관문·AUTH
 * 관문), 역할(서버/클라이언트)별 차이, 송신 미룸과 유휴 PING 질의, 종료 쪽(규격 3.5절 CLOSE 교환, 닫는 중, 끝) 전이를 확인한다.
 */
class ConnectionPhaseTest {

    private static final int APP = FrameType.FIRST_APPLICATION;
    private static final int RESERVED = 0x0006;

    private enum Event {
        PREFACE,
        FIRST_FRAME,
        PEER_HELLO,
        OWN_HELLO_ACK,
        AUTH_GATE
    }

    private static void apply(Event event, ConnectionPhase phase) {
        if (event == Event.PREFACE) {
            phase.prefaceVerified();
        } else if (event == Event.FIRST_FRAME) {
            phase.firstFrameVerified();
        } else if (event == Event.PEER_HELLO) {
            phase.peerHelloAcked();
        } else if (event == Event.OWN_HELLO_ACK) {
            phase.ownHelloAckReceived();
        } else {
            phase.authGatePassed();
        }
    }

    /** 서버로 시작해 주어진 단계까지 사건을 차례로 넣는다. */
    private static ConnectionPhase server(Phase target) {
        return reach(ConnectionPhase.server(), target);
    }

    private static ConnectionPhase client(Phase target) {
        return reach(ConnectionPhase.client(), target);
    }

    /** 수립 쪽 단계(PREFACE부터 OPEN까지). 종료 쪽 단계는 별도 표로 확인한다. */
    private static Phase[] establishing() {
        return new Phase[] {
            Phase.PREFACE, Phase.FIRST_FRAME, Phase.PEER_HELLO, Phase.OWN_HELLO_ACK, Phase.AUTH, Phase.OPEN
        };
    }

    private static ConnectionPhase reach(ConnectionPhase phase, Phase target) {
        Event[] path = {Event.PREFACE, Event.FIRST_FRAME, Event.PEER_HELLO, Event.OWN_HELLO_ACK, Event.AUTH_GATE};
        for (Event event : path) {
            if (phase.phase() == target) {
                break;
            }
            if (phase.phase() == Phase.FIRST_FRAME && event == Event.PREFACE) {
                continue;
            }
            apply(event, phase);
        }
        assertThat(phase.phase()).isEqualTo(target);
        return phase;
    }

    @Test
    void serverStartsAtPrefaceAndClientAfterIt() {
        assertThat(ConnectionPhase.server().phase()).isEqualTo(Phase.PREFACE);
        assertThat(ConnectionPhase.client().phase()).isEqualTo(Phase.FIRST_FRAME);
    }

    static Stream<Arguments> transitions() {
        return Stream.of(
                Arguments.of(Phase.PREFACE, Event.PREFACE, Phase.FIRST_FRAME),
                Arguments.of(Phase.FIRST_FRAME, Event.FIRST_FRAME, Phase.PEER_HELLO),
                Arguments.of(Phase.PEER_HELLO, Event.PEER_HELLO, Phase.OWN_HELLO_ACK),
                Arguments.of(Phase.OWN_HELLO_ACK, Event.OWN_HELLO_ACK, Phase.AUTH),
                Arguments.of(Phase.AUTH, Event.AUTH_GATE, Phase.OPEN));
    }

    /** 사건 하나는 정해진 단계에서만 일어나고 한 칸만 나아간다. 다른 단계에서는 던지고 단계는 그대로다. */
    @ParameterizedTest
    @MethodSource("transitions")
    void eventAdvancesOnlyFromItsOwnPhase(Phase from, Event event, Phase to) {
        for (Phase phase : establishing()) {
            ConnectionPhase subject = server(phase);
            if (phase == from) {
                apply(event, subject);
                assertThat(subject.phase()).isEqualTo(to);
            } else {
                assertThatThrownBy(() -> apply(event, subject)).isInstanceOf(IllegalStateException.class);
                assertThat(subject.phase()).isEqualTo(phase);
            }
        }
    }

    @Test
    void awaitingQueriesFollowThePhase() {
        for (Phase phase : establishing()) {
            ConnectionPhase subject = server(phase);
            assertThat(subject.awaitingPreface()).isEqualTo(phase == Phase.PREFACE);
            assertThat(subject.awaitingFirstFrame()).isEqualTo(phase == Phase.FIRST_FRAME);
        }
    }

    @ParameterizedTest
    @MethodSource("establishingPhases")
    void closeAndReservedTypesAreNeverGated(Phase phase) {
        assertThat(server(phase).admit(FrameType.CLOSE, false)).isEqualTo(Admission.ALLOW);
        assertThat(server(phase).admit(RESERVED, false)).isEqualTo(Admission.IGNORE);
        assertThat(server(phase).admit(0, false)).isEqualTo(Admission.IGNORE);
    }

    static Stream<Phase> establishingPhases() {
        return Stream.of(establishing());
    }

    static Stream<Arguments> admissions() {
        Admission ok = Admission.ALLOW;
        Admission err = Admission.CONNECTION_ERROR;
        return Stream.of(
                // 단계, HELLO, HELLO(ACK), AUTH, AUTH(ACK), PING, RESET, 애플리케이션 (서버 기준 AUTH, 클라이언트 AUTH(ACK) 별도)
                Arguments.of(Phase.PEER_HELLO, ok, err, err, err, err, err, err),
                Arguments.of(Phase.OWN_HELLO_ACK, err, ok, err, err, err, err, err),
                Arguments.of(Phase.AUTH, err, err, null, null, err, err, err),
                Arguments.of(Phase.OPEN, err, err, err, err, ok, ok, ok));
    }

    @ParameterizedTest
    @MethodSource("admissions")
    void allowedTypesPerPhase(
            Phase phase,
            Admission hello,
            Admission helloAck,
            Admission auth,
            Admission authAck,
            Admission ping,
            Admission reset,
            Admission application) {
        for (ConnectionPhase subject : new ConnectionPhase[] {server(phase), client(phase)}) {
            assertThat(subject.admit(FrameType.HELLO, false)).isEqualTo(hello);
            assertThat(subject.admit(FrameType.HELLO, true)).isEqualTo(helloAck);
            assertThat(subject.admit(FrameType.PING, false)).isEqualTo(ping);
            assertThat(subject.admit(FrameType.PING, true)).isEqualTo(ping);
            assertThat(subject.admit(FrameType.RESET, false)).isEqualTo(reset);
            assertThat(subject.admit(APP, false)).isEqualTo(application);
            assertThat(subject.admit(APP + 5, false)).isEqualTo(application);
        }
        if (auth != null) {
            assertThat(server(phase).admit(FrameType.AUTH, false)).isEqualTo(auth);
            assertThat(server(phase).admit(FrameType.AUTH, true)).isEqualTo(authAck);
            assertThat(client(phase).admit(FrameType.AUTH, false)).isEqualTo(auth);
            assertThat(client(phase).admit(FrameType.AUTH, true)).isEqualTo(authAck);
        }
    }

    /** AUTH 단계에서만 AUTH를 받고, 서버는 AUTH만, 클라이언트는 AUTH(ACK)만 받는다. */
    @Test
    void authDirectionDependsOnRole() {
        assertThat(server(Phase.AUTH).admit(FrameType.AUTH, false)).isEqualTo(Admission.ALLOW);
        assertThat(server(Phase.AUTH).admit(FrameType.AUTH, true)).isEqualTo(Admission.CONNECTION_ERROR);
        assertThat(client(Phase.AUTH).admit(FrameType.AUTH, true)).isEqualTo(Admission.ALLOW);
        assertThat(client(Phase.AUTH).admit(FrameType.AUTH, false)).isEqualTo(Admission.CONNECTION_ERROR);
    }

    @Test
    void preEstablishedPhasesAdmitNothingElse() {
        for (Phase phase : new Phase[] {Phase.PREFACE, Phase.FIRST_FRAME}) {
            ConnectionPhase subject = server(phase);
            assertThat(subject.admit(FrameType.HELLO, false)).isEqualTo(Admission.CONNECTION_ERROR);
            assertThat(subject.admit(FrameType.PING, false)).isEqualTo(Admission.CONNECTION_ERROR);
            assertThat(subject.admit(APP, false)).isEqualTo(Admission.CONNECTION_ERROR);
        }
    }

    /** 애플리케이션 송신(요청, PING)은 AUTH 관문 뒤로 미루고, 유휴 PING도 그 뒤에만 보낸다(규격 3.3절). */
    @Test
    void sendsAreDeferredAndIdlePingSuppressedUntilTheGateOpens() {
        for (Phase phase : establishing()) {
            boolean open = phase == Phase.OPEN;
            ApplicationSend expected = open ? ApplicationSend.NOW : ApplicationSend.DEFER;
            assertThat(server(phase).applicationSend()).isEqualTo(expected);
            if (phase != Phase.PREFACE) {
                assertThat(client(phase).applicationSend()).isEqualTo(expected);
            }
            assertThat(server(phase).mayIdlePing()).isEqualTo(open);
        }
    }

    // 종료 쪽

    private static final long NO_ERROR = ErrorCode.NO_ERROR;
    private static final long ERROR = ErrorCode.PROTOCOL_ERROR;

    /** 종료 쪽 사건. */
    private enum CloseEvent {
        CLOSE_SENT_NO_ERROR(p -> p.closeSent(NO_ERROR)),
        CLOSE_SENT_ERROR(p -> p.closeSent(ERROR)),
        CLOSE_RECEIVED_NO_ERROR(p -> p.closeReceived(NO_ERROR)),
        CLOSE_RECEIVED_ERROR(p -> p.closeReceived(ERROR)),
        MALFORMED_CLOSE_RECEIVED(ConnectionPhase::malformedCloseReceived),
        NEGOTIATION_FAILED(ConnectionPhase::negotiationFailed),
        CONNECTION_FAILED(ConnectionPhase::connectionFailed),
        DISCONNECTED(ConnectionPhase::transportDisconnected),
        PENDING_STREAMS_ZERO(ConnectionPhase::pendingStreamsReachedZero),
        CLOSING_ABORTED(ConnectionPhase::closingAborted),
        TRANSPORT_CLOSED(ConnectionPhase::transportClosed);

        private final Consumer<ConnectionPhase> action;

        CloseEvent(Consumer<ConnectionPhase> action) {
            this.action = action;
        }
    }

    /** 수립 쪽 단계 하나에서 CLOSE를 NO_ERROR로 보낸 연결. 닫는 중이 된다. */
    private static ConnectionPhase closing(Phase established) {
        ConnectionPhase subject = server(established);
        subject.closeSent(NO_ERROR);
        return subject;
    }

    private static ConnectionPhase ended(Phase established) {
        ConnectionPhase subject = server(established);
        subject.connectionFailed();
        subject.transportClosed();
        return subject;
    }

    static Stream<Arguments> closeTransitions() {
        Phase closing = Phase.CLOSING;
        Phase closed = Phase.CLOSED;
        // 사건, 열림 단계에서 시작한 연결의 다음 단계, 닫을 때가 되는가, 정상 종료인가
        return Stream.of(
                Arguments.of(CloseEvent.CLOSE_SENT_NO_ERROR, closing, false, true),
                Arguments.of(CloseEvent.CLOSE_SENT_ERROR, closing, true, false),
                Arguments.of(CloseEvent.CLOSE_RECEIVED_NO_ERROR, closing, false, true),
                Arguments.of(CloseEvent.CLOSE_RECEIVED_ERROR, closing, true, false),
                Arguments.of(CloseEvent.MALFORMED_CLOSE_RECEIVED, closing, true, false),
                Arguments.of(CloseEvent.NEGOTIATION_FAILED, closing, true, false),
                Arguments.of(CloseEvent.CONNECTION_FAILED, closing, true, false),
                Arguments.of(CloseEvent.DISCONNECTED, Phase.OPEN, true, false),
                // 종료 중이 아닌 연결에서는 대기 중인 스트림 0개 도달도 닫는 일의 근거가 아니다.
                Arguments.of(CloseEvent.PENDING_STREAMS_ZERO, Phase.OPEN, false, false),
                Arguments.of(CloseEvent.CLOSING_ABORTED, Phase.OPEN, false, false),
                Arguments.of(CloseEvent.TRANSPORT_CLOSED, closed, false, false));
    }

    @ParameterizedTest
    @MethodSource("closeTransitions")
    void closeEventFromOpen(CloseEvent event, Phase next, boolean due, boolean normal) {
        ConnectionPhase subject = server(Phase.OPEN);
        event.action.accept(subject);
        assertThat(subject.phase()).isEqualTo(next);
        assertThat(subject.shouldCloseTransport()).isEqualTo(due);
        assertThat(subject.endsNormally()).isEqualTo(normal);
    }

    /** CLOSE 교환은 수립이 어디까지 진행됐든 일어나고, 닫는 중 단계 값 하나로 보인다. */
    @ParameterizedTest
    @MethodSource("establishingPhases")
    void closeExchangeIsPossibleInEveryEstablishingPhase(Phase established) {
        ConnectionPhase[] subjects = established == Phase.PREFACE
                ? new ConnectionPhase[] {server(established)}
                : new ConnectionPhase[] {server(established), client(established)};
        for (ConnectionPhase subject : subjects) {
            subject.closeReceived(NO_ERROR);
            assertThat(subject.phase()).isEqualTo(Phase.CLOSING);
            assertThat(subject.closeExchanged()).isTrue();
        }
    }

    /** 닫는 중에도 수립은 이어진다: 받아도 되는 프레임과 사건이 CLOSE 전과 같다. */
    @ParameterizedTest
    @MethodSource("establishingPhases")
    void closingKeepsAdmittingFramesByEstablishment(Phase established) {
        ConnectionPhase open = server(established);
        ConnectionPhase closing = closing(established);
        for (int type : new int[] {FrameType.HELLO, FrameType.AUTH, FrameType.PING, FrameType.RESET, APP}) {
            for (boolean ack : new boolean[] {false, true}) {
                assertThat(closing.admit(type, ack)).isEqualTo(open.admit(type, ack));
            }
        }
        assertThat(closing.applicationSend()).isEqualTo(open.applicationSend());
        assertThat(closing.mayIdlePing()).isEqualTo(open.mayIdlePing());
        assertThat(closing.admit(FrameType.CLOSE, false)).isEqualTo(Admission.ALLOW);
        assertThat(closing.admit(RESERVED, false)).isEqualTo(Admission.IGNORE);
    }

    @Test
    void closingContinuesEstablishmentEvents() {
        ConnectionPhase subject = closing(Phase.PEER_HELLO);
        subject.peerHelloAcked();
        subject.ownHelloAckReceived();
        subject.authGatePassed();
        assertThat(subject.phase()).isEqualTo(Phase.CLOSING);
        assertThat(subject.admit(APP, false)).isEqualTo(Admission.ALLOW);
    }

    /** 처음 받은 CLOSE만 첫 CLOSE이고, 두 번째 CLOSE는 아니다(규격 3.5절). 오류 코드와 무관하다. */
    @ParameterizedTest
    @MethodSource("secondCloses")
    void onlyTheFirstReceivedCloseIsFirst(long first, long second) {
        ConnectionPhase subject = server(Phase.OPEN);
        assertThat(subject.closeReceived(first)).isTrue();
        assertThat(subject.closeReceived(second)).isFalse();
    }

    static Stream<Arguments> secondCloses() {
        return Stream.of(
                Arguments.of(NO_ERROR, NO_ERROR),
                Arguments.of(NO_ERROR, ERROR),
                Arguments.of(ERROR, NO_ERROR),
                Arguments.of(ERROR, ERROR));
    }

    /** 형태가 잘못된 CLOSE는 처음 받은 CLOSE로 세지 않는다. 그 last_stream_id는 쓰지 않는다. */
    @Test
    void malformedCloseIsNotTheFirstClose() {
        ConnectionPhase subject = server(Phase.OPEN);
        subject.malformedCloseReceived();
        assertThat(subject.closeExchanged()).isTrue();
        assertThat(subject.closeReceived(NO_ERROR)).isTrue();
    }

    /** 내가 보낸 CLOSE는 받은 CLOSE로 세지 않는다. 엇갈려 받은 CLOSE가 처음 받은 CLOSE다. */
    @Test
    void sentCloseDoesNotCountAsReceived() {
        ConnectionPhase subject = closing(Phase.OPEN);
        assertThat(subject.closeReceived(NO_ERROR)).isTrue();
        assertThat(subject.closeReceived(NO_ERROR)).isFalse();
    }

    /** 종료 중에 받은 오류 CLOSE는 곧바로 닫는다(규격 3.5절). */
    @Test
    void errorCloseWhileClosingClosesAtOnce() {
        ConnectionPhase subject = closing(Phase.OPEN);
        assertThat(subject.shouldCloseTransport()).isFalse();
        subject.closeReceived(ERROR);
        assertThat(subject.shouldCloseTransport()).isTrue();
        assertThat(subject.endsNormally()).isFalse();
    }

    /** 대기 중인 스트림 0개 도달과 중단은 종료 중일 때만 닫을 때로 만들고, 정상 종료로 남는다. */
    @ParameterizedTest
    @EnumSource(
            value = CloseEvent.class,
            names = {"PENDING_STREAMS_ZERO", "CLOSING_ABORTED"})
    void closingEndsNormallyWhenNoStreamsRemainOrIsAborted(CloseEvent event) {
        ConnectionPhase subject = closing(Phase.OPEN);
        event.action.accept(subject);
        assertThat(subject.phase()).isEqualTo(Phase.CLOSING);
        assertThat(subject.shouldCloseTransport()).isTrue();
        assertThat(subject.endsNormally()).isTrue();
    }

    /** 오류나 끊김이 종료 중인 연결의 끝을 정상 종료에서 비정상으로 바꾼다(WebSocket Close를 덧붙이지 않는다). */
    @ParameterizedTest
    @EnumSource(
            value = CloseEvent.class,
            names = {"CONNECTION_FAILED", "DISCONNECTED", "CLOSE_RECEIVED_ERROR"})
    void failureWhileClosingIsNotANormalEnd(CloseEvent event) {
        ConnectionPhase subject = closing(Phase.OPEN);
        assertThat(subject.endsNormally()).isTrue();
        event.action.accept(subject);
        assertThat(subject.shouldCloseTransport()).isTrue();
        assertThat(subject.endsNormally()).isFalse();
    }

    /** 오류로 끝나기로 한 연결은 뒤늦은 NO_ERROR CLOSE로 정상 종료가 되지 않는다. */
    @Test
    void errorEndStaysErrorEnd() {
        ConnectionPhase subject = server(Phase.OPEN);
        subject.connectionFailed();
        subject.closeReceived(NO_ERROR);
        assertThat(subject.endsNormally()).isFalse();
    }

    @Test
    void mayOpenRequestOnlyWhileNoCloseExchanged() {
        assertThat(server(Phase.OPEN).mayOpenRequest()).isTrue();
        assertThat(server(Phase.AUTH).mayOpenRequest()).isTrue();
        assertThat(closing(Phase.OPEN).mayOpenRequest()).isFalse();
        assertThat(ended(Phase.OPEN).mayOpenRequest()).isFalse();
    }

    /** 끝난 연결은 어떤 종료 쪽 사건에도 반응하지 않고, 아무것도 받지 않는다. */
    @ParameterizedTest
    @EnumSource(CloseEvent.class)
    void endedConnectionIgnoresEveryEvent(CloseEvent event) {
        ConnectionPhase subject = ended(Phase.OPEN);
        event.action.accept(subject);
        assertThat(subject.phase()).isEqualTo(Phase.CLOSED);
        assertThat(subject.isEnded()).isTrue();
        assertThat(subject.shouldCloseTransport()).isFalse();
        assertThat(subject.closeReceived(NO_ERROR)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("establishingPhases")
    void endedConnectionAdmitsNothing(Phase established) {
        ConnectionPhase subject = ended(established);
        for (int type :
                new int[] {FrameType.CLOSE, FrameType.HELLO, FrameType.AUTH, FrameType.PING, FrameType.RESET, APP}) {
            assertThat(subject.admit(type, false)).isEqualTo(Admission.IGNORE);
        }
        assertThat(subject.mayIdlePing()).isFalse();
        assertThat(subject.applicationSend()).isEqualTo(ApplicationSend.REFUSE);
    }

    @Test
    void mayCloseOnlyWhileNoCloseExchanged() {
        assertThat(server(Phase.AUTH).mayClose()).isTrue();
        assertThat(server(Phase.OPEN).mayClose()).isTrue();
        assertThat(closing(Phase.OPEN).mayClose()).isFalse();
        assertThat(ended(Phase.OPEN).mayClose()).isFalse();
    }

    /** 닫은 로컬 원인은 단계가 보관하고 끝난 뒤에도 읽는다(규격 3.5절, #199). */
    @Test
    void localCauseIsKept() {
        ConnectionPhase subject = server(Phase.AUTH);
        assertThat(subject.closeCause()).isNull();
        IllegalStateException cause = new IllegalStateException("x");
        subject.causedBy(cause);
        subject.connectionFailed();
        subject.transportClosed();
        assertThat(subject.closeCause()).isSameAs(cause);
    }
}
