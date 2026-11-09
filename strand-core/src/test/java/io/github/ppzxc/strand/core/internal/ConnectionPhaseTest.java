package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.ConnectionPhase.Admission;
import io.github.ppzxc.strand.core.internal.ConnectionPhase.Phase;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 연결 단계(수립 쪽)의 internal seam 테스트. 핸드셰이크 바이트 없이 단계 x 사건 전이 표, 단계별 프레임 타입 허용 표(규격 3.3절 ACK 관문·AUTH
 * 관문), 역할(서버/클라이언트)별 차이, 송신 미룸과 유휴 PING 질의를 확인한다.
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
        for (Phase phase : Phase.values()) {
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
        for (Phase phase : Phase.values()) {
            ConnectionPhase subject = server(phase);
            assertThat(subject.awaitingPreface()).isEqualTo(phase == Phase.PREFACE);
            assertThat(subject.awaitingFirstFrame()).isEqualTo(phase == Phase.FIRST_FRAME);
        }
    }

    @ParameterizedTest
    @EnumSource(Phase.class)
    void closeAndReservedTypesAreNeverGated(Phase phase) {
        assertThat(server(phase).admit(FrameType.CLOSE, false)).isEqualTo(Admission.ALLOW);
        assertThat(server(phase).admit(RESERVED, false)).isEqualTo(Admission.IGNORE);
        assertThat(server(phase).admit(0, false)).isEqualTo(Admission.IGNORE);
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
        for (Phase phase : Phase.values()) {
            boolean open = phase == Phase.OPEN;
            assertThat(server(phase).defersApplicationSends()).isEqualTo(!open);
            if (phase != Phase.PREFACE) {
                assertThat(client(phase).defersApplicationSends()).isEqualTo(!open);
            }
            assertThat(server(phase).mayIdlePing()).isEqualTo(open);
        }
    }
}
