package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.ConnectionPhase.Admission;
import io.github.ppzxc.strand.core.internal.ConnectionPhase.ApplicationSend;
import io.github.ppzxc.strand.core.internal.ConnectionPhase.Phase;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

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

    private static ConnectionPhase server(Phase target) {
        return reach(ConnectionPhase.server(), target);
    }

    private static ConnectionPhase client(Phase target) {
        return reach(ConnectionPhase.client(), target);
    }

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
        assertThat(server(phase).admit(RESERVED, false)).isEqualTo(Admission.RESERVED);
        assertThat(server(phase).admit(0, false)).isEqualTo(Admission.RESERVED);
    }

    static Stream<Phase> establishingPhases() {
        return Stream.of(establishing());
    }

    static Stream<Arguments> admissions() {
        Admission ok = Admission.ALLOW;
        Admission err = Admission.CONNECTION_ERROR;
        return Stream.of(
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

    private static final long NO_ERROR = ErrorCode.NO_ERROR;
    private static final long ERROR = ErrorCode.PROTOCOL_ERROR;

    private enum CloseEvent {
        CLOSE_SENT_NO_ERROR {
            @Override
            void apply(ConnectionPhase p) {
                p.closeSent(NO_ERROR);
            }
        },
        CLOSE_SENT_ERROR {
            @Override
            void apply(ConnectionPhase p) {
                p.closeSent(ERROR);
            }
        },
        CLOSE_RECEIVED_NO_ERROR {
            @Override
            void apply(ConnectionPhase p) {
                p.closeReceived(NO_ERROR);
            }
        },
        CLOSE_RECEIVED_ERROR {
            @Override
            void apply(ConnectionPhase p) {
                p.closeReceived(ERROR);
            }
        },
        CONNECTION_FAILED {
            @Override
            void apply(ConnectionPhase p) {
                p.connectionFailed();
            }
        },
        DISCONNECTED {
            @Override
            void apply(ConnectionPhase p) {
                p.transportDisconnected();
            }
        },
        PENDING_STREAMS_ZERO {
            @Override
            void apply(ConnectionPhase p) {
                p.pendingStreamsReachedZero();
            }
        },
        TRANSPORT_CLOSED {
            @Override
            void apply(ConnectionPhase p) {
                p.transportClosed();
            }
        };

        abstract void apply(ConnectionPhase p);
    }

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

        return Stream.of(
                Arguments.of(CloseEvent.CLOSE_SENT_NO_ERROR, closing, false),
                Arguments.of(CloseEvent.CLOSE_SENT_ERROR, closing, true),
                Arguments.of(CloseEvent.CLOSE_RECEIVED_NO_ERROR, closing, false),
                Arguments.of(CloseEvent.CLOSE_RECEIVED_ERROR, closing, true),
                Arguments.of(CloseEvent.CONNECTION_FAILED, closing, true),
                Arguments.of(CloseEvent.DISCONNECTED, Phase.OPEN, true),
                Arguments.of(CloseEvent.PENDING_STREAMS_ZERO, Phase.OPEN, false),
                Arguments.of(CloseEvent.TRANSPORT_CLOSED, closed, false));
    }

    @ParameterizedTest
    @MethodSource("closeTransitions")
    void closeEventFromOpen(CloseEvent event, Phase next, boolean due) {
        ConnectionPhase subject = server(Phase.OPEN);
        event.apply(subject);
        assertThat(subject.phase()).isEqualTo(next);
        assertThat(subject.shouldCloseTransport()).isEqualTo(due);
    }

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
        assertThat(closing.admit(RESERVED, false)).isEqualTo(Admission.RESERVED);
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

    @Test
    void malformedCloseIsNotTheFirstClose() {
        ConnectionPhase subject = server(Phase.OPEN);
        subject.connectionFailed();
        assertThat(subject.closeExchanged()).isTrue();
        assertThat(subject.closeReceived(NO_ERROR)).isTrue();
    }

    @Test
    void sentCloseDoesNotCountAsReceived() {
        ConnectionPhase subject = closing(Phase.OPEN);
        assertThat(subject.closeReceived(NO_ERROR)).isTrue();
        assertThat(subject.closeReceived(NO_ERROR)).isFalse();
    }

    @Test
    void errorCloseWhileClosingClosesAtOnce() {
        ConnectionPhase subject = closing(Phase.OPEN);
        assertThat(subject.shouldCloseTransport()).isFalse();
        subject.closeReceived(ERROR);
        assertThat(subject.shouldCloseTransport()).isTrue();
    }

    @Test
    void closingEndsNormallyWhenNoStreamsRemainOrIsAborted() {
        ConnectionPhase subject = closing(Phase.OPEN);
        subject.pendingStreamsReachedZero();
        assertThat(subject.phase()).isEqualTo(Phase.CLOSING);
        assertThat(subject.shouldCloseTransport()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = CloseEvent.class,
            names = {"CONNECTION_FAILED", "DISCONNECTED", "CLOSE_RECEIVED_ERROR"})
    void failureWhileClosingIsNotANormalEnd(CloseEvent event) {
        ConnectionPhase subject = closing(Phase.OPEN);
        event.apply(subject);
        assertThat(subject.shouldCloseTransport()).isTrue();
    }

    @Test
    void errorEndStaysErrorEnd() {
        ConnectionPhase subject = server(Phase.OPEN);
        subject.connectionFailed();
        subject.closeReceived(NO_ERROR);
    }

    @Test
    void isActiveOnlyWhileNoCloseExchanged() {
        assertThat(server(Phase.OPEN).isActive()).isTrue();
        assertThat(server(Phase.AUTH).isActive()).isTrue();
        assertThat(closing(Phase.OPEN).isActive()).isFalse();
        assertThat(ended(Phase.OPEN).isActive()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(CloseEvent.class)
    void endedConnectionIgnoresEveryEvent(CloseEvent event) {
        ConnectionPhase subject = ended(Phase.OPEN);
        event.apply(subject);
        assertThat(subject.phase()).isEqualTo(Phase.CLOSED);
        assertThat(subject.isEnded()).isTrue();
        assertThat(subject.shouldCloseTransport()).isFalse();
        assertThat(subject.closeReceived(NO_ERROR)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("establishingPhases")
    void endedConnectionSendsNothing(Phase established) {
        ConnectionPhase subject = ended(established);
        assertThat(subject.mayIdlePing()).isFalse();
        assertThat(subject.applicationSend()).isEqualTo(ApplicationSend.REFUSE);
        for (int type :
                new int[] {FrameType.CLOSE, FrameType.HELLO, FrameType.AUTH, FrameType.PING, FrameType.RESET, APP}) {
            assertThatThrownBy(() -> subject.admit(type, false)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void disconnectAfterDrainedIsNotANormalEnd() {
        ConnectionPhase subject = closing(Phase.OPEN);
        subject.pendingStreamsReachedZero();
        subject.transportDisconnected();
        assertThat(subject.shouldCloseTransport()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = CloseEvent.class,
            names = {"CLOSE_SENT_NO_ERROR", "CLOSE_RECEIVED_NO_ERROR", "CLOSE_SENT_ERROR", "CONNECTION_FAILED"})
    void closeExchangeAfterDisconnectMovesToClosingAsAnError(CloseEvent event) {
        ConnectionPhase subject = server(Phase.OPEN);
        subject.transportDisconnected();
        event.apply(subject);
        assertThat(subject.phase()).isEqualTo(Phase.CLOSING);
        assertThat(subject.closeExchanged()).isTrue();
        assertThat(subject.shouldCloseTransport()).isTrue();
    }

    @Test
    void repeatedDisconnectIsIdempotentAndFailedStaysFailed() {
        ConnectionPhase subject = server(Phase.OPEN);
        subject.transportDisconnected();
        subject.transportDisconnected();
        assertThat(subject.closeExchanged()).isFalse();
        assertThat(subject.phase()).isEqualTo(Phase.OPEN);
        ConnectionPhase failed = closing(Phase.OPEN);
        failed.connectionFailed();
        failed.pendingStreamsReachedZero();
        failed.transportDisconnected();
        assertThat(failed.shouldCloseTransport()).isTrue();
        assertThat(failed.phase()).isEqualTo(Phase.CLOSING);
    }

    @Test
    void pendingStreamsZeroDoesNotMakeADisconnectNormal() {
        ConnectionPhase disconnected = server(Phase.OPEN);
        disconnected.transportDisconnected();
        disconnected.pendingStreamsReachedZero();
        assertThat(disconnected.closeExchanged()).isFalse();
        assertThat(disconnected.shouldCloseTransport()).isTrue();
    }
}
