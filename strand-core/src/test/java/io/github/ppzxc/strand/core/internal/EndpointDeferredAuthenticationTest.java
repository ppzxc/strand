package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class EndpointDeferredAuthenticationTest {
    @Test
    void partialEventCollectionLeavesLaterRequestsEligibleForRefusal() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        server.completeAuthentication(Drive.drainEvents(server).get(0).authenticationId(), AuthDecision.PASS);
        Drive.drainMessages(server);
        Drive.receive(server, Frames.request(257, 1, new byte[0]));
        Drive.receive(server, Frames.request(257, 3, new byte[0]));

        assertThat(server.drainEvents(1))
                .singleElement()
                .satisfies(e -> assertThat(e.streamId()).isEqualTo(1));
        server.close(ErrorCode.INTERNAL_ERROR);
        assertThat(server.drainEvents(1)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
        assertThat(Drive.drainMessages(server))
                .anySatisfy(message -> assertThat(message.bytes()).isEqualTo(Frames.reset(3, ErrorCode.REFUSED)));
    }

    @Test
    void boundedEventCollectionPreservesRemainingEventsInOrder() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        byte[] auth = EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553");
        Drive.receive(server, auth);
        Drive.receive(server, auth);

        assertThat(server.drainEvents(1)).extracting(Event::kind).containsExactly(Event.Kind.AUTHENTICATION_REQUIRED);
        assertThat(server.drainEvents(1)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainEvents(1)).isEmpty();
    }

    @Test
    void requestAfterAuthenticationIsReportedWithoutAnyAutomaticRefusal() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = Drive.drainEvents(server).get(0).authenticationId();
        server.completeAuthentication(id, AuthDecision.PASS);
        Drive.drainMessages(server);

        Drive.receive(server, Frames.request(257, 1, new byte[0]));

        assertThat(Drive.drainEvents(server)).extracting(Event::kind).containsExactly(Event.Kind.REQUEST_RECEIVED);
        assertThat(Drive.drainMessages(server)).isEmpty();
        server.refuseRequest(1);
        assertThat(Drive.drainMessages(server))
                .singleElement()
                .satisfies(message -> assertThat(message.bytes()).isEqualTo(Frames.reset(1, ErrorCode.REFUSED)));
    }

    @Test
    void lateAuthenticationDoesNotReopenClosedConnection() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = Drive.drainEvents(server).get(0).authenticationId();
        server.close(ErrorCode.NO_ERROR);
        Drive.drainEvents(server);
        Drive.drainMessages(server);

        server.completeAuthentication(id, AuthDecision.PASS);

        assertThat(Drive.drainEvents(server)).isEmpty();
        assertThat(Drive.drainMessages(server)).isEmpty();
    }

    @Test
    void duplicateAuthenticationWhileDecisionPendingClosesConnection() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        byte[] auth = EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553");
        Drive.receive(server, auth);
        Drive.drainEvents(server);
        Drive.receive(server, auth);

        assertThat(Drive.drainEvents(server)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void authenticationIsRequestedWithoutCallingUserCode() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.drainMessages(server);
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));

        Event event = Drive.drainEvents(server).get(0);
        assertThat(event.kind()).isEqualTo(Event.Kind.AUTHENTICATION_REQUIRED);
        assertThat(event.authenticationMechanism()).hasValue("ANONYMOUS");
        assertThat(Drive.drainMessages(server)).isEmpty();
        server.completeAuthentication(2, AuthDecision.PASS);
        assertThat(Drive.drainMessages(server)).isEmpty();
        server.completeAuthentication(event.authenticationId(), AuthDecision.PASS);
        assertThat(Drive.drainMessages(server)).hasSize(1);
        server.completeAuthentication(event.authenticationId(), AuthDecision.PASS);
        assertThat(Drive.drainMessages(server)).isEmpty();
    }

    private static Endpoint authPending() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        Drive.drainMessages(server);
        return server;
    }

    @Test
    void specInputIsStillProcessedWhileTheHandlerIsPreparing() {
        Endpoint server = authPending();
        server.completeAuthentication(1, AuthDecision.PASS);
        Drive.drainMessages(server);
        Drive.drainEvents(server);

        Drive.receive(server, EndpointTest.bytes("00000018 0002 00 00 0000000000000000 0000 0000 0102030405060708"));

        assertThat(Drive.drainMessages(server))
                .singleElement()
                .satisfies(message -> assertThat(message.bytes()[5]).isEqualTo((byte) 2));
        assertThat(Drive.drainEvents(server)).isEmpty();
    }

    @Test
    void requestBeforeTheAuthGateOpensIsAProtocolErrorWhileTheDecisionIsPending() {
        Endpoint server = authPending();

        Drive.receive(server, Frames.request(257, 1, new byte[0]));

        assertThat(Drive.drainEvents(server))
                .extracting(Event::kind)
                .containsExactly(Event.Kind.AUTHENTICATION_REQUIRED, Event.Kind.CLOSED);
    }

    @Test
    void failedDecisionSendsNoAcknowledgementAndEndsTheConnection() {
        Endpoint server = authPending();

        server.completeAuthentication(1, AuthDecision.REJECT);

        assertThat(Drive.drainEvents(server)).extracting(Event::kind).contains(Event.Kind.CLOSED);
        server.completeAuthentication(1, AuthDecision.PASS);
        assertThat(Drive.drainMessages(server))
                .noneSatisfy(message -> assertThat(message.bytes()).hasSize(20));
    }

    private static final String AUTH_ACK_NO_LIFETIME = "0000001000050100000000000000000000000000";

    private static String passAndCollectAck(OptionalLong lifetime, EndpointConfig config) {
        Endpoint server = Endpoint.server(config);
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = Drive.drainEvents(server).get(0).authenticationId();
        Drive.drainMessages(server);
        server.completeAuthentication(id, AuthDecision.PASS, lifetime);
        StringBuilder sent = new StringBuilder();
        for (OutboundMessage message : Drive.drainMessages(server)) {
            sent.append(EndpointTest.hex(message.bytes()));
        }
        return sent.toString();
    }

    @Test
    void passDecisionCarriesTheCredentialLifetimeIntoAuthAck() {
        assertThat(passAndCollectAck(OptionalLong.of(300), EndpointConfig.defaults()))
                .isEqualTo("000000160005010000000000000000000000000000010000012c");
    }

    @Test
    void lifetimeComesFromTheDecisionNotFromTheConnectionConfig() {
        assertThat(passAndCollectAck(
                        OptionalLong.of(7), EndpointConfig.defaults().credentialLifetimeSeconds(60)))
                .endsWith("000100000007");
        assertThat(passAndCollectAck(
                        OptionalLong.empty(), EndpointConfig.defaults().credentialLifetimeSeconds(60)))
                .isEqualTo(AUTH_ACK_NO_LIFETIME);
    }

    @Test
    void passWithoutLifetimeAnnouncesNone() {
        assertThat(passAndCollectAck(OptionalLong.empty(), EndpointConfig.defaults()))
                .isEqualTo(AUTH_ACK_NO_LIFETIME);
    }

    @Test
    void maximumUnsignedLifetimeIsCarried() {
        assertThat(passAndCollectAck(OptionalLong.of(0xFFFF_FFFFL), EndpointConfig.defaults()))
                .endsWith("ffffffff");
    }

    @Test
    void lifetimeOutOfUnsigned32BitRangeIsRejected() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        assertThatThrownBy(() -> server.completeAuthentication(1, AuthDecision.PASS, OptionalLong.of(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> server.completeAuthentication(1, AuthDecision.PASS, OptionalLong.of(0x1_0000_0000L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lifetimeExpiringDoesNotCloseTheConnection() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = Drive.drainEvents(server).get(0).authenticationId();
        server.completeAuthentication(id, AuthDecision.PASS, OptionalLong.of(1));
        Drive.drainMessages(server);

        Drive.advance(server, 3_600_000);

        assertThat(Drive.drainEvents(server)).noneMatch(e -> e.kind() == Event.Kind.CLOSED);
    }
}
