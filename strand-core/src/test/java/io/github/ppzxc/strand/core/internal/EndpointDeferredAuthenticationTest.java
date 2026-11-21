package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class EndpointDeferredAuthenticationTest {
    @Test
    void partialEventCollectionLeavesLaterRequestsEligibleForRefusal() {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        server.completeAuthentication(server.drainEvents().get(0).authenticationId(), AuthDecision.PASS);
        server.handlerReady();
        server.drainMessages();
        server.receive(Frames.request(257, 1, new byte[0]));
        server.receive(Frames.request(257, 3, new byte[0]));

        assertThat(server.drainEvents(1))
                .singleElement()
                .satisfies(e -> assertThat(e.streamId()).isEqualTo(1));
        server.close(ErrorCode.INTERNAL_ERROR);
        assertThat(server.drainEvents(1)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainMessages())
                .anySatisfy(message -> assertThat(message.bytes()).isEqualTo(Frames.reset(3, ErrorCode.REFUSED)));
    }

    @Test
    void boundedEventCollectionPreservesRemainingEventsInOrder() {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        byte[] auth = EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553");
        server.receive(auth);
        server.receive(auth);

        assertThat(server.drainEvents(1)).extracting(Event::kind).containsExactly(Event.Kind.AUTHENTICATION_REQUIRED);
        assertThat(server.drainEvents(1)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
        assertThat(server.drainEvents(1)).isEmpty();
    }

    @Test
    void requestBeforeHandlerReadyIsRefused() {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = server.drainEvents().get(0).authenticationId();
        server.completeAuthentication(id, AuthDecision.PASS);
        server.drainMessages();

        server.receive(Frames.request(257, 1, new byte[0]));

        assertThat(server.drainEvents()).isEmpty();
        assertThat(server.drainMessages())
                .singleElement()
                .satisfies(message -> assertThat(message.bytes()).isEqualTo(Frames.reset(1, ErrorCode.REFUSED)));
        server.handlerReady();
        server.receive(Frames.request(257, 3, new byte[0]));
        assertThat(server.drainEvents()).extracting(Event::kind).containsExactly(Event.Kind.REQUEST_RECEIVED);
    }

    @Test
    void lateAuthenticationDoesNotReopenClosedConnection() {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = server.drainEvents().get(0).authenticationId();
        server.close(ErrorCode.NO_ERROR);
        server.drainEvents();
        server.drainMessages();

        server.completeAuthentication(id, AuthDecision.PASS);

        assertThat(server.drainEvents()).isEmpty();
        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void duplicateAuthenticationWhileDecisionPendingClosesConnection() {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        byte[] auth = EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553");
        server.receive(auth);
        server.drainEvents();
        server.receive(auth);

        assertThat(server.drainEvents()).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
    }

    @Test
    void authenticationIsRequestedWithoutCallingUserCode() {
        EndpointConfig config = EndpointConfig.defaults().authenticator((mechanism, credentials) -> {
            throw new AssertionError("authenticator must execute outside core");
        });
        Endpoint server = Endpoint.serverWithDeferredAuthentication(config);
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.drainMessages();
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));

        Event event = server.drainEvents().get(0);
        assertThat(event.kind()).isEqualTo(Event.Kind.AUTHENTICATION_REQUIRED);
        assertThat(event.authenticationMechanism()).hasValue("ANONYMOUS");
        assertThat(server.drainMessages()).isEmpty();
        server.completeAuthentication(2, AuthDecision.PASS);
        assertThat(server.drainMessages()).isEmpty();
        server.completeAuthentication(event.authenticationId(), AuthDecision.PASS);
        assertThat(server.drainMessages()).hasSize(1);
        server.completeAuthentication(event.authenticationId(), AuthDecision.PASS);
        assertThat(server.drainMessages()).isEmpty();
    }

    private static Endpoint authPending() {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        server.drainMessages();
        return server;
    }

    @Test
    void specInputIsStillProcessedWhileTheHandlerIsPreparing() {
        Endpoint server = authPending();
        server.completeAuthentication(1, AuthDecision.PASS);
        server.drainMessages();
        server.drainEvents();

        server.receive(EndpointTest.bytes("00000018 0002 00 00 0000000000000000 0000 0000 0102030405060708"));

        assertThat(server.drainMessages())
                .singleElement()
                .satisfies(message -> assertThat(message.bytes()[5]).isEqualTo((byte) 2));
        assertThat(server.drainEvents()).isEmpty();
    }

    @Test
    void requestBeforeTheAuthGateOpensIsAProtocolErrorWhileTheDecisionIsPending() {
        Endpoint server = authPending();

        server.receive(Frames.request(257, 1, new byte[0]));

        assertThat(server.drainEvents())
                .extracting(Event::kind)
                .containsExactly(Event.Kind.AUTHENTICATION_REQUIRED, Event.Kind.CLOSED);
    }

    @Test
    void failedDecisionSendsNoAcknowledgementAndEndsTheConnection() {
        Endpoint server = authPending();

        server.completeAuthentication(1, AuthDecision.REJECT);

        assertThat(server.drainEvents()).extracting(Event::kind).contains(Event.Kind.CLOSED);
        server.completeAuthentication(1, AuthDecision.PASS);
        assertThat(server.drainMessages())
                .noneSatisfy(message -> assertThat(message.bytes()).hasSize(20));
    }

    private static final String AUTH_ACK_NO_LIFETIME = "0000001000050100000000000000000000000000";

    private static String passAndCollectAck(OptionalLong lifetime, EndpointConfig config) {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(config);
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = server.drainEvents().get(0).authenticationId();
        server.drainMessages();
        server.completeAuthentication(id, AuthDecision.PASS, lifetime);
        StringBuilder sent = new StringBuilder();
        for (OutboundMessage message : server.drainMessages()) {
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
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        assertThatThrownBy(() -> server.completeAuthentication(1, AuthDecision.PASS, OptionalLong.of(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> server.completeAuthentication(1, AuthDecision.PASS, OptionalLong.of(0x1_0000_0000L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void lifetimeExpiringDoesNotCloseTheConnection() {
        Endpoint server = Endpoint.serverWithDeferredAuthentication(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        long id = server.drainEvents().get(0).authenticationId();
        server.completeAuthentication(id, AuthDecision.PASS, OptionalLong.of(1));
        server.handlerReady();
        server.drainMessages();

        server.advance(3_600_000);

        assertThat(server.drainEvents()).noneMatch(e -> e.kind() == Event.Kind.CLOSED);
    }
}
