package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EndpointDeferredAuthenticationTest {
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
}
