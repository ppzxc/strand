package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class EndpointNextDeadlineTest {

    @Test
    void negativeElapsedTimeIsRejectedWithoutChangingTheClock() {
        Endpoint server = open(quiet());
        Drive.advance(server, 100);
        server.request(257, new byte[0], 500);

        assertThatIllegalArgumentException().isThrownBy(() -> Drive.advance(server, -1));
        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(601));
    }

    @Test
    void requestDeadlineDoesNotWrapIntoThePast() {
        Endpoint server = open(quiet());
        Drive.advance(server, 100);
        server.request(257, new byte[0], Long.MAX_VALUE);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(Long.MAX_VALUE));
        Drive.advance(server, 1);
        assertThat(Drive.drainEvents(server)).isEmpty();
    }

    @Test
    void elapsedNanosAdvanceTheClockToAnAbsoluteInstant() {
        Endpoint server = open(quiet());
        server.advanceTo(100_000_000L);
        server.request(257, new byte[0], 500);
        Drive.drainMessages(server);

        assertThat(server.nextDeadlineNanos()).isEqualTo(OptionalLong.of(601_000_000L));
        server.advanceTo(600_999_999L);
        assertThat(Drive.drainEvents(server)).isEmpty();
        server.advanceTo(601_000_000L);
        assertThat(Drive.drainEvents(server)).hasSize(1);
        assertThat(server.nextDeadlineNanos()).isEmpty();
    }

    @Test
    void theSameInstantIsAcceptedAndChangesNothing() {
        Endpoint server = open(quiet());
        server.advanceTo(5_000_000L);
        server.request(257, new byte[0], 10);

        server.advanceTo(5_000_000L);

        assertThat(Drive.drainEvents(server)).isEmpty();
        assertThat(server.nextDeadlineNanos()).isEqualTo(OptionalLong.of(16_000_000L));
    }

    @Test
    void negativeOrRegressingInstantsAreRejectedWithoutChangingState() {
        Endpoint server = open(quiet());
        server.advanceTo(5_000_000L);
        server.request(257, new byte[0], 10);

        assertThatIllegalArgumentException().isThrownBy(() -> server.advanceTo(-1));
        assertThatIllegalArgumentException().isThrownBy(() -> server.advanceTo(4_999_999L));

        assertThat(server.nextDeadlineNanos()).isEqualTo(OptionalLong.of(16_000_000L));
        server.advanceTo(5_000_000L);
    }

    @Test
    void maximumElapsedNanosDoNotOverflowTheDeadline() {
        Endpoint server = open(quiet());
        server.advanceTo(Long.MAX_VALUE);
        server.request(257, new byte[0], Long.MAX_VALUE);

        assertThat(server.nextDeadlineNanos()).isEqualTo(OptionalLong.of(Long.MAX_VALUE));
    }

    private static final String PREFACE = "c0de535452300000";
    private static final String HELLO = "0000001000010000000000000000000000000000";
    private static final String HELLO_ACK = "0000001000010100000000000000000000000000";
    private static final String AUTH_ANONYMOUS = "0000001a0005000000000000000000000000000009414e4f4e594d4f5553";
    private static final String PING_ACK = "0000001800020100000000000000000000000000" + "0102030405060708";
    private static final byte[] PING_PAYLOAD = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    private static final long FAR = Long.MAX_VALUE / 4;

    private static byte[] bytes(String hex) {
        return EndpointTest.bytes(hex);
    }

    private static EndpointConfig quiet() {
        return EndpointConfig.defaults().idleIntervalMs(0).ackTimeoutMs(FAR).authTimeoutMs(FAR);
    }

    private static Endpoint open(EndpointConfig config) {
        Endpoint server = Drive.server(config);
        Drive.receive(server, bytes(PREFACE));
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        Drive.receive(server, bytes(AUTH_ANONYMOUS));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        return server;
    }

    @Test
    void emptyWhenNothingIsDue() {
        Endpoint server = open(quiet());

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.empty());
    }

    @Test
    void reflectsARequestDeadline() {
        Endpoint server = open(quiet());
        Drive.advance(server, 100);
        server.request(257, new byte[0], 500);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(601));
    }

    @Test
    void advancingToTheRequestDeadlineEndsTheRequest() {
        Endpoint server = open(quiet());
        Drive.advance(server, 100);
        server.request(257, new byte[0], 500);
        Drive.drainMessages(server);

        Drive.advance(server, 499);
        assertThat(Drive.drainEvents(server)).isEmpty();
        Drive.advance(server, 2);

        assertThat(Drive.drainEvents(server)).hasSize(1);
        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.empty());
    }

    @Test
    void reflectsTheAckWaitOfTheHelloAndEndingItClosesTheConnection() {
        Endpoint server = Drive.server(
                EndpointConfig.defaults().idleIntervalMs(0).ackTimeoutMs(1000).authTimeoutMs(FAR));
        Drive.receive(server, bytes(PREFACE));
        Drive.drainMessages(server);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(1001));
        Drive.advance(server, 1000);
        assertThat(Drive.drainEvents(server)).isEmpty();
        Drive.advance(server, 1);
        assertThat(Drive.drainEvents(server)).isNotEmpty();
        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.empty());
    }

    @Test
    void reflectsTheAckWaitOfThePing() {
        Endpoint server = open(
                EndpointConfig.defaults().idleIntervalMs(0).ackTimeoutMs(1000).authTimeoutMs(FAR));
        Drive.advance(server, 300);
        server.ping(PING_PAYLOAD);
        Drive.drainMessages(server);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(1301));
        Drive.receive(server, bytes(PING_ACK));
        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.empty());
    }

    @Test
    void reflectsTheAckWaitOfTheAuth() {
        Endpoint client =
                Endpoint.client(EndpointConfig.defaults().idleIntervalMs(0).ackTimeoutMs(1000));
        Drive.drainMessages(client);
        Drive.advance(client, 200);
        Drive.receive(client, bytes(HELLO));
        Drive.drainMessages(client);

        assertThat(client.nextDeadline()).isEqualTo(OptionalLong.of(1001));
        Drive.receive(client, bytes(HELLO_ACK));
        Drive.drainMessages(client);

        assertThat(client.nextDeadline()).isEqualTo(OptionalLong.of(1201));
    }

    @Test
    void reflectsTheServerAuthWait() {
        Endpoint server = Drive.server(
                EndpointConfig.defaults().idleIntervalMs(0).ackTimeoutMs(FAR).authTimeoutMs(700));
        Drive.receive(server, bytes(PREFACE));
        Drive.advance(server, 50);
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        Drive.drainMessages(server);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(751));
        Drive.advance(server, 700);
        assertThat(Drive.drainEvents(server)).isEmpty();
        Drive.advance(server, 1);
        assertThat(Drive.drainEvents(server)).isNotEmpty();
    }

    @Test
    void reflectsTheIdlePingSchedule() {
        Endpoint server = open(quiet().idleIntervalMs(3000));
        Drive.advance(server, 100);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(3000));
        Drive.advance(server, 2899);
        assertThat(Drive.drainMessages(server)).isEmpty();
        Drive.advance(server, 1);
        assertThat(Drive.drainMessages(server)).isNotEmpty();
    }

    @Test
    void anIdlePingWaitsForItsAckBeforeTheNextIdlePing() {
        Endpoint server = open(quiet().idleIntervalMs(3000));
        Drive.advance(server, 3000);
        Drive.drainMessages(server);

        assertThat(server.nextDeadline().getAsLong()).isGreaterThan(3000 + 3000);
    }

    @Test
    void returnsTheEarliestOfSeveral() {
        Endpoint server = open(EndpointConfig.defaults()
                .idleIntervalMs(0)
                .ackTimeoutMs(1000)
                .authTimeoutMs(FAR)
                .idleIntervalMs(5000));
        server.request(257, new byte[0], 4000);
        Drive.advance(server, 200);
        server.ping(PING_PAYLOAD);
        Drive.drainMessages(server);

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.of(1201));
    }

    @Test
    void emptyOnceTheConnectionEnded() {
        Endpoint server = open(quiet().idleIntervalMs(3000));
        server.request(257, new byte[0], 500);
        server.ping(PING_PAYLOAD);

        server.disconnect();

        assertThat(server.nextDeadline()).isEqualTo(OptionalLong.empty());
    }
}
