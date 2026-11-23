package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * 결과를 요청에 짝짓고 본문을 읽는다(#232). {@code RESULT}는 {@code request}가 돌려준 번호표를 담고, 응답 받음의 {@code RESULT}와
 * {@code REQUEST_RECEIVED}는 본문을 담는다. {@link Endpoint}의 외부 동작만 본다.
 */
class EndpointResultTicketBodyTest {

    private static final int REQUEST_TYPE = 0x0100;
    private static final int OK = 0;
    private static final byte[] NO_BYTES = new byte[0];

    private static Endpoint client(EndpointConfig peerConfig) {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Drive.receive(client, Frames.hello(peerConfig));
        Drive.receive(client, Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        return client;
    }

    private static void passAuthGate(Endpoint client) {
        Drive.receive(client, Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));
        Drive.drainMessages(client);
        Drive.drainEvents(client);
    }

    private static Endpoint establishedClient() {
        Endpoint client = client(EndpointConfig.defaults());
        passAuthGate(client);
        return client;
    }

    private static Endpoint establishedServer() {
        Endpoint server = Drive.server(EndpointConfig.defaults().knownTypes(Collections.singleton(REQUEST_TYPE)));
        Drive.receive(server, Frames.preface());
        Drive.receive(server, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(server, Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        Drive.receive(server, Frames.auth("ANONYMOUS", NO_BYTES));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        return server;
    }

    private static Event onlyResult(Endpoint endpoint) {
        List<Event> events = Drive.drainEvents(endpoint);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        return events.get(0);
    }

    private static long sentStreamId(Endpoint endpoint) {
        List<OutboundMessage> messages = Drive.drainMessages(endpoint);
        assertThat(messages).hasSize(1);
        return Frames.streamIdOf(messages.get(0).bytes(), 0);
    }

    private static Event resultAfterAuthenticated(Endpoint client) {
        List<Event> events = Drive.drainEvents(client);
        assertThat(events).hasSize(2);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.AUTHENTICATED);
        assertThat(events.get(1).kind()).isEqualTo(Event.Kind.RESULT);
        return events.get(1);
    }

    // --- 번호표 ---

    @Test
    void resultOfAnAnsweredRequestCarriesItsTicket() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long second = client.request(REQUEST_TYPE, NO_BYTES);
        List<OutboundMessage> sent = Drive.drainMessages(client);
        long secondStream = Frames.streamIdOf(sent.get(1).bytes(), 0);

        Drive.receive(client, Frames.response(REQUEST_TYPE, secondStream, OK, NO_BYTES));

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(second));
    }

    @Test
    void resultOfAnExpiredRequestCarriesItsTicket() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long ticket = client.request(REQUEST_TYPE, NO_BYTES, 100);
        Drive.drainMessages(client);

        Drive.advance(client, 101);

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfACancelledRequestCarriesItsTicketWhetherCancelledByTicketOrByStream() {
        Endpoint client = establishedClient();
        long byTicket = client.request(REQUEST_TYPE, NO_BYTES);
        long byStream = client.request(REQUEST_TYPE, NO_BYTES);
        List<OutboundMessage> sent = Drive.drainMessages(client);

        client.cancelRequest(byTicket);
        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(byTicket));
        Drive.drainMessages(client);
        client.cancel(Frames.streamIdOf(sent.get(1).bytes(), 0));
        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(byStream));
    }

    @Test
    void resultOfAResetRequestCarriesItsTicket() {
        Endpoint client = establishedClient();
        long ticket = client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);

        Drive.receive(client, Frames.reset(stream, ErrorCode.REFUSED));

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultsOfRequestsEndedByTheConnectionBreakingCarryTheirTickets() {
        Endpoint client = establishedClient();
        long first = client.request(REQUEST_TYPE, NO_BYTES);
        Drive.drainMessages(client);
        long second = client.request(REQUEST_TYPE, NO_BYTES);

        client.disconnect();

        List<Event> events = Drive.drainEvents(client);
        assertThat(events.get(0).ticket()).isEqualTo(OptionalLong.of(first));
        assertThat(events.get(1).ticket()).isEqualTo(OptionalLong.of(second));
        assertThat(events.get(2).kind()).isEqualTo(Event.Kind.CLOSED);
    }

    @Test
    void resultOfARequestEndedByCloseAfterLastStreamIdCarriesItsTicket() {
        Endpoint client = establishedClient();
        long ticket = client.request(REQUEST_TYPE, NO_BYTES);
        Drive.drainMessages(client);

        Drive.receive(client, Frames.close(ErrorCode.NO_ERROR, 0));

        Event result = Drive.drainEvents(client).get(0);
        assertThat(result.kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultsOfRequestsDeferredBeforeTheAuthGateCarryTheirTicketsWhenTheConnectionBreaks() {
        Endpoint client = client(EndpointConfig.defaults());
        long first = client.request(REQUEST_TYPE, NO_BYTES);
        long second = client.request(REQUEST_TYPE, NO_BYTES, 100);

        client.disconnect();

        List<Event> events = Drive.drainEvents(client);
        assertThat(events.get(0).ticket()).isEqualTo(OptionalLong.of(first));
        assertThat(events.get(1).ticket()).isEqualTo(OptionalLong.of(second));
    }

    @Test
    void resultsOfRequestsDeferredBeforeTheAuthGateCarryTheirTicketsWhenCancelledOrExpired() {
        Endpoint client = client(EndpointConfig.defaults());
        long cancelled = client.request(REQUEST_TYPE, NO_BYTES);
        long expiring = client.request(REQUEST_TYPE, NO_BYTES, 100);

        client.cancelRequest(cancelled);
        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(cancelled));
        Drive.advance(client, 101);
        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(expiring));
    }

    @Test
    void resultsOfDeferredRequestsEndedByClosingCarryTheirTickets() {
        Endpoint client = client(EndpointConfig.defaults());
        long first = client.request(REQUEST_TYPE, NO_BYTES);
        long second = client.request(REQUEST_TYPE, NO_BYTES);

        client.close(ErrorCode.NO_ERROR);

        List<Event> events = Drive.drainEvents(client);
        assertThat(events.get(0).ticket()).isEqualTo(OptionalLong.of(first));
        assertThat(events.get(1).ticket()).isEqualTo(OptionalLong.of(second));
    }

    @Test
    void resultOfADeferredRequestThatIsTooLargeWhenSentCarriesItsTicket() {
        Endpoint client = client(EndpointConfig.defaults().maxFrameSize(16384));
        long ticket = client.request(REQUEST_TYPE, new byte[20000]);

        Drive.receive(client, Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));

        Event result = resultAfterAuthenticated(client);
        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfADeferredRequestOverTheStreamLimitWhenSentCarriesItsTicket() {
        Endpoint client = client(EndpointConfig.defaults().maxConcurrentStreams(1));
        client.request(REQUEST_TYPE, NO_BYTES);
        long second = client.request(REQUEST_TYPE, NO_BYTES);

        Drive.receive(client, Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));

        assertThat(resultAfterAuthenticated(client).ticket()).isEqualTo(OptionalLong.of(second));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseTheConnectionIsClosingCarriesItsTicket() {
        Endpoint client = establishedClient();
        client.close(ErrorCode.NO_ERROR);
        Drive.drainEvents(client);

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseTheConnectionEndedCarriesItsTicket() {
        Endpoint client = establishedClient();
        client.disconnect();
        Drive.drainEvents(client);

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseStreamNumbersAreExhaustedCarriesItsTicket() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(Long.MAX_VALUE));
        Drive.receive(client, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(client, Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        passAuthGate(client);
        client.request(REQUEST_TYPE, NO_BYTES);
        Drive.drainMessages(client);

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        Event result = Drive.drainEvents(client).get(0);
        assertThat(result.kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseOfTheStreamLimitCarriesItsTicket() {
        Endpoint client = client(EndpointConfig.defaults().maxConcurrentStreams(1));
        passAuthGate(client);
        client.request(REQUEST_TYPE, NO_BYTES);
        Drive.drainMessages(client);

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void eventsOtherThanResultHaveNoTicket() {
        Endpoint server = establishedServer();
        Drive.receive(server, Frames.request(REQUEST_TYPE, 1, NO_BYTES));
        server.disconnect();

        List<Event> events = Drive.drainEvents(server);
        assertThat(events).hasSize(2);
        for (Event event : events) {
            assertThat(event.ticket()).isEmpty();
        }
    }

    // --- 본문 ---

    @Test
    void responseBodyIsReadableFromTheAnsweredResult() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);

        Drive.receive(client, Frames.response(REQUEST_TYPE, stream, OK, new byte[] {1, 2, 3}));

        assertThat(onlyResult(client).body()).containsExactly(1, 2, 3);
    }

    @Test
    void responseBodyExcludesTheExtensionsArea() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);
        byte[] unknownExtension = {0x70, 0x00, 0, 4, 0, 0, 0, 9};

        Drive.receive(
                client, withExtensions(Frames.response(REQUEST_TYPE, stream, OK, new byte[] {5, 6}), unknownExtension));

        assertThat(onlyResult(client).body()).containsExactly(5, 6);
    }

    /** 확장 영역이 없는 프레임에 확장 영역을 끼워 넣는다. */
    private static byte[] withExtensions(byte[] frame, byte[] extensions) {
        byte[] out = new byte[frame.length + extensions.length];
        System.arraycopy(frame, 0, out, 0, Frames.HEADER_SIZE);
        System.arraycopy(extensions, 0, out, Frames.HEADER_SIZE, extensions.length);
        System.arraycopy(
                frame,
                Frames.HEADER_SIZE,
                out,
                Frames.HEADER_SIZE + extensions.length,
                frame.length - Frames.HEADER_SIZE);
        int length = frame.length - 4 + extensions.length;
        out[0] = (byte) (length >>> 24);
        out[1] = (byte) (length >>> 16);
        out[2] = (byte) (length >>> 8);
        out[3] = (byte) length;
        out[16] = (byte) (extensions.length >>> 8);
        out[17] = (byte) extensions.length;
        return out;
    }

    @Test
    void requestBodyIsReadableFromTheReceivedRequestEvent() {
        Endpoint server = establishedServer();

        Drive.receive(server, Frames.request(REQUEST_TYPE, 1, new byte[] {9, 8, 7}));

        List<Event> events = Drive.drainEvents(server);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.REQUEST_RECEIVED);
        assertThat(events.get(0).body()).containsExactly(9, 8, 7);
    }

    @Test
    void changingTheReturnedBodyDoesNotChangeTheEvent() {
        Endpoint server = establishedServer();
        Drive.receive(server, Frames.request(REQUEST_TYPE, 1, new byte[] {9, 8, 7}));
        Event event = Drive.drainEvents(server).get(0);

        event.body()[0] = 0;

        assertThat(event.body()).containsExactly(9, 8, 7);
    }

    @Test
    void reusingTheInputBufferAfterReceiveDoesNotChangeTheBody() {
        Endpoint server = establishedServer();
        byte[] frame = Frames.request(REQUEST_TYPE, 1, new byte[] {9, 8, 7});
        Drive.receive(server, frame);

        Arrays.fill(frame, (byte) 0);

        assertThat(Drive.drainEvents(server).get(0).body()).containsExactly(9, 8, 7);
    }

    @Test
    void responseOfAnotherTypeIsDiscardedWithItsBody() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);

        Drive.receive(client, Frames.response(REQUEST_TYPE + 1, stream, OK, new byte[] {1, 2, 3}));

        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.INTERNAL);
        assertThat(result.body()).isEmpty();
    }

    @Test
    void emptyResponseBodyIsEmpty() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);

        Drive.receive(client, Frames.response(REQUEST_TYPE, stream, OK, NO_BYTES));

        assertThat(onlyResult(client).body()).isEmpty();
    }

    @Test
    void resultsWithoutAResponseHaveAnEmptyBody() {
        Endpoint client = establishedClient();
        long cancelled = client.request(REQUEST_TYPE, new byte[] {1});
        client.request(REQUEST_TYPE, new byte[] {2}, 100);
        client.request(REQUEST_TYPE, new byte[] {3});
        client.cancelRequest(cancelled);
        Drive.advance(client, 101);
        client.disconnect();
        client.request(REQUEST_TYPE, new byte[] {4});

        List<Event> events = Drive.drainEvents(client);
        assertThat(events).hasSize(5);
        for (Event event : events) {
            assertThat(event.body()).isEmpty();
        }
    }

    @Test
    void closedAndAuthenticatedEventsHaveAnEmptyBody() {
        Endpoint client = client(EndpointConfig.defaults());
        Drive.receive(client, Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));
        client.disconnect();

        List<Event> events = Drive.drainEvents(client);
        assertThat(events).hasSize(2);
        for (Event event : events) {
            assertThat(event.body()).isEmpty();
        }
    }
}
