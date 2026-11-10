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
        client.receive(Frames.hello(peerConfig));
        client.receive(Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        return client;
    }

    private static void passAuthGate(Endpoint client) {
        client.receive(Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));
        client.drainMessages();
        client.drainEvents();
    }

    private static Endpoint establishedClient() {
        Endpoint client = client(EndpointConfig.defaults());
        passAuthGate(client);
        return client;
    }

    private static Endpoint establishedServer() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().knownTypes(Collections.singleton(REQUEST_TYPE)));
        server.receive(Frames.preface());
        server.receive(Frames.hello(EndpointConfig.defaults()));
        server.receive(Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        server.receive(Frames.auth("ANONYMOUS", NO_BYTES));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static Event onlyResult(Endpoint endpoint) {
        List<Event> events = endpoint.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        return events.get(0);
    }

    private static long sentStreamId(Endpoint endpoint) {
        List<OutboundMessage> messages = endpoint.drainMessages();
        assertThat(messages).hasSize(1);
        return Frames.streamIdOf(messages.get(0).bytes(), 0);
    }

    private static Event resultAfterAuthenticated(Endpoint client) {
        List<Event> events = client.drainEvents();
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
        List<OutboundMessage> sent = client.drainMessages();
        long secondStream = Frames.streamIdOf(sent.get(1).bytes(), 0);

        client.receive(Frames.response(REQUEST_TYPE, secondStream, OK, NO_BYTES));

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(second));
    }

    @Test
    void resultOfAnExpiredRequestCarriesItsTicket() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long ticket = client.request(REQUEST_TYPE, NO_BYTES, 100);
        client.drainMessages();

        client.advance(101);

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfACancelledRequestCarriesItsTicketWhetherCancelledByTicketOrByStream() {
        Endpoint client = establishedClient();
        long byTicket = client.request(REQUEST_TYPE, NO_BYTES);
        long byStream = client.request(REQUEST_TYPE, NO_BYTES);
        List<OutboundMessage> sent = client.drainMessages();

        client.cancelRequest(byTicket);
        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(byTicket));
        client.drainMessages();
        client.cancel(Frames.streamIdOf(sent.get(1).bytes(), 0));
        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(byStream));
    }

    @Test
    void resultOfAResetRequestCarriesItsTicket() {
        Endpoint client = establishedClient();
        long ticket = client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);

        client.receive(Frames.reset(stream, ErrorCode.REFUSED));

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultsOfRequestsEndedByTheConnectionBreakingCarryTheirTickets() {
        Endpoint client = establishedClient();
        long first = client.request(REQUEST_TYPE, NO_BYTES);
        client.drainMessages();
        long second = client.request(REQUEST_TYPE, NO_BYTES);

        client.disconnect();

        List<Event> events = client.drainEvents();
        assertThat(events.get(0).ticket()).isEqualTo(OptionalLong.of(first));
        assertThat(events.get(1).ticket()).isEqualTo(OptionalLong.of(second));
        assertThat(events.get(2).kind()).isEqualTo(Event.Kind.CLOSED);
    }

    @Test
    void resultOfARequestEndedByCloseAfterLastStreamIdCarriesItsTicket() {
        Endpoint client = establishedClient();
        long ticket = client.request(REQUEST_TYPE, NO_BYTES);
        client.drainMessages();

        client.receive(Frames.close(ErrorCode.NO_ERROR, 0));

        Event result = client.drainEvents().get(0);
        assertThat(result.kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultsOfRequestsDeferredBeforeTheAuthGateCarryTheirTicketsWhenTheConnectionBreaks() {
        Endpoint client = client(EndpointConfig.defaults());
        long first = client.request(REQUEST_TYPE, NO_BYTES);
        long second = client.request(REQUEST_TYPE, NO_BYTES, 100);

        client.disconnect();

        List<Event> events = client.drainEvents();
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
        client.advance(101);
        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(expiring));
    }

    @Test
    void resultsOfDeferredRequestsEndedByClosingCarryTheirTickets() {
        Endpoint client = client(EndpointConfig.defaults());
        long first = client.request(REQUEST_TYPE, NO_BYTES);
        long second = client.request(REQUEST_TYPE, NO_BYTES);

        client.close(ErrorCode.NO_ERROR);

        List<Event> events = client.drainEvents();
        assertThat(events.get(0).ticket()).isEqualTo(OptionalLong.of(first));
        assertThat(events.get(1).ticket()).isEqualTo(OptionalLong.of(second));
    }

    @Test
    void resultOfADeferredRequestThatIsTooLargeWhenSentCarriesItsTicket() {
        Endpoint client = client(EndpointConfig.defaults().maxFrameSize(16384));
        long ticket = client.request(REQUEST_TYPE, new byte[20000]);

        client.receive(Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));

        Event result = resultAfterAuthenticated(client);
        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfADeferredRequestOverTheStreamLimitWhenSentCarriesItsTicket() {
        Endpoint client = client(EndpointConfig.defaults().maxConcurrentStreams(1));
        client.request(REQUEST_TYPE, NO_BYTES);
        long second = client.request(REQUEST_TYPE, NO_BYTES);

        client.receive(Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));

        assertThat(resultAfterAuthenticated(client).ticket()).isEqualTo(OptionalLong.of(second));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseTheConnectionIsClosingCarriesItsTicket() {
        Endpoint client = establishedClient();
        client.close(ErrorCode.NO_ERROR);
        client.drainEvents();

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseTheConnectionEndedCarriesItsTicket() {
        Endpoint client = establishedClient();
        client.disconnect();
        client.drainEvents();

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        assertThat(onlyResult(client).ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseStreamNumbersAreExhaustedCarriesItsTicket() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().nextStreamId(Long.MAX_VALUE));
        client.receive(Frames.hello(EndpointConfig.defaults()));
        client.receive(Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        passAuthGate(client);
        client.request(REQUEST_TYPE, NO_BYTES);
        client.drainMessages();

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        Event result = client.drainEvents().get(0);
        assertThat(result.kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void resultOfARequestRefusedLocallyBecauseOfTheStreamLimitCarriesItsTicket() {
        Endpoint client = client(EndpointConfig.defaults().maxConcurrentStreams(1));
        passAuthGate(client);
        client.request(REQUEST_TYPE, NO_BYTES);
        client.drainMessages();

        long ticket = client.request(REQUEST_TYPE, NO_BYTES);

        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.ticket()).isEqualTo(OptionalLong.of(ticket));
    }

    @Test
    void eventsOtherThanResultHaveNoTicket() {
        Endpoint server = establishedServer();
        server.receive(Frames.request(REQUEST_TYPE, 1, NO_BYTES));
        server.disconnect();

        List<Event> events = server.drainEvents();
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

        client.receive(Frames.response(REQUEST_TYPE, stream, OK, new byte[] {1, 2, 3}));

        assertThat(onlyResult(client).body()).containsExactly(1, 2, 3);
    }

    @Test
    void responseBodyExcludesTheExtensionsArea() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);
        byte[] unknownExtension = {0x70, 0x00, 0, 4, 0, 0, 0, 9};

        client.receive(withExtensions(Frames.response(REQUEST_TYPE, stream, OK, new byte[] {5, 6}), unknownExtension));

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

        server.receive(Frames.request(REQUEST_TYPE, 1, new byte[] {9, 8, 7}));

        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.REQUEST_RECEIVED);
        assertThat(events.get(0).body()).containsExactly(9, 8, 7);
    }

    @Test
    void changingTheReturnedBodyDoesNotChangeTheEvent() {
        Endpoint server = establishedServer();
        server.receive(Frames.request(REQUEST_TYPE, 1, new byte[] {9, 8, 7}));
        Event event = server.drainEvents().get(0);

        event.body()[0] = 0;

        assertThat(event.body()).containsExactly(9, 8, 7);
    }

    @Test
    void reusingTheInputBufferAfterReceiveDoesNotChangeTheBody() {
        Endpoint server = establishedServer();
        byte[] frame = Frames.request(REQUEST_TYPE, 1, new byte[] {9, 8, 7});
        server.receive(frame);

        Arrays.fill(frame, (byte) 0);

        assertThat(server.drainEvents().get(0).body()).containsExactly(9, 8, 7);
    }

    @Test
    void responseOfAnotherTypeIsDiscardedWithItsBody() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);

        client.receive(Frames.response(REQUEST_TYPE + 1, stream, OK, new byte[] {1, 2, 3}));

        Event result = onlyResult(client);
        assertThat(result.status()).isEqualTo(Status.INTERNAL);
        assertThat(result.body()).isEmpty();
    }

    @Test
    void emptyResponseBodyIsEmpty() {
        Endpoint client = establishedClient();
        client.request(REQUEST_TYPE, NO_BYTES);
        long stream = sentStreamId(client);

        client.receive(Frames.response(REQUEST_TYPE, stream, OK, NO_BYTES));

        assertThat(onlyResult(client).body()).isEmpty();
    }

    @Test
    void resultsWithoutAResponseHaveAnEmptyBody() {
        Endpoint client = establishedClient();
        long cancelled = client.request(REQUEST_TYPE, new byte[] {1});
        client.request(REQUEST_TYPE, new byte[] {2}, 100);
        client.request(REQUEST_TYPE, new byte[] {3});
        client.cancelRequest(cancelled);
        client.advance(101);
        client.disconnect();
        client.request(REQUEST_TYPE, new byte[] {4});

        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(5);
        for (Event event : events) {
            assertThat(event.body()).isEmpty();
        }
    }

    @Test
    void closedAndAuthenticatedEventsHaveAnEmptyBody() {
        Endpoint client = client(EndpointConfig.defaults());
        client.receive(Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));
        client.disconnect();

        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(2);
        for (Event event : events) {
            assertThat(event.body()).isEmpty();
        }
    }
}
