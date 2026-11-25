package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EndpointExpiryCloseOrderTest {

    private static final int REQUEST_TYPE = 0x0100;

    private static Endpoint established() {
        Endpoint server = Drive.server(EndpointConfig.defaults());
        Drive.receive(server, Frames.preface());
        Drive.receive(server, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(server, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        Drive.receive(server, Frames.auth("ANONYMOUS", new byte[0]));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        return server;
    }

    private static List<Integer> types(Endpoint endpoint) {
        List<Integer> types = new ArrayList<>();
        for (OutboundMessage message : Drive.drainMessages(endpoint)) {
            types.add(Frames.typeOf(message.bytes(), 0));
        }
        return types;
    }

    @Test
    void expiryResetPrecedesCloseAnchoredBehindAnUnwrittenRequest() {
        Endpoint server = established();
        server.request(REQUEST_TYPE, new byte[0], 10);
        Drive.drainMessages(server);
        server.request(REQUEST_TYPE, new byte[0], 10);
        server.close(ErrorCode.NO_ERROR);

        Drive.advance(server, 11);

        assertThat(types(server)).containsExactly(FrameType.RESET, FrameType.CLOSE);
        List<Event> events = Drive.drainEvents(server);
        assertThat(events)
                .filteredOn(event -> event.kind() == Event.Kind.RESULT)
                .extracting(Event::certainty)
                .containsExactly(Optional.of(Certainty.UNKNOWN), Optional.of(Certainty.NOT_PROCESSED));
    }
}
