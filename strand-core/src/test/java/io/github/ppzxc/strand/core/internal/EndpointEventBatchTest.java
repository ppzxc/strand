package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class EndpointEventBatchTest {

    private static final int REQUEST_TYPE = 0x0100;
    private static final byte[] NO_BYTES = new byte[0];

    @Test
    void boundedDrainsPreserveOrderWithoutLosingEventsAcrossBatches() {
        Endpoint server = Drive.server(EndpointConfig.defaults().knownTypes(Collections.singleton(REQUEST_TYPE)));
        Drive.receive(server, Frames.preface());
        Drive.receive(server, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(server, Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        Drive.receive(server, Frames.auth("ANONYMOUS", NO_BYTES));
        Drive.drainMessages(server);
        Drive.drainEvents(server);

        for (long streamId = 1; streamId <= 33; streamId += 2) {
            Drive.receive(server, Frames.request(REQUEST_TYPE, streamId, NO_BYTES));
        }

        assertThat(server.drainEvents(0)).isEmpty();
        List<Long> drainedStreamIds = new ArrayList<>();
        while (true) {
            List<Event> batch = server.drainEvents(4);
            if (batch.isEmpty()) {
                break;
            }
            batch.stream().map(Event::streamId).forEach(drainedStreamIds::add);
        }

        assertThat(drainedStreamIds).containsExactly(
                1L, 3L, 5L, 7L, 9L, 11L, 13L, 15L, 17L,
                19L, 21L, 23L, 25L, 27L, 29L, 31L, 33L);
    }
}
