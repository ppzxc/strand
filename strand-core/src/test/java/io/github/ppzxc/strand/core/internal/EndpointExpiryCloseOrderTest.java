package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * 쓰기를 시작한 요청과 쓰기 전 요청이 같은 {@code advance}에서 함께 만료될 때의 송신 순서(#217). 쓰기 전 요청 뒤에 놓인 CLOSE가 있으면
 * 만료로 보내는 {@code RESET}의 자리는 그 요청의 프레임을 거두는 시점에 달려 있다. 리팩터링 전 구현이 낸 순서를 고정한다.
 */
class EndpointExpiryCloseOrderTest {

    private static final int REQUEST_TYPE = 0x0100;

    private static Endpoint established() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(Frames.preface());
        server.receive(Frames.hello(EndpointConfig.defaults()));
        server.receive(Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        server.receive(Frames.auth("ANONYMOUS", new byte[0]));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static List<Integer> types(Endpoint endpoint) {
        List<Integer> types = new ArrayList<>();
        for (OutboundMessage message : endpoint.drainMessages()) {
            types.add(Frames.typeOf(message.bytes(), 0));
        }
        return types;
    }

    @Test
    void expiryResetPrecedesCloseAnchoredBehindAnUnwrittenRequest() {
        Endpoint server = established();
        server.request(REQUEST_TYPE, new byte[0], 10);
        server.drainMessages();
        server.request(REQUEST_TYPE, new byte[0], 10);
        server.close(ErrorCode.NO_ERROR);

        server.advance(11);

        assertThat(types(server)).containsExactly(FrameType.RESET, FrameType.CLOSE);
        List<Event> events = server.drainEvents();
        assertThat(events)
                .filteredOn(event -> event.kind() == Event.Kind.RESULT)
                .extracting(Event::certainty)
                .containsExactly(Optional.of(Certainty.UNKNOWN), Optional.of(Certainty.NOT_PROCESSED));
    }
}
