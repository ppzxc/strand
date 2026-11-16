package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 협상된 한도를 전송 모듈이 읽을 수 있다. 상대 HELLO를 받기 전에는 기본값이고, 받은 뒤에는 상대가 알린 값이다. */
class EndpointNegotiatedLimitsTest {

    private static final long DEFAULT_MAX_FRAME_SIZE = 65_536;
    private static final long DEFAULT_MAX_CONCURRENT_STREAMS = 100;

    @Test
    void beforeThePeerHelloThePeerLimitsAreTheDefaults() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().maxConcurrentStreams(7));

        assertThat(client.peerMaxFrameSize()).isEqualTo(DEFAULT_MAX_FRAME_SIZE);
        assertThat(client.peerMaxConcurrentStreams()).isEqualTo(DEFAULT_MAX_CONCURRENT_STREAMS);
        assertThat(client.ownMaxConcurrentStreams()).isEqualTo(7);
    }

    @Test
    void afterThePeerHelloThePeerLimitsAreWhatThePeerAdvertised() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().maxConcurrentStreams(7));
        Endpoint server =
                Endpoint.server(EndpointConfig.defaults().maxFrameSize(20_000).maxConcurrentStreams(3));
        for (OutboundMessage message : client.drainMessages()) {
            server.receive(message.bytes());
        }
        for (OutboundMessage message : server.drainMessages()) {
            client.receive(message.bytes());
        }

        assertThat(client.peerMaxFrameSize()).isEqualTo(20_000);
        assertThat(client.peerMaxConcurrentStreams()).isEqualTo(3);
        assertThat(client.ownMaxConcurrentStreams()).isEqualTo(7);
        assertThat(server.peerMaxFrameSize()).isEqualTo(DEFAULT_MAX_FRAME_SIZE);
        assertThat(server.peerMaxConcurrentStreams()).isEqualTo(7);
    }
}
