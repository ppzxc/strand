package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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
                Drive.server(EndpointConfig.defaults().maxFrameSize(20_000).maxConcurrentStreams(3));
        for (OutboundMessage message : Drive.drainMessages(client)) {
            Drive.receive(server, message.bytes());
        }
        for (OutboundMessage message : Drive.drainMessages(server)) {
            Drive.receive(client, message.bytes());
        }

        assertThat(client.peerMaxFrameSize()).isEqualTo(20_000);
        assertThat(client.peerMaxConcurrentStreams()).isEqualTo(3);
        assertThat(client.ownMaxConcurrentStreams()).isEqualTo(7);
        assertThat(server.peerMaxFrameSize()).isEqualTo(DEFAULT_MAX_FRAME_SIZE);
        assertThat(server.peerMaxConcurrentStreams()).isEqualTo(7);
    }
}
