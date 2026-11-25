package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class OutboundMessageLengthTest {

    private static final int PREFACE_BYTES = 8;

    @Test
    void lengthIsTheNumberOfBytesTheMessageCarries() {
        List<OutboundMessage> messages = Drive.drainMessages(Endpoint.client(EndpointConfig.defaults()));

        assertThat(messages.get(0).length()).isEqualTo(PREFACE_BYTES);
        for (OutboundMessage message : messages) {
            assertThat(message.length()).isEqualTo(message.bytes().length);
        }
        assertThat(messages.get(1).length()).isGreaterThan(PREFACE_BYTES);
    }
}
