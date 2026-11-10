package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** 보낼 메시지의 길이를 복사 없이 알 수 있다. 전송 모듈이 보낸 바이트 수를 세는 데 쓴다. */
class OutboundMessageLengthTest {

    /** 연결 서문은 8바이트다(ADR 0002). */
    private static final int PREFACE_BYTES = 8;

    @Test
    void lengthIsTheNumberOfBytesTheMessageCarries() {
        List<OutboundMessage> messages =
                Endpoint.client(EndpointConfig.defaults()).drainMessages();

        assertThat(messages.get(0).length()).isEqualTo(PREFACE_BYTES);
        for (OutboundMessage message : messages) {
            assertThat(message.length()).isEqualTo(message.bytes().length);
        }
        assertThat(messages.get(1).length()).isGreaterThan(PREFACE_BYTES);
    }
}
