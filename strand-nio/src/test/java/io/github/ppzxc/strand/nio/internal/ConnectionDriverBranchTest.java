package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import org.junit.jupiter.api.Test;

/** 구동기 시작 조건 분기. */
class ConnectionDriverBranchTest {

    @Test
    void aListenerWithoutAnExecutorIsRejectedAtStart() {
        InMemoryPassage passage = InMemoryPassage.streams().first();

        assertThatThrownBy(() -> ConnectionDriver.client(EndpointConfig.defaults(), passage)
                        .listener(new ConnectionListener() {})
                        .start())
                .isInstanceOf(IllegalStateException.class);
        passage.close();
    }
}
