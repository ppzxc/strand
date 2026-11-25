package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ConnectAttemptStateTest {

    @Test
    void aCancelBeforeConfirmationIsAcceptedRepeatedly() {
        ConnectAttemptState state = new ConnectAttemptState();

        assertThat(state.cancel()).isTrue();
        assertThat(state.cancel()).isTrue();
    }

    @Test
    void aCancelAfterConfirmationIsNotAccepted() {
        ConnectAttemptState state = new ConnectAttemptState();

        state.confirm();

        assertThat(state.cancel()).isFalse();
    }
}
