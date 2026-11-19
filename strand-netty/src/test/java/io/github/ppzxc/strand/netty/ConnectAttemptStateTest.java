package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 연결 시도 취소 상태: 확정 전 취소는 반복해도 접수되고, 확정 뒤 취소는 접수하지 않는다(#325). */
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
