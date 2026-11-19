package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** 취소 접수와 확정의 순서, 진행 중 작업 끊기를 엔진 없이 결정적으로 확인한다(#319). */
class ConnectAttemptStateTest {

    @Test
    void aCancelBeforeConfirmationIsAcceptedRepeatedlyAndAbortsTheInProgressWorkOnce() {
        ConnectAttemptState state = new ConnectAttemptState();
        AtomicInteger aborts = new AtomicInteger();
        state.registerAbort(aborts::incrementAndGet);

        assertThat(state.cancel()).isTrue();
        assertThat(state.cancel()).isTrue();

        assertThat(state.cancelRequested()).isTrue();
        assertThat(aborts).hasValue(1);
    }

    @Test
    void aCancelAfterConfirmationIsNotAcceptedAndAbortsNothing() {
        ConnectAttemptState state = new ConnectAttemptState();
        AtomicInteger aborts = new AtomicInteger();
        state.registerAbort(aborts::incrementAndGet);
        state.confirm();

        assertThat(state.cancel()).isFalse();

        assertThat(state.cancelRequested()).isFalse();
        assertThat(aborts).hasValue(0);
    }

    @Test
    void workRegisteredAfterAcceptedCancelIsAbortedAtOnceAndClearingNeverAborts() {
        ConnectAttemptState state = new ConnectAttemptState();
        AtomicInteger aborts = new AtomicInteger();
        state.registerAbort(null);
        assertThat(state.cancel()).isTrue();

        state.registerAbort(aborts::incrementAndGet);
        state.registerAbort(null);

        assertThat(aborts).hasValue(1);
    }
}
