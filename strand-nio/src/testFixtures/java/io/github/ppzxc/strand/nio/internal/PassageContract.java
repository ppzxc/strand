package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * {@link Passage#await}와 {@link Passage#wakeup}이 지켜야 하는 계약. 통로 구현의 테스트가 이 클래스를 상속해 {@link #create}만 채운다. 읽을 것이
 * 있을 때의 동작은 통로마다 읽을 것을 만드는 방법이 달라 여기서 다루지 않는다.
 */
public abstract class PassageContract {

    /**
     * 시험할 통로 하나. 상대 끝은 아무것도 쓰지 않는다.
     *
     * @return 새 통로
     */
    protected abstract Passage create();

    /**
     * 양수 시간 제한이 실제 시간으로 지나가는 통로인가. 시간을 흘리는 방법이 따로 있는 통로(테스트용 수동 시간)는 false로 답해 그 시험을 건너뛴다.
     *
     * @return 실제 시간으로 지나가면 true
     */
    protected boolean timeoutElapsesInRealTime() {
        return true;
    }

    private static <T> T within(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    private static CompletableFuture<Inbound> awaitOnAnotherThread(Passage passage, long timeoutMs) {
        CompletableFuture<Inbound> result = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        result.complete(passage.await(timeoutMs));
                    } catch (IOException | RuntimeException e) {
                        result.completeExceptionally(e);
                    }
                },
                "passage-contract-await");
        thread.setDaemon(true);
        thread.start();
        return result;
    }

    @Test
    void aZeroTimeoutDoesNotWaitAndReturnsNoneWhenNothingIsToRead() throws Exception {
        Passage passage = create();

        Inbound inbound = within(awaitOnAnotherThread(passage, 0));

        assertThat(inbound.kind()).isEqualTo(Inbound.Kind.NONE);
        passage.close();
    }

    @Test
    void aZeroTimeoutDoesNotWaitEvenWhenWokenBefore() throws Exception {
        Passage passage = create();
        passage.wakeup();

        assertThat(within(awaitOnAnotherThread(passage, 0)).kind()).isEqualTo(Inbound.Kind.NONE);
        passage.close();
    }

    @Test
    void aWakeupBeforeAwaitEndsTheNextAwaitWithoutALimit() throws Exception {
        Passage passage = create();
        passage.wakeup();

        assertThat(within(awaitOnAnotherThread(passage, -1)).kind()).isEqualTo(Inbound.Kind.NONE);
        passage.close();
    }

    @Test
    void anAwaitWithoutALimitEndsWhenWokenWhileWaiting() throws Exception {
        Passage passage = create();
        CompletableFuture<Inbound> waiting = awaitOnAnotherThread(passage, -1);

        assertThat(waiting).isNotDone();
        passage.wakeup();

        assertThat(within(waiting).kind()).isEqualTo(Inbound.Kind.NONE);
        passage.close();
    }

    @Test
    void aPositiveTimeoutReturnsNoneOnceItRunsOut() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(timeoutElapsesInRealTime());
        Passage passage = create();

        assertThat(within(awaitOnAnotherThread(passage, 20)).kind()).isEqualTo(Inbound.Kind.NONE);
        passage.close();
    }
}
