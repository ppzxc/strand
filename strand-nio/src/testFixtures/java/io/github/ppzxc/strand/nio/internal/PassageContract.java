package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * {@link Passage#await}와 {@link Passage#wakeup}이 지켜야 하는 계약. 통로 구현의 테스트가 이 클래스를 상속해 {@link #create}만 채운다. 읽을 것이
 * 있을 때의 동작은 통로마다 읽을 것을 만드는 방법이 달라 여기서 다루지 않는다.
 * (TODO #239: NIO 통로가 이 계약을 상속할 때, 읽을 것이 있을 때 {@code await(0)}이 그것을 돌려주는지도 보강한다.)
 */
public abstract class PassageContract {

    /**
     * 시험할 통로 하나. 상대 끝은 아무것도 쓰지 않는다.
     *
     * @return 새 통로
     */
    protected abstract Passage create();

    /**
     * 가장 최근 {@link #create}가 돌려준 통로의 상대 끝. 여기에 쓴 것을 그 통로가 읽는다.
     *
     * @return 상대 끝
     */
    protected abstract Passage peer();

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

    private static OutboundMessage someMessage() {
        // 엔드포인트가 만든 첫 메시지(서문 또는 첫 프레임). 바이너리 메시지를 직접 만들 수 없어 엔드포인트에서 얻는다.
        return Endpoint.client(EndpointConfig.defaults()).drainMessages().get(0);
    }

    /** 상한 시간 안에서 {@code await(0)}을 되풀이해 {@code count}바이트가 모일 때까지 읽는다. 바이트 스트림은 쪼개져 올 수 있다. */
    private static byte[] pollBytes(Passage passage, int count) throws IOException {
        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (collected.size() < count) {
            if (System.nanoTime() > limit) {
                throw new AssertionError(count + "바이트 중 " + collected.size() + "바이트만 읽었다");
            }
            Inbound inbound = passage.await(0);
            if (inbound.kind() == Inbound.Kind.END) {
                throw new AssertionError("읽는 도중 끝남");
            }
            collected.write(inbound.data(), 0, inbound.data().length);
        }
        return collected.toByteArray();
    }

    @Test
    void aZeroTimeoutAwaitReturnsWhatThePeerWrote() throws Exception {
        Passage passage = create();
        OutboundMessage message = someMessage();

        peer().write(message);

        assertThat(pollBytes(passage, message.bytes().length)).isEqualTo(message.bytes());
        passage.close();
        peer().close();
    }

    @Test
    void anAwaitWithoutALimitReturnsWhatThePeerWroteWhileItWaits() throws Exception {
        Passage passage = create();
        OutboundMessage message = someMessage();
        CompletableFuture<Inbound> waiting = awaitOnAnotherThread(passage, -1);
        assertThat(waiting).isNotDone();

        peer().write(message);

        Inbound first = within(waiting);
        byte[] rest = pollBytes(passage, message.bytes().length - first.data().length);
        byte[] all = new byte[first.data().length + rest.length];
        System.arraycopy(first.data(), 0, all, 0, first.data().length);
        System.arraycopy(rest, 0, all, first.data().length, rest.length);
        assertThat(first.kind()).isNotEqualTo(Inbound.Kind.NONE);
        assertThat(all).isEqualTo(message.bytes());
        passage.close();
        peer().close();
    }

    @Test
    void whatWasReadStaysIntactAfterLaterReads() throws Exception {
        Passage passage = create();
        OutboundMessage message = someMessage();
        int length = message.bytes().length;
        peer().write(message);
        byte[] firstRead = pollBytes(passage, length);
        byte[] snapshot = Arrays.copyOf(firstRead, firstRead.length);

        peer().write(message);
        pollBytes(passage, length);

        assertThat(firstRead).isEqualTo(snapshot);
        passage.close();
        peer().close();
    }

    @Test
    void thePeerClosingEndsTheReadingSideWithEndOrAnIoError() throws Exception {
        Passage passage = create();

        peer().close();

        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean ended = false;
        while (!ended && System.nanoTime() < limit) {
            try {
                ended = passage.await(0).kind() == Inbound.Kind.END;
            } catch (IOException e) {
                ended = true;
            }
        }
        assertThat(ended).as("상대가 닫은 것을 읽는 쪽이 알았다").isTrue();
        passage.close();
    }
}
