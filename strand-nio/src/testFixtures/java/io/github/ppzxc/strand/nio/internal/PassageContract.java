package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * {@link Passage#await}와 {@link Passage#wakeup}이 지켜야 하는 계약. 통로 구현의 테스트가 이 클래스를 상속해 {@link #create}와 {@link #peer}를 채운다.
 * 읽을 것은 상대 끝에 {@link Passage#write}로 써서 만든다. 상대가 쓴 것이 이쪽에 닿는 데 걸리는 시간은 통로마다 달라(TCP는 즉시가 아니다), 읽는 시험은
 * {@code await}를 상한 시간(5초) 안에서만 되풀이한다.
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
     * 쓰기가 막히는 통로 하나. 상대 끝이 읽지 않는 동안 {@link Passage#write}가 일부만 받다가 0을 돌려줘야 한다. 상대 끝은 {@link #peer}로 얻고,
     * 막힘은 {@link #unblockWrites}로 푼다.
     *
     * @return 쓰기가 막히는 새 통로
     */
    protected abstract Passage createBlockable();

    /**
     * {@link #createBlockable}이 만든 통로의 쓰기 막힘을 푼다. 상대가 읽게 하거나 쓰기 용량을 늘린다. 풀린 뒤 통로는 더 받아야 하고, 쓰기를
     * 기다리던 {@link Passage#await}가 깨어나야 한다.
     *
     * @param blocked 쓰기가 막힌 통로
     * @return 풀면서 상대 끝에서 읽어 낸 바이트(읽지 않고 푸는 구현은 빈 배열)
     * @throws IOException 풀다가 입출력 오류가 났을 때
     */
    protected abstract byte[] unblockWrites(Passage blocked) throws IOException;

    /**
     * 메시지를 다 쓸 때까지 쓴다. 통로가 받지 못하면 쓰기 가능해질 때까지 기다린다.
     *
     * @param passage 쓸 통로
     * @param message 쓸 메시지
     * @throws IOException 입출력 오류
     */
    public static void writeFully(Passage passage, OutboundMessage message) throws IOException {
        int offset = 0;
        if (message.length() == 0) {
            passage.write(message, 0);
            return;
        }
        while (offset < message.length()) {
            int accepted = passage.write(message, offset);
            offset += accepted;
            if (accepted == 0) {
                passage.await(-1, false, true);
            }
        }
    }

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
        return awaitOnAnotherThread(passage, timeoutMs, true, false);
    }

    private static CompletableFuture<Inbound> awaitOnAnotherThread(
            Passage passage, long timeoutMs, boolean read, boolean write) {
        CompletableFuture<Inbound> result = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        result.complete(passage.await(timeoutMs, read, write));
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
        return TestMessages.firstMessage();
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

        writeFully(peer(), message);

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

        writeFully(peer(), message);

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
        writeFully(peer(), message);
        byte[] firstRead = pollBytes(passage, length);
        byte[] snapshot = Arrays.copyOf(firstRead, firstRead.length);

        writeFully(peer(), message);
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

    /** 막힐 때까지 쓴다. 쓰다가 막히면 멈춘다. 받아 준 바이트 수를 돌려준다. */
    private static int writeUntilBlocked(Passage passage, List<OutboundMessage> messages) throws IOException {
        int accepted = 0;
        for (OutboundMessage message : messages) {
            int offset = 0;
            while (offset < message.length()) {
                int count = passage.write(message, offset);
                if (count == 0) {
                    return accepted;
                }
                offset += count;
                accepted += count;
            }
        }
        return accepted;
    }

    @Test
    void aWriteTakesOnlyWhatItCanTakeNowAndTellsHowMuchWithoutBlocking() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        Passage passage = createBlockable();
        CompletableFuture<Integer> writing = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        writing.complete(writeUntilBlocked(passage, messages));
                    } catch (IOException | RuntimeException e) {
                        writing.completeExceptionally(e);
                    }
                },
                "passage-contract-write");
        thread.setDaemon(true);
        thread.start();

        int accepted = within(writing);

        assertThat(accepted).isPositive().isLessThan(TestMessages.totalBytes(messages));
        passage.close();
        peer().close();
    }

    @Test
    void whatWasAcceptedInPartsArrivesIntactAndInOrder() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests().subList(0, 3);
        Passage passage = createBlockable();
        int total = TestMessages.totalBytes(messages);
        CompletableFuture<Void> writing = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        for (OutboundMessage message : messages) {
                            writeFully(passage, message);
                        }
                        writing.complete(null);
                    } catch (IOException | RuntimeException e) {
                        writing.completeExceptionally(e);
                    }
                },
                "passage-contract-write");
        thread.setDaemon(true);
        thread.start();

        // 막힘을 풀 때마다 상대가 읽은 것을 모은다.
        ByteArrayOutputStream received = new ByteArrayOutputStream();
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (received.size() < total && System.nanoTime() < limit) {
            byte[] drained = unblockWrites(passage);
            received.write(drained, 0, drained.length);
            Inbound inbound = peer().await(10);
            received.write(inbound.data(), 0, inbound.data().length);
        }

        within(writing);
        assertThat(received.toByteArray()).isEqualTo(TestMessages.concatenated(messages));
        passage.close();
        peer().close();
    }

    @Test
    void anAwaitForWritabilityEndsOnceTheBlockedWriteCanTakeMore() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        Passage passage = createBlockable();
        writeUntilBlocked(passage, messages);
        CompletableFuture<Inbound> waiting = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        waiting.complete(passage.await(-1, false, true));
                    } catch (IOException | RuntimeException e) {
                        waiting.completeExceptionally(e);
                    }
                },
                "passage-contract-await");
        thread.setDaemon(true);
        thread.start();
        assertThat(waiting).isNotDone();

        unblockWrites(passage);

        assertThat(within(waiting).kind()).isEqualTo(Inbound.Kind.NONE);
        passage.close();
        peer().close();
    }

    @Test
    void anAwaitForWritabilityReturnsAtOnceWhenTheWriteIsNotBlocked() throws Exception {
        Passage passage = create();

        assertThat(within(awaitOnAnotherThread(passage, -1, false, true)).kind())
                .isEqualTo(Inbound.Kind.NONE);
        passage.close();
    }

    @Test
    void aWakeupEndsAnAwaitForWritabilityThatIsStillBlocked() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        Passage passage = createBlockable();
        writeUntilBlocked(passage, messages);
        CompletableFuture<Inbound> waiting = awaitOnAnotherThread(passage, -1, false, true);
        assertThat(waiting).isNotDone();

        passage.wakeup();

        assertThat(within(waiting).kind()).isEqualTo(Inbound.Kind.NONE);
        passage.close();
        peer().close();
    }

    @Test
    void anAwaitThatDoesNotWaitForReadingNeitherWakesForNorTakesWhatThePeerWrote() throws Exception {
        Passage passage = create();
        OutboundMessage message = someMessage();
        CompletableFuture<Inbound> waiting = awaitOnAnotherThread(passage, -1, false, false);
        assertThat(waiting).isNotDone();

        writeFully(peer(), message);
        // 읽을 것이 생겼어도 읽기를 기다리지 않는 대기는 깨어나지 않는다. 깨움으로만 끝난다.
        assertThat(waiting).isNotDone();
        passage.wakeup();

        assertThat(within(waiting).kind()).isEqualTo(Inbound.Kind.NONE);
        assertThat(pollBytes(passage, message.bytes().length)).isEqualTo(message.bytes());
        passage.close();
        peer().close();
    }

    @Test
    void closingEndsAnAwaitForWritabilityWithAnIoError() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        Passage passage = createBlockable();
        writeUntilBlocked(passage, messages);
        CompletableFuture<Inbound> waiting = awaitOnAnotherThread(passage, -1, false, true);
        assertThat(waiting).isNotDone();

        passage.close();

        assertThatThrownBy(() -> within(waiting)).hasCauseInstanceOf(IOException.class);
        peer().close();
    }
}
