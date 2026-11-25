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

public abstract class PassageContract {

    protected abstract Passage create();

    protected abstract Passage peer();

    protected abstract Passage createBlockable();

    protected abstract byte[] unblockWrites(Passage blocked) throws IOException;

    public static void writeFully(Passage passage, OutboundMessage message) throws IOException {
        int offset = 0;
        if (message.length() == 0) {
            TestMessages.write(passage, message, 0);
            return;
        }
        while (offset < message.length()) {
            int accepted = TestMessages.write(passage, message, offset);
            offset += accepted;
            if (accepted == 0) {
                passage.await(-1, false, true);
            }
        }
    }

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

        return TestMessages.firstMessage();
    }

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

    private static int writeUntilBlocked(Passage passage, List<OutboundMessage> messages) throws IOException {
        int accepted = 0;
        for (OutboundMessage message : messages) {
            int offset = 0;
            while (offset < message.length()) {
                int count = TestMessages.write(passage, message, offset);
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
