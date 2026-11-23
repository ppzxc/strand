package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/** TCP 통로만의 동작: 읽기 버퍼 크기, 부분 쓰기와 백프레셔, 리셋, 보내는 쪽만 닫기. */
class TcpPassageTest {

    private static final int SMALL_SOCKET_BUFFER = 4096;
    /** 이 시간 동안 쓰기가 끝나지 않으면 막힌 것으로 본다. */
    private static final int SILENCE_MS = 300;

    private static <T> T within(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    /** 상대가 읽지 않는 동안 쓰기가 끝나지 않는다. 상한 부정 대기로 확인한다. */
    private static void assertStaysBlocked(CompletableFuture<Void> writing) {
        assertThatThrownBy(() -> writing.get(SILENCE_MS, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    private interface IoAction {
        void run() throws Exception;
    }

    private static CompletableFuture<Void> inBackground(IoAction action) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        action.run();
                        result.complete(null);
                    } catch (Exception e) {
                        result.completeExceptionally(e);
                    }
                },
                "tcp-passage-test");
        thread.setDaemon(true);
        thread.start();
        return result;
    }

    /**
     * 연결 드라이버처럼 한 스레드에서 메시지들을 쓰면서 읽는다. 쓰기가 막히면 쓰기 가능과 읽기를 함께 기다리고, 다 쓴 뒤에는 상대가 보낸 만큼 읽어
     * 모은다. 읽은 바이트를 돌려준다.
     */
    private static byte[] writeWhileReading(Passage passage, List<OutboundMessage> messages) throws IOException {
        ByteArrayOutputStream received = new ByteArrayOutputStream();
        int expected = TestMessages.totalBytes(messages);
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        int index = 0;
        int offset = 0;
        while ((index < messages.size() || received.size() < expected) && System.nanoTime() < limit) {
            while (index < messages.size()) {
                OutboundMessage message = messages.get(index);
                int accepted = TestMessages.write(passage, message, offset);
                offset += accepted;
                if (offset == message.length()) {
                    index++;
                    offset = 0;
                }
                if (accepted == 0) {
                    break;
                }
            }
            Inbound inbound = passage.await(100, true, index < messages.size());
            received.write(inbound.data(), 0, inbound.data().length);
        }
        return received.toByteArray();
    }

    /** 상한 시간 안에서 {@code count}바이트가 모일 때까지 기다리며 읽는다. */
    private static byte[] readExactly(Passage passage, int count) throws IOException {
        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (collected.size() < count) {
            if (System.nanoTime() > limit) {
                throw new AssertionError(count + "바이트 중 " + collected.size() + "바이트만 읽었다");
            }
            Inbound inbound = passage.await(100);
            collected.write(inbound.data(), 0, inbound.data().length);
        }
        return collected.toByteArray();
    }

    @Test
    void aSmallReadBufferHandsOverNoMoreThanItsSizePerRead() throws Exception {
        TcpLoopback link = TcpLoopback.open();
        Passage reader = TcpPassage.of(link.client, 3);
        link.accepted.write(ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7}));

        List<Integer> sizes = new ArrayList<>();
        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (collected.size() < 7 && System.nanoTime() < limit) {
            Inbound inbound = reader.await(100);
            if (inbound.kind() == Inbound.Kind.BYTES) {
                sizes.add(inbound.data().length);
                collected.write(inbound.data(), 0, inbound.data().length);
            }
        }

        assertThat(collected.toByteArray()).containsExactly(1, 2, 3, 4, 5, 6, 7);
        assertThat(sizes).allSatisfy(size -> assertThat(size).isBetween(1, 3));
        reader.close();
    }

    @Test
    void writingMoreThanTheSocketBuffersBlocksUntilThePeerReadsAndDeliversEverything() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        TcpLoopback link = TcpLoopback.open(SMALL_SOCKET_BUFFER);
        Passage writer = TcpPassage.of(link.client);
        Passage reader = TcpPassage.of(link.accepted);

        CompletableFuture<Void> writing = inBackground(() -> {
            for (OutboundMessage message : messages) {
                PassageContract.writeFully(writer, message);
            }
        });

        // 읽는 쪽이 읽기 전에는 다 쓸 수 없다(백프레셔). 읽기 시작하면 모두 닿고 쓰기가 끝난다.
        assertStaysBlocked(writing);
        byte[] received = readExactly(reader, TestMessages.totalBytes(messages));
        within(writing);
        assertThat(received).isEqualTo(TestMessages.concatenated(messages));
        writer.close();
        reader.close();
    }

    @Test
    void bothSidesWritingALotAtOnceBeforeReadingDoNotDeadlock() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        TcpLoopback link = TcpLoopback.open(SMALL_SOCKET_BUFFER);
        Passage a = TcpPassage.of(link.client);
        Passage b = TcpPassage.of(link.accepted);
        byte[][] receivedBy = new byte[2][];

        // 양쪽이 모두 다 쓰고 나서야 읽으려 한다. 한쪽이 쓰기에 막혀도 읽기를 멈추지 않으면 소켓 버퍼가 비어 서로의 쓰기가 풀린다.
        CompletableFuture<Void> aDone = inBackground(() -> receivedBy[0] = writeWhileReading(a, messages));
        CompletableFuture<Void> bDone = inBackground(() -> receivedBy[1] = writeWhileReading(b, messages));

        within(aDone);
        within(bDone);
        assertThat(receivedBy[0]).isEqualTo(TestMessages.concatenated(messages));
        assertThat(receivedBy[1]).isEqualTo(TestMessages.concatenated(messages));
        a.close();
        b.close();
    }

    @Test
    void closingWhileAWriteIsBlockedEndsTheWriteWithAnIoError() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        TcpLoopback link = TcpLoopback.open(SMALL_SOCKET_BUFFER);
        Passage writer = TcpPassage.of(link.client);
        CompletableFuture<Void> writing = inBackground(() -> {
            for (OutboundMessage message : messages) {
                PassageContract.writeFully(writer, message);
            }
        });
        assertStaysBlocked(writing);

        writer.close();

        assertThatThrownBy(() -> within(writing)).hasCauseInstanceOf(IOException.class);
        link.accepted.close();
    }

    @Test
    void aResetFromThePeerIsAnIoErrorOnRead() throws Exception {
        TcpLoopback link = TcpLoopback.open();
        Passage reader = TcpPassage.of(link.client);
        link.accepted.setOption(StandardSocketOptions.SO_LINGER, 0);

        link.accepted.close();

        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        IOException failure = null;
        while (failure == null && System.nanoTime() < limit) {
            try {
                reader.await(100);
            } catch (IOException e) {
                failure = e;
            }
        }
        assertThat(failure).isNotNull();
        reader.close();
    }

    @Test
    void shuttingDownOutputEndsThePeersReadButKeepsReadingWhatThePeerSends() throws Exception {
        TcpLoopback link = TcpLoopback.open();
        Passage passage = TcpPassage.of(link.client);
        Passage peer = TcpPassage.of(link.accepted);

        passage.shutdownOutput();

        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        Inbound seen = Inbound.none();
        while (seen.kind() != Inbound.Kind.END && System.nanoTime() < limit) {
            seen = peer.await(100);
        }
        assertThat(seen.kind()).isEqualTo(Inbound.Kind.END);
        OutboundMessage preface = TestMessages.firstMessage();
        PassageContract.writeFully(peer, preface);
        assertThat(readExactly(passage, preface.bytes().length)).isEqualTo(preface.bytes());
        passage.close();
        peer.close();
    }

    @Test
    void aReadBufferSmallerThanOneIsRejected() throws Exception {
        TcpLoopback link = TcpLoopback.open();

        assertThatThrownBy(() -> TcpPassage.of(link.client, 0)).isInstanceOf(IllegalArgumentException.class);
        link.client.close();
        link.accepted.close();
    }

    @Test
    void onceTheEndWasSeenEveryLaterAwaitReportsItAgain() throws Exception {
        TcpLoopback link = TcpLoopback.open();
        Passage passage = TcpPassage.of(link.client);
        link.accepted.close();

        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        Inbound seen = Inbound.none();
        while (seen.kind() != Inbound.Kind.END && System.nanoTime() < limit) {
            seen = passage.await(100);
        }

        assertThat(seen.kind()).isEqualTo(Inbound.Kind.END);
        assertThat(passage.await(0).kind()).isEqualTo(Inbound.Kind.END);
        assertThat(passage.await(-1).kind()).isEqualTo(Inbound.Kind.END);
        passage.close();
    }

    @Test
    void anEndSeenWhileAWriteIsBlockedDoesNotStopTheWriteAndIsReportedAfterwards() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        TcpLoopback link = TcpLoopback.open(SMALL_SOCKET_BUFFER);
        Passage writer = TcpPassage.of(link.client);
        link.accepted.shutdownOutput();
        CompletableFuture<Void> writing = inBackground(() -> {
            for (OutboundMessage message : messages) {
                PassageContract.writeFully(writer, message);
            }
        });

        // 상대는 보내는 쪽만 닫았을 뿐 계속 읽는다. 쓰기는 EOF를 만나도 끝까지 가야 한다.
        ByteBuffer sink = ByteBuffer.allocate(8192);
        int total = TestMessages.totalBytes(messages);
        int received = 0;
        while (received < total) {
            sink.clear();
            int count = link.accepted.read(sink);
            assertThat(count).isPositive();
            received += count;
        }

        within(writing);
        assertThat(writer.await(0).kind()).isEqualTo(Inbound.Kind.END);
        writer.close();
        link.accepted.close();
    }

    @Test
    void closingWhileAwaitingEndsTheAwaitWithAnIoError() throws Exception {
        TcpLoopback link = TcpLoopback.open();
        Passage passage = TcpPassage.of(link.client);
        CompletableFuture<Inbound> awaiting = new CompletableFuture<>();
        Thread thread = new Thread(
                () -> {
                    try {
                        awaiting.complete(passage.await(-1));
                    } catch (IOException e) {
                        awaiting.completeExceptionally(e);
                    }
                },
                "tcp-passage-await");
        thread.setDaemon(true);
        thread.start();
        // 선택기에서 실제로 기다리기 시작한 뒤에 닫는다. 그래야 닫힘이 기다림을 깨우는지를 본다.
        StackWatch.awaitFrame(thread, StackWatch::inSelector);

        passage.close();
        passage.close();

        assertThatThrownBy(() -> within(awaiting)).hasCauseInstanceOf(IOException.class);
        link.accepted.close();
    }
}
