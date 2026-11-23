package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayDeque;
import javax.net.ssl.SSLEngine;
import org.junit.jupiter.api.Test;

/** 암호문 통로가 막히거나 끊기거나 규칙을 어기는 경계: 소비한 평문 보고와 끝남 처리가 정확해야 한다. */
class TlsPassageEdgeTest {

    /** 암호문 통로를 흉내 낸다: 받아 줄 바이트 수와 읽힐 것을 테스트가 정한다. */
    private static final class ScriptedTransport implements Passage {
        final ArrayDeque<Inbound> inbox = new ArrayDeque<>();
        int acceptLimit = Integer.MAX_VALUE;
        int written;
        boolean woken;
        boolean outputShutdown;
        boolean closed;

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) {
            Inbound next = inbox.poll();
            return next == null ? Inbound.none() : next;
        }

        @Override
        public int writeBytes(byte[] bytes, int offset, int length) {
            int count = Math.min(length, acceptLimit);
            written += count;
            return count;
        }

        @Override
        public void wakeup() {
            woken = true;
        }

        @Override
        public void shutdownOutput() {
            outputShutdown = true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static TlsPassage client(ScriptedTransport transport) throws Exception {
        TlsSizes sizes = TlsSizes.of(TestTls.client());
        SSLEngine engine = TlsEngines.client(TestTls.client(), "localhost", 1);
        return new TlsPassage(transport, engine, sizes);
    }

    @Test
    void ciphertextTheTransportCannotTakeStaysPendingAndNoPlaintextIsConsumed() throws Exception {
        ScriptedTransport transport = new ScriptedTransport();
        transport.acceptLimit = 0;
        TlsPassage passage = client(transport);

        passage.await(0, true, true);

        // ClientHello를 wrap했지만 통로가 한 바이트도 받지 않았다. 암호문이 남아 있고 평문은 소비하지 않았다.
        assertThat(passage.pendingOutput()).isTrue();
        assertThat(passage.writeBytes(new byte[] {1, 2, 3}, 0, 3)).isZero();
        assertThat(transport.written).isZero();
        transport.acceptLimit = 10;
        passage.await(0, true, true);
        assertThat(transport.written).isPositive();
    }

    @Test
    void aTransportEndWithoutCloseNotifyIsReportedAsEndEveryTime() throws Exception {
        ScriptedTransport transport = new ScriptedTransport();
        TlsPassage passage = client(transport);
        transport.inbox.add(Inbound.end());

        assertThat(passage.await(0, true, false).kind()).isEqualTo(Inbound.Kind.END);
        assertThat(passage.await(0, true, false).kind()).isEqualTo(Inbound.Kind.END);
    }

    @Test
    void aCiphertextChunkLargerThanTheReceiveSpaceIsAProtocolFailure() throws Exception {
        ScriptedTransport transport = new ScriptedTransport();
        TlsPassage passage = client(transport);
        transport.inbox.add(
                Inbound.bytes(new byte[3 * TlsSizes.of(TestTls.client()).packetBufferSize()]));

        assertThatThrownBy(() -> passage.await(0, true, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("암호문");
    }

    @Test
    void garbageInsteadOfARecordFailsTheHandshake() throws Exception {
        ScriptedTransport transport = new ScriptedTransport();
        TlsPassage passage = client(transport);
        transport.inbox.add(Inbound.bytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));

        assertThatThrownBy(() -> passage.await(0, true, false)).isInstanceOf(javax.net.ssl.SSLException.class);
    }

    @Test
    void wakeupShutdownOutputAndCloseReachTheTransport() throws Exception {
        ScriptedTransport transport = new ScriptedTransport();
        TlsPassage passage = client(transport);

        passage.wakeup();
        passage.shutdownOutput();
        passage.close();

        assertThat(transport.woken).isTrue();
        assertThat(transport.outputShutdown).isTrue();
        assertThat(transport.closed).isTrue();
    }

    @Test
    void messageWritesAreAdaptedToBytesWrites() throws Exception {
        ScriptedTransport transport = new ScriptedTransport();
        TlsPassage passage = client(transport);

        // 핸드셰이크 전에는 평문을 소비하지 않으므로 0이다.
        assertThat(TestMessages.write(passage, TestMessages.largeRequests().get(0), 0))
                .isZero();
        assertThat(passage.peerPrincipalOr("fallback")).isEqualTo("fallback");
    }
}
