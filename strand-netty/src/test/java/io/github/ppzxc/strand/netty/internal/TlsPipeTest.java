package io.github.ppzxc.strand.netty.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.Test;

class TlsPipeTest {
    private static final byte[] HELLO = "hello strand".getBytes(StandardCharsets.UTF_8);

    private static final class Wire implements TlsPipe.Sink {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int budget = Integer.MAX_VALUE;
        boolean failing;

        @Override
        public int write(ByteBuffer source) throws IOException {
            if (failing) {
                throw new IOException("선이 끊겼다");
            }
            int count = Math.min(source.remaining(), budget);
            byte[] copy = new byte[count];
            source.get(copy);
            bytes.write(copy);
            budget = budget == Integer.MAX_VALUE ? budget : budget - count;
            return count;
        }

        byte[] take() {
            byte[] taken = bytes.toByteArray();
            bytes.reset();
            return taken;
        }
    }

    private final Wire toServer = new Wire();
    private final Wire toClient = new Wire();
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    private TlsSizes sizes;
    private TlsPipe client;
    private TlsPipe server;

    private void pipes(SSLEngine clientEngine, SSLEngine serverEngine, ByteBufAllocator clientAllocator)
            throws Exception {
        sizes = TlsSizes.of(TestTls.client());
        client = new TlsPipe(clientEngine, sizes, clientAllocator, toServer);
        server = new TlsPipe(serverEngine, sizes, allocator, toClient);
    }

    private void pipes() throws Exception {
        pipes(TlsEngines.client(TestTls.client(), "localhost", 1), TlsEngines.server(TestTls.server()), allocator);
    }

    private void deliver(Wire from, TlsPipe to) throws IOException {
        byte[] all = from.take();
        for (int offset = 0; offset < all.length; offset += sizes.packetBufferSize()) {
            int length = Math.min(sizes.packetBufferSize(), all.length - offset);
            byte[] piece = new byte[length];
            System.arraycopy(all, offset, piece, 0, length);
            to.feed(piece);
        }
    }

    private void handshake() throws IOException {
        for (int round = 0; round < 50 && !(client.handshakeComplete() && server.handshakeComplete()); round++) {
            client.pump();
            deliver(toServer, server);
            server.pump();
            deliver(toClient, client);
        }
        assertThat(client.handshakeComplete()).isTrue();
        assertThat(server.handshakeComplete()).isTrue();
    }

    @Test
    void plaintextWrittenByOneSideArrivesAtTheOtherAndTheReportedConsumptionIsExact() throws Exception {
        pipes();
        handshake();

        int consumed = client.writePlain(HELLO, 0, HELLO.length);
        deliver(toServer, server);

        assertThat(consumed).isEqualTo(HELLO.length);
        assertThat(server.pollPlain()).isEqualTo(HELLO);
        assertThat(server.pollPlain()).isNull();
        assertThat(client.pendingOutput()).isFalse();
        assertThat(server.canAcceptRead()).isTrue();
    }

    @Test
    void nothingIsConsumedBeforeTheHandshakeEndsOrForAnEmptyRequest() throws Exception {
        pipes();

        assertThat(client.writePlain(HELLO, 0, HELLO.length)).isZero();
        handshake();
        assertThat(client.writePlain(HELLO, 0, 0)).isZero();
    }

    @Test
    void ciphertextThatTheChannelDoesNotAcceptKeepsTheSingleDataSlotAndBlocksTheNextWrap() throws Exception {
        pipes();
        handshake();
        toServer.budget = 5;

        int first = client.writePlain(HELLO, 0, HELLO.length);
        int whileBlocked = client.writePlain(HELLO, 0, HELLO.length);

        assertThat(first).isEqualTo(HELLO.length);
        assertThat(client.pendingOutput()).isTrue();
        assertThat(whileBlocked).isZero();

        toServer.budget = Integer.MAX_VALUE;
        client.pump();
        deliver(toServer, server);

        assertThat(client.pendingOutput()).isFalse();
        assertThat(server.pollPlain()).isEqualTo(HELLO);
        assertThat(client.writePlain(HELLO, 0, HELLO.length)).isEqualTo(HELLO.length);
    }

    @Test
    void closeNotifyEndsThePeerAndLaterBytesAreDiscardedWithoutSpinning() throws Exception {
        pipes();
        handshake();

        client.shutdownOutput();
        deliver(toServer, server);
        assertThat(server.peerEnded()).isTrue();
        server.feed(new byte[] {23, 3, 3, 0, 1, 0});
        server.pump();

        assertThat(server.peerEnded()).isTrue();
        assertThat(server.pollPlain()).isNull();
    }

    @Test
    void anOversizedCiphertextPieceIsRejectedBecauseItExceedsTheReceiveSpace() throws Exception {
        pipes();

        assertThatThrownBy(() -> client.feed(new byte[2 * sizes.packetBufferSize() + 1]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("받는 암호문 공간");
    }

    @Test
    void aPeerThatDoesNotNegotiateTheStrandProtocolIsRejectedAfterTheHandshake() throws Exception {
        SSLEngine noAlpn = TestTls.client().createSSLEngine("localhost", 1);
        noAlpn.setUseClientMode(true);
        pipes(noAlpn, TlsEngines.server(TestTls.server()), allocator);

        assertThatThrownBy(this::handshake).isInstanceOf(SSLException.class).hasMessageContaining("ALPN");
    }

    @Test
    void renegotiationAfterTheHandshakeIsRefusedOnTls12() throws Exception {
        SSLEngine clientEngine = TlsEngines.client(TestTls.client(), "localhost", 1);
        SSLEngine serverEngine = TlsEngines.server(TestTls.server());
        clientEngine.setEnabledProtocols(new String[] {"TLSv1.2"});
        serverEngine.setEnabledProtocols(new String[] {"TLSv1.2"});
        pipes(clientEngine, serverEngine, allocator);
        handshake();

        clientEngine.beginHandshake();

        assertThatThrownBy(client::pump).isInstanceOf(SSLException.class).hasMessageContaining("재협상");
    }

    @Test
    void aPostHandshakeMessageOnTls13IsNotRenegotiation() throws Exception {
        SSLEngine clientEngine = TlsEngines.client(TestTls.client(), "localhost", 1);
        pipes(clientEngine, TlsEngines.server(TestTls.server()), allocator);
        handshake();
        assertThat(clientEngine.getSession().getProtocol()).isEqualTo("TLSv1.3");

        clientEngine.beginHandshake();
        client.pump();
        deliver(toServer, server);
        server.pump();
        deliver(toClient, client);

        assertThat(client.peerEnded()).isFalse();
        assertThat(client.writePlain(HELLO, 0, HELLO.length)).isEqualTo(HELLO.length);
    }

    @Test
    void releasingReturnsTheCiphertextBuffersThatNeverReachedTheChannel() throws Exception {
        pipes();
        handshake();
        toServer.budget = 0;
        client.writePlain(HELLO, 0, HELLO.length);
        assertThat(allocator.metric().usedDirectMemory()).isPositive();

        client.release();
        server.release();

        assertThat(allocator.metric().usedDirectMemory()).isZero();
        client.release();
    }

    @Test
    void thePeerPrincipalIsTheCertificateSubjectAfterTheHandshakeAndTheFallbackBefore() throws Exception {
        pipes();
        Object fallback = new Object();

        assertThat(client.peerPrincipalOr(fallback)).isSameAs(fallback);
        handshake();

        assertThat(client.peerPrincipalOr(fallback)).isNotSameAs(fallback);
        assertThat(client.peerPrincipalOr(fallback).toString()).contains("localhost");
    }

    @Test
    void aFailingChannelDuringTheCloseNotifyIsSwallowedBecauseTheOutputWasAlreadyEnding() throws Exception {
        pipes();
        handshake();
        toServer.failing = true;

        client.shutdownOutput();

        assertThat(client.pendingOutput()).isTrue();
        assertThatThrownBy(() -> client.writePlain(HELLO, 0, HELLO.length)).isInstanceOf(IOException.class);
    }

    @Test
    void readingStopsOnceMoreThanOnePacketOfAnUnfinishedRecordIsHeld() throws Exception {
        pipes();
        assertThat(client.canAcceptRead()).isTrue();
        byte[] first = new byte[sizes.packetBufferSize()];
        first[0] = 22;
        first[1] = 3;
        first[2] = 3;
        first[3] = (byte) (18000 >> 8);
        first[4] = (byte) 18000;
        client.feed(first);
        assertThat(client.canAcceptRead()).isTrue();

        client.feed(new byte[100]);

        assertThat(client.canAcceptRead()).isFalse();
    }

    @Test
    void aHandshakeMessageWaitsWhileTheChannelHoldsTheEarlierOneAndIsSentOnceItDrains() throws Exception {
        pipes();
        client.pump();
        toClient.budget = 0;

        deliver(toServer, server);
        server.pump();

        assertThat(server.pendingOutput()).isTrue();
        toClient.budget = Integer.MAX_VALUE;
        server.pump();
        assertThat(server.pendingOutput()).isFalse();
        handshake();
        assertThat(client.handshakeComplete()).isTrue();
    }
}
