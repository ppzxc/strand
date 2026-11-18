package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** TCP 위의 TLS 통로. 평문 소비량을 정확히 보고하고, ALPN·호스트 이름 검증 같은 규격 3.7절 보안 조건을 통로에서 확인한다. */
class TlsPassageTest {

    private TlsPassage client;
    private TlsPassage server;
    private TlsSizes sizes;

    @AfterEach
    void close() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    private void openPair() throws Exception {
        openPair(TestTls.client(), "localhost", true);
    }

    private void openPair(javax.net.ssl.SSLContext clientContext, String host, boolean alpn) throws Exception {
        TcpLoopback pair = TcpLoopback.open();
        sizes = TlsSizes.of(TestTls.client());
        int port = ((InetSocketAddress) pair.client.getRemoteAddress()).getPort();
        SSLEngine clientEngine;
        if (alpn) {
            clientEngine = TlsEngines.client(clientContext, host, port);
        } else {
            clientEngine = clientContext.createSSLEngine(host, port);
            clientEngine.setUseClientMode(true);
            SSLParameters parameters = clientEngine.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            clientEngine.setSSLParameters(parameters);
        }
        client = new TlsPassage(TcpPassage.of(pair.client, sizes.packetBufferSize()), clientEngine, sizes);
        server = new TlsPassage(
                TcpPassage.of(pair.accepted, sizes.packetBufferSize()), TlsEngines.server(TestTls.server()), sizes);
    }

    /**
     * 통로를 전송 스레드처럼 한 스레드에서 돌린다. {@code toWrite}를 다 소비하고 {@code expectedRead}바이트를 다 읽고 핸드셰이크가 끝나면
     * 멈추고, 읽은 바이트를 돌려준다. 쓰기와 읽기를 같은 스레드가 하므로 통로를 한 스레드만 부른다.
     */
    private static CompletableFuture<byte[]> drive(TlsPassage passage, byte[] toWrite, int expectedRead) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ByteArrayOutputStream read = new ByteArrayOutputStream();
                int offset = 0;
                long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while ((!passage.handshakeComplete()
                                || offset < toWrite.length
                                || read.size() < expectedRead
                                || passage.pendingOutput())
                        && System.nanoTime() < limit) {
                    offset += passage.writeBytes(toWrite, offset, toWrite.length - offset);
                    Inbound in = passage.await(20, true, offset < toWrite.length || passage.pendingOutput());
                    if (in.kind() == Inbound.Kind.BYTES) {
                        read.write(in.data(), 0, in.data().length);
                    }
                }
                return read.toByteArray();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static void pumpUntilFailure(TlsPassage passage) throws IOException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < limit) {
            passage.await(20, true, false);
        }
    }

    @Test
    void handshakeCompletesAndPlaintextRoundTripsInBothDirections() throws Exception {
        openPair();
        byte[] toServer = {1, 2, 3, 4, 5};
        byte[] toClient = {9, 8, 7};

        CompletableFuture<byte[]> clientRead = drive(client, toServer, toClient.length);
        CompletableFuture<byte[]> serverRead = drive(server, toClient, toServer.length);

        assertThat(serverRead.get(25, TimeUnit.SECONDS)).isEqualTo(toServer);
        assertThat(clientRead.get(25, TimeUnit.SECONDS)).isEqualTo(toClient);
        assertThat(client.handshakeComplete()).isTrue();
        assertThat(server.handshakeComplete()).isTrue();
    }

    @Test
    void writeConsumesAtMostOneRecordPerCallAndReportsExactlyWhatWasConsumed() throws Exception {
        openPair();
        byte[] payload = new byte[100_000];
        new Random(1).nextBytes(payload);
        CompletableFuture<byte[]> serverRead = drive(server, new byte[0], payload.length);

        int offset = 0;
        int largest = 0;
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while ((offset < payload.length || client.pendingOutput()) && System.nanoTime() < limit) {
            int consumed = client.writeBytes(payload, offset, payload.length - offset);
            assertThat(consumed).isBetween(0, payload.length - offset);
            largest = Math.max(largest, consumed);
            offset += consumed;
            client.await(20, true, offset < payload.length || client.pendingOutput());
        }

        assertThat(offset).isEqualTo(payload.length);
        // 한 번의 wrap은 레코드 하나(평문 16 KiB 이하)만 소비한다. 한꺼번에 다 받아 주지 않는다.
        assertThat(largest).isPositive().isLessThanOrEqualTo(sizes.applicationBufferSize());
        assertThat(serverRead.get(25, TimeUnit.SECONDS)).isEqualTo(payload);
    }

    @Test
    void writeConsumesNothingBeforeTheHandshakeIsComplete() throws Exception {
        openPair();

        assertThat(client.handshakeComplete()).isFalse();
        assertThat(client.writeBytes(new byte[] {1, 2, 3}, 0, 3)).isZero();
    }

    @Test
    void serverRejectsAClientThatDoesNotOfferTheStrandAlpnIdentifier() throws Exception {
        TcpLoopback pair = TcpLoopback.open();
        sizes = TlsSizes.of(TestTls.client());
        server = new TlsPassage(
                TcpPassage.of(pair.accepted, sizes.packetBufferSize()), TlsEngines.server(TestTls.server()), sizes);
        int port = ((InetSocketAddress) pair.client.getRemoteAddress()).getPort();
        SSLEngine plainClient = TestTls.client().createSSLEngine("localhost", port);
        plainClient.setUseClientMode(true);
        CompletableFuture<Void> rawClient = CompletableFuture.runAsync(() -> rawHandshake(pair.client, plainClient));

        assertThatThrownBy(() -> pumpUntilFailure(server))
                .isInstanceOf(SSLException.class)
                .hasMessageContaining("ALPN");
        pair.client.close();
        rawClient.cancel(true);
    }

    /** ALPN을 제시하지 않는 클라이언트의 핸드셰이크를 {@link TlsPassage} 없이 엔진만으로 끝까지 진행한다. */
    private static void rawHandshake(java.nio.channels.SocketChannel channel, SSLEngine engine) {
        try {
            engine.beginHandshake();
            java.nio.ByteBuffer in = java.nio.ByteBuffer.allocate(64 * 1024);
            java.nio.ByteBuffer out = java.nio.ByteBuffer.allocate(64 * 1024);
            java.nio.ByteBuffer app = java.nio.ByteBuffer.allocate(64 * 1024);
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < limit) {
                switch (engine.getHandshakeStatus()) {
                    case NEED_TASK:
                        Runnable task = engine.getDelegatedTask();
                        while (task != null) {
                            task.run();
                            task = engine.getDelegatedTask();
                        }
                        break;
                    case NEED_WRAP:
                        out.clear();
                        engine.wrap(java.nio.ByteBuffer.allocate(0), out);
                        out.flip();
                        while (out.hasRemaining()) {
                            channel.write(out);
                        }
                        break;
                    case NEED_UNWRAP:
                        in.flip();
                        javax.net.ssl.SSLEngineResult result = in.hasRemaining() ? engine.unwrap(in, app) : null;
                        in.compact();
                        if (result == null
                                || result.getStatus() == javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                            if (channel.read(in) < 0) {
                                return;
                            }
                        }
                        app.clear();
                        break;
                    default:
                        return;
                }
            }
        } catch (IOException e) {
            // 서버가 연결을 끊었다.
        }
    }

    @Test
    void clientRejectsAServerWhoseCertificateDoesNotMatchTheHostName() throws Exception {
        openPair(TestTls.client(), "wrong.example.invalid", true);
        CompletableFuture<Void> serverSide = CompletableFuture.runAsync(() -> {
            try {
                pumpUntilFailure(server);
            } catch (IOException e) {
                // 클라이언트가 핸드셰이크를 중단하면 서버도 실패한다.
            }
        });

        assertThatThrownBy(() -> pumpUntilFailure(client)).isInstanceOf(SSLException.class);
        serverSide.cancel(true);
    }

    @Test
    void clientRejectsAnUntrustedServerCertificate() throws Exception {
        openPair(TestTls.stranger(), "localhost", true);
        CompletableFuture<Void> serverSide = CompletableFuture.runAsync(() -> {
            try {
                pumpUntilFailure(server);
            } catch (IOException e) {
                // 클라이언트가 핸드셰이크를 중단하면 서버도 실패한다.
            }
        });

        assertThatThrownBy(() -> pumpUntilFailure(client)).isInstanceOf(SSLException.class);
        serverSide.cancel(true);
    }

    @Test
    void enginesNeverEnableProtocolsBelowTls12() throws Exception {
        SSLEngine clientEngine = TlsEngines.client(TestTls.client(), "localhost", 1);
        SSLEngine serverEngine = TlsEngines.server(TestTls.server());

        assertThat(Arrays.asList(clientEngine.getEnabledProtocols()))
                .isSubsetOf("TLSv1.2", "TLSv1.3")
                .isNotEmpty();
        assertThat(Arrays.asList(serverEngine.getEnabledProtocols()))
                .isSubsetOf("TLSv1.2", "TLSv1.3")
                .isNotEmpty();
        assertThat(clientEngine.getUseClientMode()).isTrue();
        assertThat(serverEngine.getUseClientMode()).isFalse();
        assertThat(clientEngine.getSSLParameters().getEndpointIdentificationAlgorithm())
                .isEqualTo("HTTPS");
    }

    @Test
    void peerCloseIsReportedAsEnd() throws Exception {
        openPair();
        CompletableFuture<byte[]> serverDone = drive(server, new byte[0], 0);
        drive(client, new byte[0], 0).get(25, TimeUnit.SECONDS);
        serverDone.get(25, TimeUnit.SECONDS);

        client.shutdownOutput();

        Inbound seen = Inbound.none();
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (seen.kind() != Inbound.Kind.END && System.nanoTime() < limit) {
            seen = server.await(50, true, false);
        }
        assertThat(seen.kind()).isEqualTo(Inbound.Kind.END);
    }

    @Test
    void pollWithoutInputReturnsNoneImmediately() throws Exception {
        openPair();

        assertThat(client.await(0).kind()).isEqualTo(Inbound.Kind.NONE);
    }
}
