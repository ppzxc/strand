package io.github.ppzxc.strand.integration.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

final class TlsOutputDriver {
    private final BoundaryChannel channel;
    private final Endpoint endpoint;
    private final SSLEngine engine;
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    private final List<ByteBuf> allocated = new ArrayList<>();
    private final Deque<ByteBuf> ciphertext = new ArrayDeque<>();
    private final List<Integer> plaintextReports = new ArrayList<>();
    private final List<Integer> ciphertextWrites = new ArrayList<>();
    private final int packetSize;
    private final int reservationLimit;
    private ByteBuffer plaintext;
    private long plaintextId = -1;
    private int zeroCiphertextWrites;
    private int partialCiphertextWrites;
    private int budgetBlocks;
    private boolean retryScheduled;
    private boolean closeAfterFlush;

    TlsOutputDriver(BoundaryChannel channel, Endpoint endpoint, SSLEngine engine, int reservedPackets) {
        this.channel = channel;
        this.endpoint = endpoint;
        this.engine = engine;
        this.packetSize = engine.getSession().getPacketBufferSize();
        this.reservationLimit = packetSize * reservedPackets;
        channel.closeFuture().addListener(future -> {
            endpoint.disconnect();
            releaseAll();
        });
    }

    static SSLEngine handshakeClient(SSLContext context, BoundaryChannel channel, String protocol) throws Exception {
        SSLEngine engine = context.createSSLEngine("localhost", 0);
        engine.setUseClientMode(true);
        engine.setEnabledProtocols(new String[] {protocol});
        engine.beginHandshake();
        ByteBuffer incoming = ByteBuffer.allocate(engine.getSession().getPacketBufferSize() * 2);
        ByteBuffer outgoing = ByteBuffer.allocate(engine.getSession().getPacketBufferSize() * 2);
        ByteBuffer application = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize() * 2);
        ByteBuffer empty = ByteBuffer.allocate(0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

        while (engine.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            if (System.nanoTime() > deadline) throw new IllegalStateException("TLS handshake timed out");
            switch (engine.getHandshakeStatus()) {
                case NEED_TASK:
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) task.run();
                    break;
                case NEED_WRAP:
                    outgoing.clear();
                    engine.wrap(empty, outgoing);
                    outgoing.flip();
                    while (outgoing.hasRemaining()) {
                        if (channel.writeAttempt(outgoing) == 0) Thread.yield();
                    }
                    break;
                case NEED_UNWRAP:
                    int n = channel.readAttempt(incoming);
                    if (n < 0) throw new IllegalStateException("peer closed during handshake");
                    incoming.flip();
                    engine.unwrap(incoming, application);
                    incoming.compact();
                    if (n == 0) Thread.yield();
                    break;
                default:
                    throw new IllegalStateException(engine.getHandshakeStatus().toString());
            }
        }
        return engine;
    }

    void command(Runnable command) {
        channel.eventLoop().execute(() -> {
            command.run();
            pump();
        });
    }

    void enqueue(Runnable command) {
        channel.eventLoop().execute(command);
    }

    void enqueuePump() {
        channel.eventLoop().execute(this::pump);
    }

    void pump() {
        if (!channel.isOpen()) return;
        try {
            while (true) {
                flushCiphertext();
                EndpointOutput output = endpoint.nextOutput();
                if (output == null) {
                    plaintext = null;
                    break;
                }
                if (reserved() + packetSize > reservationLimit) {

                    endpoint.outputProgress(output.id(), 0);
                    plaintextReports.add(0);
                    budgetBlocks++;
                    break;
                }
                if (output.id() != plaintextId) {
                    plaintext = ByteBuffer.wrap(output.bytes());
                    plaintextId = output.id();
                }
                ByteBuf reservation = allocator.directBuffer(packetSize);
                allocated.add(reservation);
                SSLEngineResult result = engine.wrap(plaintext, reservation.nioBuffer(0, packetSize));
                if (result.getStatus() != SSLEngineResult.Status.OK) {
                    throw new SSLException("unexpected wrap result " + result);
                }
                reservation.writerIndex(result.bytesProduced());
                if (result.bytesProduced() > 0) {
                    ciphertext.addLast(reservation);
                } else {
                    reservation.release();
                }
                endpoint.outputProgress(plaintextId, result.bytesConsumed());
                plaintextReports.add(result.bytesConsumed());
            }
            if (!ciphertext.isEmpty() || endpoint.nextOutput() != null) {
                scheduleRetry();
            } else if (closeAfterFlush) {
                channel.close();
            }
        } catch (IOException failure) {
            channel.close();
        }
    }

    private void flushCiphertext() throws IOException {
        while (!ciphertext.isEmpty()) {
            ByteBuf head = ciphertext.peekFirst();
            int count = channel.writeAttempt(head.nioBuffer());
            ciphertextWrites.add(count);
            if (count == 0) {
                zeroCiphertextWrites++;
                return;
            }
            if (count < head.readableBytes()) partialCiphertextWrites++;
            head.skipBytes(count);
            if (!head.isReadable()) {
                ciphertext.removeFirst();
                head.release();
            }
        }
    }

    private int reserved() {
        return ciphertext.size() * packetSize;
    }

    private void scheduleRetry() {
        if (retryScheduled) return;
        retryScheduled = true;
        channel.eventLoop()
                .schedule(
                        () -> {
                            retryScheduled = false;
                            pump();
                        },
                        1,
                        TimeUnit.MILLISECONDS);
    }

    void closeGracefully() {
        channel.eventLoop().execute(() -> {
            try {
                engine.closeOutbound();
                ByteBuf notify = allocator.directBuffer(packetSize);
                allocated.add(notify);
                SSLEngineResult result = engine.wrap(ByteBuffer.allocate(0), notify.nioBuffer(0, packetSize));
                notify.writerIndex(result.bytesProduced());
                ciphertext.addLast(notify);
                closeAfterFlush = true;
                pump();
            } catch (SSLException failure) {
                channel.close();
            }
        });
    }

    private void releaseAll() {
        for (ByteBuf buffer : ciphertext) buffer.release();
        ciphertext.clear();
    }

    boolean tlsOutboundOpen() {
        return !engine.isOutboundDone();
    }

    boolean tlsOutboundDone() {
        return engine.isOutboundDone();
    }

    int queuedCiphertextBytes() {
        int bytes = 0;
        for (ByteBuf buffer : ciphertext) bytes += buffer.readableBytes();
        return bytes;
    }

    List<Integer> plaintextReports() {
        return new ArrayList<>(plaintextReports);
    }

    List<Integer> ciphertextWrites() {
        return new ArrayList<>(ciphertextWrites);
    }

    boolean hasPendingOutput() {
        return endpoint.nextOutput() != null;
    }

    int zeroCiphertextWrites() {
        return zeroCiphertextWrites;
    }

    int partialCiphertextWrites() {
        return partialCiphertextWrites;
    }

    int budgetBlocks() {
        return budgetBlocks;
    }

    boolean allReleased() {
        for (ByteBuf buffer : allocated) {
            if (buffer.refCnt() != 0) return false;
        }
        return allocator.metric().usedDirectMemory() == 0;
    }
}
