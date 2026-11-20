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

/**
 * 검증용 TLS 구동. core 출력 프레임을 하나씩 JDK {@link SSLEngine}으로 wrap해 실제 평문 소비량을 보고하고, 암호문은 별도 대기열에서 실제
 * 소켓 쓰기량대로 내보낸다. wrap 전에 암호문 공간(패킷 버퍼 하나)을 예약하며 예약 합계가 한도를 넘으면 wrap하지 않고 소비량 0을 보고한다. 모든
 * 메서드는 이벤트 루프에서 부른다.
 */
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
        // 검증용이므로 이벤트 루프에서 handshake가 끝날 때까지 기다린다. 제품 구현이 아니다.
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

    /** 이벤트 루프에서 core 명령 하나를 실행한 뒤 곧바로 출력을 이어 처리한다. */
    void command(Runnable command) {
        channel.eventLoop().execute(() -> {
            command.run();
            pump();
        });
    }

    /** 명령만 이벤트 루프에 넣는다. 한 번에 밀려 있던 여러 명령 뒤에 {@link #pump()}가 이어지는 경우를 재현한다. */
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
                    // 암호문 공간을 예약하지 못하면 wrap하지 않는다. 소비량 0은 시작이 아니다.
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

    /** 대기 중인 암호문을 실제 소켓 쓰기량만큼만 내보낸다. 0 반환이면 멈춘다. */
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

    /** TLS 종료를 시작한다: close_notify를 암호문 대기열 끝에 넣고, 모두 실제로 쓴 뒤 통로를 닫는다. */
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

    /** core가 아직 내보내지 못한(평문 소비가 끝나지 않은) 출력이 있다. */
    boolean hasPendingOutput() {
        return endpoint.nextOutput() != null;
    }

    int zeroCiphertextWrites() {
        return zeroCiphertextWrites;
    }

    /** 대기 중인 암호문 조각보다 적게 실제로 쓴 횟수. */
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
