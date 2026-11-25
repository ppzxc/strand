package io.github.ppzxc.strand.netty.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import org.jspecify.annotations.Nullable;

public final class TlsPipe {
    public interface Sink {
        int write(ByteBuffer bytes) throws IOException;
    }

    private static final ByteBuffer NOTHING = ByteBuffer.allocate(0);

    private final SSLEngine engine;
    private final Sink sink;
    private final ByteBufAllocator allocator;
    private final int packetSize;
    private final ByteBuffer netIn;
    private ByteBuffer appBuffer;
    private final ArrayDeque<ByteBuf> outQueue = new ArrayDeque<>(2);

    private @Nullable ByteBuf dataOut;
    private @Nullable ByteBuf controlOut;
    private final ArrayDeque<byte[]> readyPlain = new ArrayDeque<>();
    private final boolean requireAlpn;
    private boolean handshakeDone;
    private boolean peerEnded;
    private boolean closeNotified;

    public TlsPipe(SSLEngine engine, TlsSizes sizes, ByteBufAllocator allocator, Sink sink) throws SSLException {
        this(engine, sizes, allocator, sink, true);
    }

    public TlsPipe(SSLEngine engine, TlsSizes sizes, ByteBufAllocator allocator, Sink sink, boolean requireAlpn)
            throws SSLException {
        this.requireAlpn = requireAlpn;
        this.engine = engine;
        this.sink = sink;
        this.allocator = allocator;
        this.packetSize = sizes.packetBufferSize();
        this.netIn = ByteBuffer.allocate(2 * packetSize);
        this.netIn.flip();
        this.appBuffer = ByteBuffer.allocate(sizes.applicationBufferSize());
        engine.beginHandshake();
    }

    public boolean handshakeComplete() {
        return handshakeDone;
    }

    public boolean canAcceptRead() {
        return netIn.remaining() <= packetSize;
    }

    public boolean peerEnded() {
        return peerEnded;
    }

    public void feed(byte[] cipher) throws IOException {
        netIn.compact();
        if (cipher.length > netIn.remaining()) {
            netIn.flip();
            throw new IOException("암호문 조각이 받는 암호문 공간을 넘는다: " + cipher.length);
        }
        netIn.put(cipher);
        netIn.flip();
        pump();
    }

    public byte @Nullable [] pollPlain() {
        return readyPlain.poll();
    }

    public int writePlain(byte[] bytes, int offset, int length) throws IOException {
        pump();
        if (!handshakeDone || dataOut != null || length <= 0) {
            return 0;
        }
        ByteBuf slot = allocator.directBuffer(packetSize, packetSize);
        SSLEngineResult result;
        try {
            result = engine.wrap(ByteBuffer.wrap(bytes, offset, length), slot.nioBuffer(0, packetSize));
        } catch (SSLException | RuntimeException e) {
            slot.release();
            throw e;
        }
        slot.writerIndex(result.bytesProduced());
        dataOut = slot;
        outQueue.add(slot);
        afterResult(result);
        flush();
        return result.bytesConsumed();
    }

    public Object peerPrincipalOr(Object fallback) {
        try {
            return engine.getSession().getPeerPrincipal();
        } catch (SSLException e) {
            return fallback;
        }
    }

    public boolean pendingOutput() {
        return !outQueue.isEmpty();
    }

    public void pump() throws IOException {
        boolean progress;
        do {
            flush();
            progress = false;
            switch (engine.getHandshakeStatus()) {
                case NEED_TASK:
                    runTasks();
                    progress = true;
                    break;
                case NEED_WRAP:
                    progress = wrapControl();
                    break;
                default:
                    break;
            }
            progress |= unwrapBuffered();
            requireNoRenegotiation();
        } while (progress);
        flush();
    }

    private void runTasks() {
        Runnable task = engine.getDelegatedTask();
        while (task != null) {
            task.run();
            task = engine.getDelegatedTask();
        }
    }

    private boolean wrapControl() throws SSLException {
        if (controlOut != null) {
            return false;
        }
        ByteBuf slot = allocator.directBuffer(packetSize, packetSize);
        SSLEngineResult result;
        try {
            result = engine.wrap(NOTHING, slot.nioBuffer(0, packetSize));
        } catch (SSLException | RuntimeException e) {
            slot.release();
            throw e;
        }
        slot.writerIndex(result.bytesProduced());
        controlOut = slot;
        outQueue.add(slot);
        afterResult(result);
        return result.bytesProduced() > 0 || result.getHandshakeStatus() != HandshakeStatus.NEED_WRAP;
    }

    private boolean unwrapBuffered() throws SSLException {
        boolean progress = false;
        while (netIn.hasRemaining()) {
            if (peerEnded) {
                netIn.position(netIn.limit());
                break;
            }
            HandshakeStatus status = engine.getHandshakeStatus();
            if (status == HandshakeStatus.NEED_WRAP || status == HandshakeStatus.NEED_TASK) {
                break;
            }
            appBuffer.clear();
            SSLEngineResult result;
            try {
                result = engine.unwrap(netIn, appBuffer);
            } finally {
                appBuffer.flip();
            }
            if (appBuffer.hasRemaining()) {
                byte[] plain = new byte[appBuffer.remaining()];
                appBuffer.get(plain);
                readyPlain.add(plain);
            }
            afterResult(result);
            switch (result.getStatus()) {
                case BUFFER_UNDERFLOW:
                    return progress;
                case BUFFER_OVERFLOW:
                    appBuffer = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
                    break;
                case CLOSED:
                    peerEnded = true;
                    return true;
                default:
                    if (result.bytesConsumed() == 0) {
                        return progress;
                    }
                    progress = true;
                    break;
            }
        }
        return progress;
    }

    private void afterResult(SSLEngineResult result) throws SSLException {
        if (!handshakeDone && result.getHandshakeStatus() == HandshakeStatus.FINISHED) {
            handshakeDone = true;
            String protocol = TlsEngines.negotiatedProtocol(engine);
            if (requireAlpn && !TlsEngines.ALPN_IDENTIFIER.equals(protocol)) {
                throw new SSLException("ALPN 식별자 " + TlsEngines.ALPN_IDENTIFIER + "가 협상되지 않았다");
            }
        }
    }

    private void requireNoRenegotiation() throws SSLException {
        if (handshakeDone
                && engine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING
                && !"TLSv1.3".equals(engine.getSession().getProtocol())
                && !closeNotified
                && !peerEnded) {
            throw new SSLException("TLS 재협상은 허용하지 않는다");
        }
    }

    @SuppressWarnings("ReferenceEquality")
    private void flush() throws IOException {
        ByteBuf head = outQueue.peek();
        while (head != null) {
            while (head.isReadable()) {
                int accepted = sink.write(head.nioBuffer());
                if (accepted == 0) {
                    return;
                }
                head.skipBytes(accepted);
            }
            outQueue.poll();
            if (head == dataOut) {
                dataOut = null;
            } else {
                controlOut = null;
            }
            head.release();
            head = outQueue.peek();
        }
    }

    @SuppressWarnings("EmptyCatch")
    public void shutdownOutput() {
        try {
            closeNotified = true;
            engine.closeOutbound();
            pump();
        } catch (IOException | RuntimeException ignored) {
        }
    }

    public void release() {
        ByteBuf head = outQueue.poll();
        while (head != null) {
            head.release();
            head = outQueue.poll();
        }
        dataOut = null;
        controlOut = null;
    }
}
