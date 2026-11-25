package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import org.jspecify.annotations.Nullable;

@SuppressWarnings("EmptyCatch")
public final class TlsPassage implements Passage {
    private final Passage transport;
    private final SSLEngine engine;
    private final ByteBuffer netIn;
    private final ByteBuffer dataOut;
    private final ByteBuffer controlOut;

    private final ArrayDeque<ByteBuffer> outQueue = new ArrayDeque<>(2);

    private final ArrayDeque<byte[]> readyPlain = new ArrayDeque<>();
    private ByteBuffer appBuffer;
    private boolean handshakeDone;
    private boolean peerEnded;
    private boolean transportEnded;
    private boolean closeNotified;

    public TlsPassage(Passage transport, SSLEngine engine, TlsSizes sizes) throws SSLException {
        this.transport = transport;
        this.engine = engine;
        int packet = sizes.packetBufferSize();
        this.netIn = ByteBuffer.allocate(2 * packet);
        this.netIn.flip();
        this.dataOut = emptyReadMode(packet);
        this.controlOut = emptyReadMode(packet);
        this.appBuffer = ByteBuffer.allocate(sizes.applicationBufferSize());
        engine.beginHandshake();
    }

    private static ByteBuffer emptyReadMode(int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.flip();
        return buffer;
    }

    public boolean handshakeComplete() {
        return handshakeDone;
    }

    @Override
    public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        pump();
        Inbound ready = nextReady();
        if (ready != null) {
            return ready;
        }
        if (peerEnded || transportEnded) {
            return Inbound.end();
        }
        boolean writeBlocked = pendingOutput() || (waitForWrite && !canWrapData());
        if (waitForWrite && !writeBlocked && !waitForRead) {
            return Inbound.none();
        }
        if (waitForWrite && !writeBlocked) {

            return transportPoll();
        }
        Inbound received = transport.await(timeoutMs, waitForRead, pendingOutput());
        return accept(received);
    }

    private Inbound transportPoll() throws IOException {
        return accept(transport.await(0, true, false));
    }

    private Inbound accept(Inbound received) throws IOException {
        switch (received.kind()) {
            case BYTES:
                feed(received.data());
                pump();
                Inbound ready = nextReady();
                return ready == null ? Inbound.none() : ready;
            case END:
                transportEnded = true;
                pump();
                Inbound last = nextReady();
                return last == null ? Inbound.end() : last;
            default:
                return Inbound.none();
        }
    }

    private @Nullable Inbound nextReady() {
        byte[] plain = readyPlain.poll();
        return plain == null ? null : Inbound.bytes(plain);
    }

    private void feed(byte[] cipher) throws IOException {
        netIn.compact();
        if (cipher.length > netIn.remaining()) {
            netIn.flip();
            throw new IOException("암호문 조각이 받는 암호문 공간을 넘는다: " + cipher.length);
        }
        netIn.put(cipher);
        netIn.flip();
    }

    @Override
    public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
        pump();
        if (!handshakeDone || !canWrapData() || length <= 0) {
            return 0;
        }
        dataOut.clear();
        SSLEngineResult result;
        try {
            result = engine.wrap(ByteBuffer.wrap(bytes, offset, length), dataOut);
        } finally {
            dataOut.flip();
        }
        outQueue.add(dataOut);
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

    @Override
    public boolean pendingOutput() {
        return !outQueue.isEmpty();
    }

    private boolean canWrapData() {
        return handshakeDone && !outQueue.contains(dataOut);
    }

    private void pump() throws IOException {
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
        if (outQueue.contains(controlOut)) {
            return false;
        }
        controlOut.clear();
        SSLEngineResult result;
        try {
            result = engine.wrap(ByteBuffer.allocate(0), controlOut);
        } finally {
            controlOut.flip();
        }
        outQueue.add(controlOut);
        afterResult(result);
        return result.bytesProduced() > 0 || result.getHandshakeStatus() != HandshakeStatus.NEED_WRAP;
    }

    private boolean unwrapBuffered() throws SSLException {
        boolean progress = false;
        while (netIn.hasRemaining()) {
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
                    if (result.bytesConsumed() == 0 && result.bytesProduced() == 0) {
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
            if (!TlsEngines.ALPN_IDENTIFIER.equals(protocol)) {
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

    private void flush() throws IOException {
        ByteBuffer head = outQueue.peek();
        while (head != null) {
            while (head.hasRemaining()) {
                int accepted =
                        transport.writeBytes(head.array(), head.arrayOffset() + head.position(), head.remaining());
                if (accepted == 0) {
                    return;
                }
                head.position(head.position() + accepted);
            }
            outQueue.poll();
            head = outQueue.peek();
        }
    }

    @Override
    public void wakeup() {
        transport.wakeup();
    }

    @Override
    public void shutdownOutput() {
        try {
            closeNotified = true;
            engine.closeOutbound();
            pump();
        } catch (IOException | RuntimeException ignored) {

        }
        transport.shutdownOutput();
    }

    @Override
    public void close() {
        transport.close();
    }
}
