package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import org.jspecify.annotations.Nullable;

/**
 * TCP 통로(암호문) 위에서 {@link SSLEngine}으로 평문 바이트 스트림을 만드는 TLS 통로. 평문을 읽고 쓰는 쪽에는 TCP 통로와 같은 {@link Passage}다.
 *
 * <p><b>평문 소비 보고.</b> {@link #writeBytes}가 돌려주는 값은 엔진이 실제로 소비한 평문 바이트 수다. 일반 write나 flush가 끝났다고 소비로 대신하지
 * 않는다. 프레임 하나를 한 번에 한 레코드씩 wrap하며, 보내는 암호문 공간이 비어 있을 때만 wrap한다(암호문 공간 예약 뒤 wrap). 평문을 소비한 뒤 암호문이
 * 아직 다 나가지 않은 상태가 있고({@link #pendingOutput}), 그 상태의 끊김은 상대가 받았는지 모른다.
 *
 * <p><b>저장 공간.</b> 받는 암호문 {@code netIn}(미완성 레코드의 나머지 한 패킷과 한 번에 읽는 조각 한 패킷), 복호한 평문(한 레코드), 보내는 데이터
 * 암호문 한 패킷, 핸드셰이크·경고 암호문 한 패킷이 전부다. {@link ProgressSpace}가 이 구성에서 최소치를 도출한다. 암호문은 wrap한 순서(순서 번호)
 * 그대로 내보낸다.
 *
 * <p><b>규격 3.7절.</b> 핸드셰이크가 끝나면 ALPN 식별자 {@value TlsEngines#ALPN_IDENTIFIER}가 협상됐는지 확인하고 아니면 연결을 거절한다. 호스트
 * 이름 검증과 프로토콜 하한은 엔진 설정이다({@link TlsEngines}). TLS 1.2에서 핸드셰이크가 끝난 뒤 다시 시작되는 핸드셰이크(재협상)는 거절한다.
 * 위임 작업(인증서 검증 등)은 이 통로를 부르는 전송 스레드에서 실행한다.
 *
 * <p>{@link #await}와 {@link #writeBytes}는 전송 스레드 하나만 부른다. {@link #wakeup}과 {@link #close}는 어느 스레드에서든 부른다.
 */
final class TlsPassage implements Passage {
    private final Passage transport;
    private final SSLEngine engine;
    private final ByteBuffer netIn;
    private final ByteBuffer dataOut;
    private final ByteBuffer controlOut;
    /** 보낼 암호문 버퍼를 wrap한 순서대로 둔다. 같은 버퍼가 두 번 들어가지 않는다. */
    private final ArrayDeque<ByteBuffer> outQueue = new ArrayDeque<>(2);

    private final ArrayDeque<byte[]> readyPlain = new ArrayDeque<>();
    private ByteBuffer appBuffer;
    private boolean handshakeDone;
    private boolean peerEnded;
    private boolean transportEnded;
    private boolean closeNotified;

    /**
     * @param transport 암호문을 주고받는 통로. 한 번에 읽는 조각이 {@code sizes.packetBufferSize()} 이하여야 한다
     * @param engine 핸드셰이크 전의 엔진({@link TlsEngines}가 만든 것)
     * @param sizes 엔진의 버퍼 크기
     * @throws SSLException 핸드셰이크를 시작할 수 없을 때
     */
    TlsPassage(Passage transport, SSLEngine engine, TlsSizes sizes) throws SSLException {
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

    /**
     * TLS 핸드셰이크가 끝났는가. 끝나기 전에는 평문을 소비하지 않는다.
     *
     * @return 끝났으면 true
     */
    boolean handshakeComplete() {
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
            // 쓸 수 있으니 기다리지 않고 돌려준다. 읽을 것이 이미 있으면 위에서 돌려줬다.
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
        if (dataOut.hasRemaining()) {
            outQueue.add(dataOut);
        }
        afterResult(result);
        flush();
        return result.bytesConsumed();
    }

    @Override
    public int write(OutboundMessage message, int offset) throws IOException {
        byte[] bytes = message.bytes();
        return writeBytes(bytes, offset, bytes.length - offset);
    }

    @Override
    public boolean pendingOutput() {
        return !outQueue.isEmpty();
    }

    /** 평문을 wrap해도 되는가: 핸드셰이크가 끝났고 데이터용 암호문 공간이 비어 있다. */
    private boolean canWrapData() {
        return handshakeDone
                && !outQueue.contains(dataOut)
                && engine.getHandshakeStatus() == HandshakeStatus.NOT_HANDSHAKING;
    }

    /** 핸드셰이크를 진행하고, 받아 둔 암호문을 평문으로 풀고, 보낼 암호문을 내보낸다. 더 할 일이 없을 때까지 돈다. */
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

    /** 핸드셰이크·경고 암호문을 wrap한다. 제어용 암호문 공간이 차 있으면 하지 않는다. */
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
        if (controlOut.hasRemaining()) {
            outQueue.add(controlOut);
        }
        afterResult(result);
        return controlOut.hasRemaining() || result.getHandshakeStatus() != HandshakeStatus.NEED_WRAP;
    }

    /** 받아 둔 암호문을 풀어 평문을 모은다. 무엇이든 소비하거나 만들었으면 true. */
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

    /** TLS 1.2에서 핸드셰이크가 끝난 뒤 다시 핸드셰이크가 시작되면(재협상) 거절한다. TLS 1.3의 사후 메시지(KeyUpdate 등)는 재협상이 아니다. */
    private void requireNoRenegotiation() throws SSLException {
        if (handshakeDone
                && engine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING
                && engine.getHandshakeStatus() != HandshakeStatus.FINISHED
                && !"TLSv1.3".equals(engine.getSession().getProtocol())
                && !closeNotified
                && !peerEnded) {
            throw new SSLException("TLS 재협상은 허용하지 않는다");
        }
    }

    /** 보낼 암호문을 통로가 받아 주는 만큼 내보낸다. wrap한 순서를 지킨다. */
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
            // 통로가 이미 끊겼거나 실패했다. 보내는 쪽을 닫으려던 것이므로 같은 결과다.
        }
        transport.shutdownOutput();
    }

    @Override
    public void close() {
        transport.close();
    }
}
