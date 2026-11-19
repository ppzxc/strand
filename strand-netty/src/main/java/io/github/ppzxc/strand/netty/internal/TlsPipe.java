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

/**
 * 채널(암호문) 위에서 {@link SSLEngine}으로 평문 바이트 스트림을 만드는 TLS 구동. Netty {@code SslHandler}를 쓰지 않고, 출력 handler가 없는 채널에
 * 직접 쓰는 방식(#315가 검증한 {@code TlsOutputDriver})을 제품 구현으로 옮겼다. 모든 메서드는 채널의 이벤트 루프에서만 부른다.
 *
 * <p><b>평문 소비 보고.</b> {@link #writePlain}이 돌려주는 값은 엔진이 실제로 소비한 평문 바이트 수다. {@code Future} 완료나 flush를 소비로 대신하지
 * 않는다. 프레임 하나를 한 번에 한 레코드씩 wrap하며, 보내는 데이터 암호문 공간(패킷 버퍼 하나)이 비어 있을 때만 wrap한다(암호문 공간 예약 뒤 wrap).
 * 평문을 소비한 뒤 암호문이 아직 다 나가지 않은 상태가 있고({@link #pendingOutput}), 그 상태의 끊김은 상대가 받았는지 모른다.
 *
 * <p><b>저장 공간.</b> 받는 암호문 {@code netIn}(미완성 레코드의 나머지 한 패킷과 한 번에 읽는 조각 한 패킷), 복호한 평문(한 레코드), 보내는 데이터
 * 암호문 한 패킷, 핸드셰이크·경고 암호문 한 패킷이 전부다. {@link ProgressSpace}가 이 구성에서 최소치를 도출한다. 보내는 암호문은 직접
 * {@link ByteBuf}이고 wrap한 순서 그대로 내보내며, 다 쓰거나 {@link #release}가 불릴 때 정확히 한 번 반환한다.
 *
 * <p><b>규격 3.7절.</b> 핸드셰이크가 끝나면 ALPN 식별자 {@value TlsEngines#ALPN_IDENTIFIER}가 협상됐는지 확인하고 아니면 연결을 거절한다. 호스트
 * 이름 검증과 프로토콜 하한은 엔진 설정이다({@link TlsEngines}). TLS 1.2에서 핸드셰이크가 끝난 뒤 다시 시작되는 핸드셰이크(재협상)는 거절한다.
 * 위임 작업(인증서 검증 등)은 이 구동을 부르는 이벤트 루프에서 실행한다.
 */
public final class TlsPipe {
    /** 채널에 실제로 쓰기를 시도한다. 비차단이며 받아 준 바이트 수를 돌려준다(0이면 받아 주지 않은 것). */
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
    /** 보낼 암호문을 wrap한 순서대로 둔다. 데이터용과 제어용이 하나씩만 들어간다. */
    private final ArrayDeque<ByteBuf> outQueue = new ArrayDeque<>(2);

    private @Nullable ByteBuf dataOut;
    private @Nullable ByteBuf controlOut;
    private final ArrayDeque<byte[]> readyPlain = new ArrayDeque<>();
    private boolean handshakeDone;
    private boolean peerEnded;
    private boolean closeNotified;

    /**
     * @param engine 핸드셰이크 전의 엔진({@link TlsEngines}가 만든 것)
     * @param sizes 엔진의 버퍼 크기
     * @param allocator 보내는 암호문 버퍼를 만드는 할당기
     * @param sink 암호문을 채널에 쓰는 곳. 한 번에 읽는 조각은 {@code sizes.packetBufferSize()} 이하여야 한다
     * @throws SSLException 핸드셰이크를 시작할 수 없을 때
     */
    public TlsPipe(SSLEngine engine, TlsSizes sizes, ByteBufAllocator allocator, Sink sink) throws SSLException {
        this.engine = engine;
        this.sink = sink;
        this.allocator = allocator;
        this.packetSize = sizes.packetBufferSize();
        this.netIn = ByteBuffer.allocate(2 * packetSize);
        this.netIn.flip();
        this.appBuffer = ByteBuffer.allocate(sizes.applicationBufferSize());
        engine.beginHandshake();
    }

    /** TLS 핸드셰이크가 끝났는가. 끝나기 전에는 평문을 소비하지 않는다. */
    public boolean handshakeComplete() {
        return handshakeDone;
    }

    /** 다음 읽기 조각(암호문 패킷 하나)을 받을 공간이 있는가. 미완성 레코드 하나가 남은 것은 정상이고, 그 이상 쌓였을 때만 읽기를 멈춘다. */
    public boolean canAcceptRead() {
        return netIn.remaining() <= packetSize;
    }

    /** 상대가 close_notify로 TLS를 끝냈는가. */
    public boolean peerEnded() {
        return peerEnded;
    }

    /**
     * 받은 암호문 조각을 쌓고 풀 수 있는 만큼 평문으로 푼다. 핸드셰이크도 진행한다.
     *
     * @param cipher 받은 암호문
     * @throws IOException 조각이 받는 암호문 공간을 넘거나 TLS 오류일 때
     */
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

    /** 풀어 둔 평문 조각 하나. 없으면 null. */
    public byte @Nullable [] pollPlain() {
        return readyPlain.poll();
    }

    /**
     * 평문 일부를 wrap해 소비한 양을 돌려준다. 핸드셰이크가 끝나지 않았거나 데이터 암호문 공간이 차 있으면 0이다.
     *
     * @return 엔진이 소비한 평문 바이트 수
     */
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
        // 만든 암호문이 없어도 슬롯을 큐에 넣는다. flush가 비어 있는 슬롯을 그 자리에서 반환한다.
        dataOut = slot;
        outQueue.add(slot);
        afterResult(result);
        flush();
        return result.bytesConsumed();
    }

    /**
     * 핸드셰이크가 확인한 상대 주체. 클라이언트는 서버 인증서의 주체다. 확인하지 못했으면 {@code fallback}이다.
     *
     * @param fallback 확인한 주체가 없을 때 돌려줄 값
     * @return 상대 주체 또는 {@code fallback}
     */
    public Object peerPrincipalOr(Object fallback) {
        try {
            return engine.getSession().getPeerPrincipal();
        } catch (SSLException e) {
            return fallback;
        }
    }

    /** 평문은 소비했지만 암호문이 아직 다 나가지 않았는가. */
    public boolean pendingOutput() {
        return !outQueue.isEmpty();
    }

    /** 핸드셰이크를 진행하고, 받아 둔 암호문을 평문으로 풀고, 보낼 암호문을 내보낸다. 더 할 일이 없을 때까지 돈다. */
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

    /** 핸드셰이크·경고 암호문을 wrap한다. 제어용 암호문 공간이 차 있으면 하지 않는다. */
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

    /** 받아 둔 암호문을 풀어 평문을 모은다. 무엇이든 소비하거나 만들었으면 true. */
    private boolean unwrapBuffered() throws SSLException {
        boolean progress = false;
        while (netIn.hasRemaining()) {
            if (peerEnded) {
                // 상대가 TLS를 끝낸 뒤 남은 바이트는 버린다. 엔진이 CLOSED만 되풀이해 돌려주므로 풀지 않는다.
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
            if (!TlsEngines.ALPN_IDENTIFIER.equals(protocol)) {
                throw new SSLException("ALPN 식별자 " + TlsEngines.ALPN_IDENTIFIER + "가 협상되지 않았다");
            }
        }
    }

    /** TLS 1.2에서 핸드셰이크가 끝난 뒤 다시 핸드셰이크가 시작되면(재협상) 거절한다. TLS 1.3의 사후 메시지(KeyUpdate 등)는 재협상이 아니다. */
    private void requireNoRenegotiation() throws SSLException {
        if (handshakeDone
                && engine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING
                && !"TLSv1.3".equals(engine.getSession().getProtocol())
                && !closeNotified
                && !peerEnded) {
            throw new SSLException("TLS 재협상은 허용하지 않는다");
        }
    }

    /** 보낼 암호문을 채널이 받아 주는 만큼 내보낸다. wrap한 순서를 지키고, 다 나간 버퍼는 그 자리에서 반환한다. */
    @SuppressWarnings("ReferenceEquality") // ByteBuf의 equals는 내용 비교다. 데이터·제어 암호문 슬롯을 같은 객체로 구별한다.
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

    /** 보내는 쪽을 TLS 수준에서 닫는다: close_notify를 만들어 나머지 암호문 뒤에 내보낸다. */
    public void shutdownOutput() {
        try {
            closeNotified = true;
            engine.closeOutbound();
            pump();
        } catch (IOException | RuntimeException ignored) {
            // 채널이 이미 끊겼거나 실패했다. 보내는 쪽을 닫으려던 것이므로 같은 결과다.
        }
    }

    /** 아직 내보내지 못한 암호문 버퍼를 모두 반환한다. 채널이 정리될 때 한 번 부른다. */
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
