package io.github.ppzxc.strand.nio.internal;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

/** 선택한 TLS 엔진의 버퍼 크기. 최소 진행 공간을 엔진에서 도출하는 입력이다. */
final class TlsSizes {
    private final int applicationBufferSize;
    private final int packetBufferSize;

    private TlsSizes(int applicationBufferSize, int packetBufferSize) {
        this.applicationBufferSize = applicationBufferSize;
        this.packetBufferSize = packetBufferSize;
    }

    /** 컨텍스트가 만드는 엔진이 알리는 크기를 읽는다. 엔진은 읽은 뒤 버린다. */
    static TlsSizes of(SSLContext context) {
        SSLEngine engine = context.createSSLEngine();
        try {
            SSLSession session = engine.getSession();
            return new TlsSizes(session.getApplicationBufferSize(), session.getPacketBufferSize());
        } finally {
            engine.closeOutbound();
        }
    }

    /** 복호한 평문 한 레코드를 담는 데 필요한 바이트(SSLSession.getApplicationBufferSize). */
    int applicationBufferSize() {
        return applicationBufferSize;
    }

    /** 암호문 한 패킷을 담는 데 필요한 바이트(SSLSession.getPacketBufferSize). */
    int packetBufferSize() {
        return packetBufferSize;
    }
}
