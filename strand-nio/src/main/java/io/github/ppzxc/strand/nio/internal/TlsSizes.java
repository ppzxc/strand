package io.github.ppzxc.strand.nio.internal;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

public final class TlsSizes {
    private final int applicationBufferSize;
    private final int packetBufferSize;

    private TlsSizes(int applicationBufferSize, int packetBufferSize) {
        this.applicationBufferSize = applicationBufferSize;
        this.packetBufferSize = packetBufferSize;
    }

    public static TlsSizes of(SSLContext context) {
        SSLEngine engine = context.createSSLEngine();
        try {
            SSLSession session = engine.getSession();
            return new TlsSizes(session.getApplicationBufferSize(), session.getPacketBufferSize());
        } finally {
            engine.closeOutbound();
        }
    }

    public int applicationBufferSize() {
        return applicationBufferSize;
    }

    public int packetBufferSize() {
        return packetBufferSize;
    }
}
