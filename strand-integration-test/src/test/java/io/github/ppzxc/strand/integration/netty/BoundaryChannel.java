package io.github.ppzxc.strand.integration.netty;

import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Experimental channel for examining actual socket progress on the event loop. */
public final class BoundaryChannel extends NioSocketChannel {
    public int readAttempt(ByteBuffer bytes) throws IOException {
        if (!eventLoop().inEventLoop()) {
            throw new IllegalStateException("read must run on event loop");
        }
        return javaChannel().read(bytes);
    }

    public int writeAttempt(ByteBuffer bytes) throws IOException {
        if (!eventLoop().inEventLoop()) {
            throw new IllegalStateException("write must run on event loop");
        }
        return javaChannel().write(bytes);
    }
}
