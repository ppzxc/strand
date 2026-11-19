package io.github.ppzxc.strand.netty.internal;

import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

/** 수락한 소켓을 {@link StrandChannel}로 만드는 리스닝 채널. */
public final class StrandServerChannel extends NioServerSocketChannel {
    @Override
    protected int doReadMessages(List<Object> buf) throws Exception {
        SocketChannel accepted = javaChannel().accept();
        if (accepted == null) {
            return 0;
        }
        try {
            buf.add(new StrandChannel(this, accepted));
            return 1;
        } catch (RuntimeException e) {
            accepted.close();
            throw e;
        }
    }
}
