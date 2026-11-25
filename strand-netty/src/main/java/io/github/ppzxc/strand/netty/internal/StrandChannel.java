package io.github.ppzxc.strand.netty.internal;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class StrandChannel extends NioSocketChannel {
    public StrandChannel() {
        super();
    }

    public StrandChannel(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    public int writeAttempt(ByteBuffer bytes) throws IOException {
        return javaChannel().write(bytes);
    }

    public void shutdownOutputNow() throws IOException {
        javaChannel().shutdownOutput();
    }

    public boolean socketHeld() {
        return javaChannel().isRegistered();
    }
}
