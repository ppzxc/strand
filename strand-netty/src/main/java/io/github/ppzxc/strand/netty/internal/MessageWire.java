package io.github.ppzxc.strand.netty.internal;

import java.io.IOException;
import org.jspecify.annotations.Nullable;

public interface MessageWire {
    void start() throws IOException;

    void feed(byte[] bytes) throws IOException;

    boolean established();

    boolean hasMessages();

    @Nullable
    WireMessage pollMessage();

    byte @Nullable [] pollWire();

    boolean wirePending();

    void sendMessage(byte[] bytes) throws IOException;

    void sendClose(int status);

    boolean closeSent();

    void release();
}
