package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.CloseReason;
import java.io.IOException;

public interface Passage {

    default Inbound await(long timeoutMs) throws IOException {
        return await(timeoutMs, true, false);
    }

    Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException;

    int writeBytes(byte[] bytes, int offset, int length) throws IOException;

    default boolean pendingOutput() {
        return false;
    }

    default void shutdownOutput(CloseReason reason) {
        shutdownOutput();
    }

    void wakeup();

    default void shutdownOutput() {
        close();
    }

    void close();
}
