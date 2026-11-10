package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.io.UncheckedIOException;

/** {@link TcpPassage}가 {@link PassageContract}를 지킨다. */
class TcpPassageContractTest extends PassageContract {

    private Passage peer;

    @Override
    protected Passage create() {
        try {
            TcpLoopback link = TcpLoopback.open();
            peer = TcpPassage.of(link.accepted);
            return TcpPassage.of(link.client);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected Passage peer() {
        return peer;
    }
}
