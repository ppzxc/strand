package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.io.UncheckedIOException;

/** {@link TcpPassage}가 {@link PassageContract}를 지킨다. */
class TcpPassageContractTest extends PassageContract {

    private static final int SMALL_SOCKET_BUFFER = 4096;

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

    @Override
    protected Passage createBlockable() {
        try {
            TcpLoopback link = TcpLoopback.open(SMALL_SOCKET_BUFFER);
            peer = TcpPassage.of(link.accepted);
            return TcpPassage.of(link.client);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 상대가 읽어 소켓 버퍼를 비운다. */
    @Override
    protected byte[] unblockWrites(Passage blocked) throws IOException {
        return peer.await(10).data();
    }
}
