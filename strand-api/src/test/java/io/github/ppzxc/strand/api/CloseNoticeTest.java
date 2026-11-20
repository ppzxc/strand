package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class CloseNoticeTest {
    @Test
    void distinguishesErrorCloseFromTransportDisconnect() {
        IllegalStateException cause = new IllegalStateException("transport");
        CloseNotice notice = CloseNotice.disconnected(cause);
        assertEquals(CloseNotice.Kind.DISCONNECTED, notice.kind());
        assertFalse(notice.sender().isPresent());
        assertSame(cause, notice.cause().get());
        CloseNotice error = CloseNotice.errorClose(1, CloseNotice.Sender.PEER, null);
        assertEquals(CloseNotice.Kind.ERROR_CLOSE, error.kind());
        assertEquals(1, error.errorCode());
        assertEquals(CloseNotice.Sender.PEER, error.sender().get());
    }
}
