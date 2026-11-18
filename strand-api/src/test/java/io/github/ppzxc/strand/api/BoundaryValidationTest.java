package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class BoundaryValidationTest {
    @Test
    void rejectsInvalidExtensions() {
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().put(-1, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().put(65536, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().put(4, new byte[8189]));
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().put(1, new byte[3]));
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().put(3, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().put(3, new byte[65]));
        assertThrows(
                IllegalArgumentException.class,
                () -> Extensions.builder()
                        .put(4, new byte[8188])
                        .put(5, new byte[1])
                        .build());
        assertEquals(
                4,
                Extensions.builder().timeoutMillis(0xffffffffL).build().get(1).get().length);
    }

    @Test
    void rejectsInconsistentUsage() {
        assertThrows(IllegalArgumentException.class, () -> new ResourceUsage(0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ResourceUsage(1, -1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ResourceUsage(1, 0, 2, 0));
        assertThrows(IllegalArgumentException.class, () -> new ResourceUsage(1, 0, 0, -1));
        assertThrows(IllegalArgumentException.class, () -> new ObservationBudget(0));
    }

    @Test
    void validatesEndAndStatusRanges() {
        assertThrows(IllegalArgumentException.class, () -> RequestEnd.peerReset(-1));
        assertThrows(IllegalArgumentException.class, () -> RequestEnd.peerReset(0x100000000L));
        assertThrows(IllegalArgumentException.class, () -> new RequestResult(-1, Certainty.UNKNOWN, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new RequestResult(256, Certainty.UNKNOWN, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> CloseNotice.errorClose(0, CloseNotice.Sender.LOCAL, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> CloseNotice.errorClose(0x100000000L, CloseNotice.Sender.LOCAL, null));
        assertEquals(
                CloseNotice.Kind.CLOSE,
                CloseNotice.close(CloseNotice.Sender.LOCAL).kind());
    }
}
