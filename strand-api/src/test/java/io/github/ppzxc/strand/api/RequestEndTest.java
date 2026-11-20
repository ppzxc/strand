package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RequestEndTest {
    @Test
    void distinguishesPeerResetFromConnectionEndAndAcceptance() {
        RequestEnd reset = RequestEnd.peerReset(4);
        assertTrue(reset.isPeerReset());
        assertEquals(4, reset.errorCode().getAsLong());
        assertFalse(reset.isConnectionEnd());
        assertTrue(RequestEnd.connectionEnd().isConnectionEnd());
        assertTrue(RequestEnd.responseAccepted().isResponseAccepted());
        assertFalse(RequestEnd.responseAccepted().errorCode().isPresent());
    }
}
