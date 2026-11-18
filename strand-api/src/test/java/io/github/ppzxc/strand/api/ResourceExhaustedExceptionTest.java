package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ResourceExhaustedExceptionTest {
    @Test
    void identifiesResourceWithoutRevealingInput() {
        ResourceExhaustedException failure = new ResourceExhaustedException(Resource.PREPARATION_BYTES);
        assertEquals(Resource.PREPARATION_BYTES, failure.resource());
        assertFalse(failure.getMessage().isEmpty());
    }
}
