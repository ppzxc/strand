package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ResponseAcceptanceTest {
    @Test
    void identifiesExhaustedResource() {
        ResponseAcceptance shortage = ResponseAcceptance.resourceExhausted(Resource.RESPONSE_BYTES);
        assertEquals(AcceptanceKind.RESOURCE_EXHAUSTED, shortage.kind());
        assertEquals(Resource.RESPONSE_BYTES, shortage.resource().get());
        assertFalse(ResponseAcceptance.accepted().resource().isPresent());
    }
}
