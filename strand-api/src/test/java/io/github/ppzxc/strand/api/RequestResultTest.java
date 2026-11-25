package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RequestResultTest {
    @Test
    void localExhaustionIdentifiesTheResource() {
        RequestResult result = RequestResult.resourceExhausted(Resource.OUTGOING_BYTES);
        assertEquals(Status.RESOURCE_EXHAUSTED, result.status());
        assertEquals(Certainty.NOT_PROCESSED, result.certainty());
        assertEquals(Resource.OUTGOING_BYTES, result.resource().get());
    }

    @Test
    void retainsStatusCertaintyAndOwnedPayload() {
        byte[] bytes = {1, 2};
        RequestResult result = new RequestResult(Status.OK, Certainty.RESPONSE, bytes);
        bytes[0] = 9;
        byte[] returned = result.payload();
        returned[0] = 7;
        assertEquals(Status.OK, result.status());
        assertEquals(Certainty.RESPONSE, result.certainty());
        assertArrayEquals(new byte[] {1, 2}, result.payload());
    }
}
