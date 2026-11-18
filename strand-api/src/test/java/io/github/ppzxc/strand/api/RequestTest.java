package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RequestTest {
    @Test
    void attachesWireExtensionsWithoutAddingLocalTimeout() {
        Extensions extensions =
                Extensions.builder().idempotencyKey(new byte[] {9}).build();
        Request request = Request.of(256, new byte[0]).extensions(extensions);
        assertArrayEquals(new byte[] {9}, request.extensions().get(3).get());
        assertFalse(request.extensions().get(1).isPresent());
    }

    @Test
    void rejectsControlTypesAndOutOfRangeTypes() {
        assertThrows(IllegalArgumentException.class, () -> Request.of(255, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Request.of(65536, new byte[0]));
        assertEquals(256, Request.of(256, new byte[0]).type());
        assertEquals(65535, Request.of(65535, new byte[0]).type());
    }

    @Test
    void ownsPayloadCopy() {
        byte[] body = {1, 2};
        Request request = Request.of(256, body);
        body[0] = 9;
        request.payload()[1] = 9;
        assertArrayEquals(new byte[] {1, 2}, request.payload());
    }
}
