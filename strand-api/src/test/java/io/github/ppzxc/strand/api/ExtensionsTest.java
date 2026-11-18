package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ExtensionsTest {
    @Test
    void validatesKnownWireExtensionsAndCopiesValues() {
        byte[] key = {1, 2};
        Extensions extensions =
                Extensions.builder().idempotencyKey(key).timeoutMillis(0).build();
        key[0] = 8;
        assertArrayEquals(new byte[] {1, 2}, extensions.get(3).get());
        byte[] read = extensions.get(3).get();
        read[0] = 7;
        assertArrayEquals(new byte[] {1, 2}, extensions.get(3).get());
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().idempotencyKey(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().timeoutMillis(-1));
        assertThrows(IllegalArgumentException.class, () -> Extensions.builder().timeoutMillis(0x100000000L));
    }
}
