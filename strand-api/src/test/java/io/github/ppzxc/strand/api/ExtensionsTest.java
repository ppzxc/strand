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

    @Test
    void encodesItemsByAscendingTagAndReadsThemBack() {
        Extensions extensions = Extensions.builder()
                .put(9, new byte[] {1})
                .timeoutMillis(0x01020304)
                .idempotencyKey(new byte[] {5, 6})
                .build();
        byte[] wire = extensions.toWire();
        assertArrayEquals(new byte[] {0, 1, 0, 4, 1, 2, 3, 4, 0, 3, 0, 2, 5, 6, 0, 9, 0, 1, 1}, wire);
        Extensions parsed = Extensions.fromWire(wire);
        assertArrayEquals(new byte[] {1}, parsed.get(9).get());
        assertArrayEquals(new byte[] {5, 6}, parsed.get(3).get());
        assertArrayEquals(wire, parsed.toWire());
    }

    @Test
    void emptyExtensionsEncodeToNothing() {
        assertEquals(0, Extensions.builder().build().toWire().length);
        assertTrue(Extensions.fromWire(new byte[0]).get(1).equals(java.util.Optional.empty()));
    }

    @Test
    void rejectsMalformedWireBytes() {
        assertThrows(IllegalArgumentException.class, () -> Extensions.fromWire(new byte[] {0, 9, 0}));
        assertThrows(IllegalArgumentException.class, () -> Extensions.fromWire(new byte[] {0, 9, 0, 2, 1}));
        assertThrows(IllegalArgumentException.class, () -> Extensions.fromWire(new byte[] {0, 9, 0, 0, 0, 9, 0, 0}));

        assertThrows(IllegalArgumentException.class, () -> Extensions.fromWire(new byte[] {0, 1, 0, 1, 1}));
    }
}
