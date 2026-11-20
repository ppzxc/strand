package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ResourceUsageTest {
    @Test
    void retainsCapacityCurrentHighWaterAndRejections() {
        ResourceUsage usage = new ResourceUsage(10, 3, 8, 4);
        assertEquals(10, usage.limit());
        assertEquals(3, usage.current());
        assertEquals(8, usage.highWater());
        assertEquals(4, usage.rejections());
        assertThrows(IllegalArgumentException.class, () -> new ResourceUsage(10, 9, 8, 0));
    }

    @Test
    void zeroLimitIsAcceptedOnlyForTlsProgressResources() {
        assertEquals(
                0, ResourceUsage.of(Resource.TLS_PLAINTEXT_BYTES, 0, 0, 0, 0).limit());
        assertEquals(
                0, ResourceUsage.of(Resource.TLS_CIPHERTEXT_BYTES, 0, 0, 0, 0).limit());
        assertEquals(0, ResourceUsage.of(Resource.TLS_CONTROL_BYTES, 0, 0, 0, 0).limit());
        assertThrows(IllegalArgumentException.class, () -> ResourceUsage.of(Resource.FRAME_ASSEMBLY_BYTES, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ResourceUsage(0, 0, 0, 0));
    }
}
