package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ShutdownResultTest {
    @Test
    void retainsDeadlineOutcomeAndRemainingResourceSnapshot() {
        Map<Resource, Long> remaining = new EnumMap<>(Resource.class);
        remaining.put(Resource.HANDLER_TASKS, 2L);
        ShutdownResult result = ShutdownResult.deadlineExceeded(remaining);
        remaining.clear();
        assertEquals(ShutdownKind.DEADLINE_EXCEEDED, result.kind());
        assertEquals(2L, result.remainingResources().get(Resource.HANDLER_TASKS));
        assertThrows(
                UnsupportedOperationException.class,
                () -> result.remainingResources().clear());
    }
}
