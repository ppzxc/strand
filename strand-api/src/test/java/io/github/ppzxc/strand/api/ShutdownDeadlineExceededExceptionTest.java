package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class ShutdownDeadlineExceededExceptionTest {
    @Test
    void exposesSameSnapshotAsShutdownResult() {
        ShutdownResult result = ShutdownResult.deadlineExceeded(Collections.singletonMap(Resource.HANDLER_TASKS, 1L));
        ShutdownDeadlineExceededException failure = new ShutdownDeadlineExceededException(result);
        assertEquals(result.remainingResources(), failure.remainingResources());
        assertThrows(
                IllegalArgumentException.class,
                () -> new ShutdownDeadlineExceededException(ShutdownResult.completed()));
    }
}
