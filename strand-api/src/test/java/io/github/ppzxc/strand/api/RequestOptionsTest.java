package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RequestOptionsTest {
    @Test
    void distinguishesImmediateDeadlineFromNoDeadlineAndRejectsNegative() {
        assertFalse(RequestOptions.noDeadline().deadline().isPresent());
        assertEquals(
                Duration.ZERO,
                RequestOptions.deadlineAfter(Duration.ZERO).deadline().get());
        assertThrows(IllegalArgumentException.class, () -> RequestOptions.deadlineAfter(Duration.ofNanos(-1)));
    }
}
