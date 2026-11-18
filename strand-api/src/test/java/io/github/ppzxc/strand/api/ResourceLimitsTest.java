package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ResourceLimitsTest {
    @Test
    void rejectsOwnerProgressSpaceSmallerThanOneConnection() {
        ConnectionLimits connection = ConnectionLimits.builder()
                .outgoingRequests(1, 100)
                .incomingRequests(1, 100)
                .responseBytes(100)
                .handlerTasks(1)
                .observations(1)
                .preparationBytes(100)
                .progressBytes(200, 100, 100, 100, 100, 100)
                .build();
        OwnerLimits owner = OwnerLimits.builder()
                .preparingConnections(1)
                .totalConnections(1)
                .preparationTasks(1, 1, 1)
                .outgoingRequests(1, 100)
                .incomingRequests(1, 100)
                .responseBytes(100)
                .handlerTasks(1)
                .observations(1)
                .preparationBytes(100)
                .progressBytes(100, 100, 100, 100, 100, 100)
                .build();
        assertThrows(
                IllegalArgumentException.class,
                () -> ResourceLimits.builder()
                        .perConnection(connection)
                        .perOwner(owner)
                        .build());
    }

    @Test
    void rejectsInsufficientProgressSpace() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ConnectionLimits.builder()
                        .outgoingRequests(1, 100)
                        .incomingRequests(1, 100)
                        .responseBytes(100)
                        .handlerTasks(1)
                        .observations(1)
                        .preparationBytes(100)
                        .progressBytes(1, 1, 1, 1, 1, 1)
                        .build());
    }

    @Test
    void requiresAllPositiveLimitsAndBothScopes() {
        assertThrows(
                IllegalStateException.class, () -> ConnectionLimits.builder().build());
        assertThrows(
                IllegalArgumentException.class, () -> ConnectionLimits.builder().handlerTasks(0));
        ConnectionLimits connection = ConnectionLimits.builder()
                .outgoingRequests(1, 100)
                .incomingRequests(1, 100)
                .responseBytes(100)
                .handlerTasks(1)
                .observations(1)
                .preparationBytes(100)
                .progressBytes(100, 100, 100, 100, 100, 100)
                .build();
        assertThrows(
                IllegalStateException.class,
                () -> ResourceLimits.builder().perConnection(connection).build());
    }
}
