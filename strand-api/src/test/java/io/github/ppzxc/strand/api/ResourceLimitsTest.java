package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ResourceLimitsTest {
    @Test
    void validatesRuntimeProgressRequirements() {
        ConnectionLimits limits = ConnectionLimits.builder()
                .outgoingRequests(1, 100)
                .incomingRequests(1, 100)
                .responseBytes(100)
                .handlerTasks(1)
                .observations(1)
                .preparationBytes(100)
                .progressBytes(100, 100, 100, 100, 100, 100)
                .build();
        assertThrows(IllegalArgumentException.class, () -> limits.requireProgress(101, 100, 100, 100, 100, 100));
        assertThrows(IllegalArgumentException.class, () -> limits.requireProgress(100, 100, 100, 100, 101, 100));
        limits.requireProgress(100, 100, 100, 100, 100, 100);
    }

    @Test
    void allowsOwnerCapSmallerThanConnectionCapWhenMinimumFits() {
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
        ResourceLimits limits = ResourceLimits.builder()
                .perConnection(connection)
                .perOwner(owner)
                .build();
        limits.requireProgress(100, 100, 100, 100, 100, 100);
        assertThrows(IllegalArgumentException.class, () -> limits.requireProgress(101, 100, 100, 100, 100, 100));
    }

    @Test
    void rejectsInsufficientProgressSpace() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ConnectionLimits.builder().progressBytes(100, 31, 100, 100, 100, 100));
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

    @Test
    void minimumProgressSpaceReportsEachResourceAndRejectsOthers() {
        MinimumProgressSpace space = MinimumProgressSpace.of(11, 22, 33, 0, 0, 0);
        assertEquals(11, space.get(Resource.FRAME_ASSEMBLY_BYTES));
        assertEquals(22, space.get(Resource.CONTROL_OUTPUT_BYTES));
        assertEquals(33, space.get(Resource.RESULT_EVENT_BYTES));
        assertEquals(0, space.get(Resource.TLS_PLAINTEXT_BYTES));
        assertEquals(0, space.get(Resource.TLS_CIPHERTEXT_BYTES));
        assertEquals(0, space.get(Resource.TLS_CONTROL_BYTES));
        assertThrows(IllegalArgumentException.class, () -> space.get(Resource.OUTGOING_REQUESTS));
    }

    @Test
    void shortfallMessageNamesResourceScopeRequiredAndConfigured() {
        ConnectionLimits connection = ConnectionLimits.builder()
                .outgoingRequests(1, 100)
                .incomingRequests(1, 100)
                .responseBytes(100)
                .handlerTasks(1)
                .observations(1)
                .preparationBytes(100)
                .progressBytes(100, 100, 100, 100, 100, 100)
                .build();
        IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class, () -> connection.requireProgress(100, 100, 101, 100, 100, 100));
        assertTrue(e.getMessage().contains("RESULT_EVENT_BYTES"), e.getMessage());
        assertTrue(e.getMessage().contains("per-connection"), e.getMessage());
        assertTrue(e.getMessage().contains("required=101"), e.getMessage());
        assertTrue(e.getMessage().contains("configured=100"), e.getMessage());
    }

    @Test
    void tlsProgressMayBeZeroForAPlainConnection() {
        ConnectionLimits.builder()
                .outgoingRequests(1, 100)
                .incomingRequests(1, 100)
                .responseBytes(100)
                .handlerTasks(1)
                .observations(1)
                .preparationBytes(100)
                .progressBytes(100, 100, 100, 0, 0, 0)
                .build()
                .requireProgress(100, 100, 100, 0, 0, 0);
    }
}
