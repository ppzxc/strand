package example;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import org.junit.jupiter.api.Test;

@org.jspecify.annotations.NullMarked
class PublicApiTest {
    @Test
    void applicationCanConfigureBothScopes() {
        ConnectionLimits connection = ConnectionLimits.builder()
                .outgoingRequests(1, 100)
                .incomingRequests(1, 100)
                .responseBytes(100)
                .handlerTasks(1)
                .observations(1)
                .preparationBytes(100)
                .progressBytes(100, 100, 100, 100, 100, 100)
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
        assertEquals(1, limits.perOwner().get(Resource.TOTAL_CONNECTIONS));
        assertEquals(100, limits.perConnection().get(Resource.FRAME_ASSEMBLY_BYTES));
    }
}
