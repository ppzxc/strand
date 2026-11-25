package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.WeakHashMap;

final class EndpointFeed {
    private static final CollectionSpace SPACE = CollectionSpace.of(1 << 16, 1 << 16);
    private static final Map<Endpoint, EndpointDriver> DRIVERS = Collections.synchronizedMap(new WeakHashMap<>());

    private EndpointFeed() {}

    private static EndpointDriver driver(Endpoint endpoint) {
        return DRIVERS.computeIfAbsent(
                endpoint,
                e -> EndpointDriver.of(e)
                        .anonymousOnly(OptionalLong.empty())
                        .space(SPACE)
                        .eventBatch(1024)
                        .eagerEvents(true)
                        .unitsPerReceive(64));
    }

    static List<Event> receive(Endpoint endpoint, byte[] bytes) {
        EndpointDriver driver = driver(endpoint);
        driver.receiveStream(bytes);
        return driver.takeEvents();
    }

    static List<Event> drain(Endpoint endpoint) {
        return driver(endpoint).takeEvents();
    }
}
