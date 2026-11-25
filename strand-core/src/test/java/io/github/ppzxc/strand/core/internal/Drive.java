package io.github.ppzxc.strand.core.internal;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.WeakHashMap;

final class Drive {
    private static final Map<Endpoint, EndpointDriver> DRIVERS = Collections.synchronizedMap(new WeakHashMap<>());

    private Drive() {}

    private static EndpointDriver newDriver(Endpoint endpoint) {
        return EndpointDriver.of(endpoint).unitsPerReceive(1);
    }

    static Endpoint server(EndpointConfig config) {
        Endpoint endpoint = Endpoint.server(config);
        DRIVERS.put(endpoint, newDriver(endpoint).anonymousOnly(config.credentialLifetimeSeconds()));
        return endpoint;
    }

    static Endpoint server(EndpointConfig config, EndpointDriver.Authentication authentication) {
        Endpoint endpoint = Endpoint.server(config);
        OptionalLong lifetime = config.credentialLifetimeSeconds();
        DRIVERS.put(endpoint, newDriver(endpoint).authentication(new EndpointDriver.Authentication() {
            @Override
            public AuthDecision decide(String mechanism, byte[] credentials) {
                return authentication.decide(mechanism, credentials);
            }

            @Override
            public OptionalLong credentialLifetimeSeconds() {
                return lifetime;
            }
        }));
        return endpoint;
    }

    private static EndpointDriver driver(Endpoint endpoint) {
        return DRIVERS.computeIfAbsent(endpoint, Drive::newDriver);
    }

    static void receive(Endpoint endpoint, byte[] bytes) {
        driver(endpoint).receiveStream(bytes);
    }

    static void settle(Endpoint endpoint) {
        driver(endpoint).settle();
    }

    static List<OutboundMessage> drainMessages(Endpoint endpoint) {
        return driver(endpoint).flush();
    }

    static List<Event> drainEvents(Endpoint endpoint) {
        return driver(endpoint).takeEvents();
    }

    static void advance(Endpoint endpoint, long ms) {
        driver(endpoint).advanceMilliseconds(ms);
    }
}
