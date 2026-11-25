package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.util.List;
import java.util.Map;

interface Transmission {

    void deliver(EndpointDriver driver, byte[] bytes);

    void apply(EndpointDriver driver, JsonObject step, Map<String, Long> requestTickets);

    List<byte[]> transmit(List<OutboundMessage> sent);

    List<String> observeAfter(List<Event> events);
}
