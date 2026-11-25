package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

public final class TestMessages {

    public static int write(Passage passage, OutboundMessage message, int offset) throws IOException {
        return passage.writeBytes(message.bytes(), offset, message.length() - offset);
    }

    private static final int APPLICATION_TYPE = 256;
    private static final int LARGE_BODY_BYTES = 60_000;
    private static final int LARGE_MESSAGES = 20;

    private static final CollectionSpace SPACE = CollectionSpace.of(1 << 16, 1 << 16);
    private static final int EVENT_BATCH = 1024;
    private static final Map<Endpoint, List<Event>> STASH = Collections.synchronizedMap(new WeakHashMap<>());

    private TestMessages() {}

    public static List<OutboundMessage> takeOutputs(Endpoint endpoint) {
        List<OutboundMessage> taken = new ArrayList<>();
        for (EndpointOutput output = endpoint.nextOutput(); output != null; output = endpoint.nextOutput()) {
            taken.add(output.message());
            endpoint.outputProgress(output.id(), output.length());
        }
        return taken;
    }

    public static void deliver(Endpoint endpoint, byte[] bytes) {
        int position = 0;
        while (position < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(bytes, position, bytes.length - position, 64, SPACE);
            position += progress.consumed();
            settle(endpoint);
            if (!progress.collectionRequired() && progress.consumed() == 0) {
                return;
            }
        }
    }

    private static void settle(Endpoint endpoint) {
        List<Event> stash = STASH.computeIfAbsent(endpoint, e -> new ArrayList<>());
        for (List<Event> batch = endpoint.drainEvents(EVENT_BATCH);
                !batch.isEmpty();
                batch = endpoint.drainEvents(EVENT_BATCH)) {
            for (Event event : batch) {
                if (event.kind() == Event.Kind.AUTHENTICATION_REQUIRED) {
                    boolean anonymous =
                            event.authenticationMechanism().orElse("").equals(EndpointConfig.ANONYMOUS)
                                    && event.body().length == 0;
                    endpoint.completeAuthentication(
                            event.authenticationId(), anonymous ? AuthDecision.PASS : AuthDecision.REJECT);
                } else {
                    stash.add(event);
                }
            }
        }
    }

    public static List<Event> drainEvents(Endpoint endpoint) {
        settle(endpoint);
        List<Event> stash = STASH.get(endpoint);
        List<Event> all = new ArrayList<>(stash);
        stash.clear();
        return all;
    }

    public static OutboundMessage firstMessage() {
        return Endpoint.client(EndpointConfig.defaults()).nextOutput().message();
    }

    public static List<OutboundMessage> largeRequests() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        List<OutboundMessage> requests = new ArrayList<>();
        for (int i = 0; i < LARGE_MESSAGES; i++) {
            client.request(APPLICATION_TYPE, new byte[LARGE_BODY_BYTES]);
        }
        boolean moved = true;
        while (moved) {
            moved = false;
            for (OutboundMessage message : takeOutputs(client)) {
                moved = true;
                if (message.length() >= LARGE_BODY_BYTES) {
                    requests.add(message);
                }
                deliver(server, message.bytes());
            }
            for (OutboundMessage message : takeOutputs(server)) {
                moved = true;
                deliver(client, message.bytes());
            }
        }
        assertThat(requests).hasSize(LARGE_MESSAGES);
        return requests;
    }

    public static int totalBytes(List<OutboundMessage> messages) {
        int total = 0;
        for (OutboundMessage message : messages) {
            total += message.length();
        }
        return total;
    }

    public static byte[] concatenated(List<OutboundMessage> messages) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (OutboundMessage message : messages) {
            byte[] bytes = message.bytes();
            out.write(bytes, 0, bytes.length);
        }
        return out.toByteArray();
    }
}
