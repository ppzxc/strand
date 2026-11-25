package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

public final class EndpointDriver {

    public interface Authentication {
        AuthDecision decide(String mechanism, byte[] credentials);

        default OptionalLong credentialLifetimeSeconds() {
            return OptionalLong.empty();
        }
    }

    public static final CollectionSpace ROOMY = CollectionSpace.of(Integer.MAX_VALUE, Integer.MAX_VALUE);

    private final Endpoint endpoint;
    private final List<Event> events = new ArrayList<>();
    private Authentication authentication;
    private CollectionSpace space = ROOMY;
    private int eventBatch = Integer.MAX_VALUE;
    private int unitsPerReceive = Integer.MAX_VALUE;
    private boolean messageChannel;
    private boolean eager;
    private boolean answered;
    private long elapsedNanos;

    private int written;

    private EndpointDriver(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public static EndpointDriver of(Endpoint endpoint) {
        return new EndpointDriver(endpoint);
    }

    public EndpointDriver authentication(Authentication authentication) {
        this.authentication = authentication;
        return this;
    }

    public EndpointDriver anonymousOnly(OptionalLong credentialLifetimeSeconds) {
        return authentication(new Authentication() {
            @Override
            public AuthDecision decide(String mechanism, byte[] credentials) {
                return mechanism.equals("ANONYMOUS") && credentials.length == 0
                        ? AuthDecision.PASS
                        : AuthDecision.REJECT;
            }

            @Override
            public OptionalLong credentialLifetimeSeconds() {
                return credentialLifetimeSeconds;
            }
        });
    }

    public EndpointDriver space(CollectionSpace space) {
        this.space = space;
        return this;
    }

    public EndpointDriver eventBatch(int eventBatch) {
        this.eventBatch = eventBatch;
        return this;
    }

    public EndpointDriver unitsPerReceive(int unitsPerReceive) {
        this.unitsPerReceive = unitsPerReceive;
        return this;
    }

    public EndpointDriver messageChannel(boolean messageChannel) {
        this.messageChannel = messageChannel;
        return this;
    }

    public EndpointDriver eagerEvents(boolean eager) {
        this.eager = eager;
        return this;
    }

    public Endpoint endpoint() {
        return endpoint;
    }

    public void receiveStream(byte[] bytes) {
        int at = 0;
        while (at < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(bytes, at, bytes.length - at, unitsPerReceive, space);
            at += progress.consumed();
            if (progress.collectionRequired()) {
                requireCollection(progress.consumed());
            } else {
                afterInput();
                if (progress.consumed() == 0) {
                    break;
                }
            }
        }
        afterInput();
    }

    public void receiveUnit(byte[] unit) {
        while (true) {
            ReceiveProgress progress = endpoint.receiveUnit(unit, 0, unit.length, space);
            if (!progress.collectionRequired()) {
                break;
            }
            requireCollection(progress.consumed());
        }
        afterInput();
    }

    public void receiveUnsupportedInput() {
        while (true) {
            ReceiveProgress progress = endpoint.receiveUnsupportedInput(space);
            if (!progress.collectionRequired()) {
                break;
            }
            requireCollection(progress.consumed());
        }
        afterInput();
    }

    private void requireCollection(int consumed) {
        if (collectEvents() == 0 && consumed == 0) {
            throw new IllegalStateException("수거 공간이 출력으로 막혔다");
        }
    }

    public void advanceMilliseconds(long milliseconds) {
        if (milliseconds < 0) {
            throw new IllegalArgumentException("경과 시간은 음수일 수 없다: " + milliseconds);
        }
        elapsedNanos += milliseconds * Deadlines.NANOS_PER_MILLI;
        endpoint.advanceTo(elapsedNanos);
        afterActivity();
    }

    public void settle() {
        afterInput();
    }

    public List<Event> takeEvents() {
        collectEvents();
        List<Event> taken = new ArrayList<>(events);
        events.clear();
        return taken;
    }

    public List<OutboundMessage> flush() {
        return flush(Long.MAX_VALUE);
    }

    public List<OutboundMessage> flush(long budgetBytes) {
        List<OutboundMessage> sent = new ArrayList<>();
        long budget = budgetBytes;
        while (true) {
            EndpointOutput output = endpoint.nextOutput();
            if (output == null) {
                break;
            }
            if (messageChannel) {
                endpoint.outputProgress(output.id(), output.length());
                sent.add(output.message());
                continue;
            }
            int remaining = output.length() - written;
            int take = (int) Math.min(budget, remaining);
            if (take > 0) {
                endpoint.outputProgress(output.id(), take);
            }
            budget -= take;
            if (take < remaining) {
                written += take;
                break;
            }
            written = 0;
            sent.add(output.message());
        }
        afterActivity();
        return sent;
    }

    private void afterInput() {
        if (eager || (authentication != null && !answered)) {
            collectEvents();
        }
    }

    private void afterActivity() {
        if (eager) {
            collectEvents();
        }
    }

    private int collectEvents() {
        int collected = 0;
        while (true) {
            List<Event> batch = endpoint.drainEvents(eventBatch);
            if (batch.isEmpty()) {
                return collected;
            }
            collected += batch.size();
            for (Event event : batch) {
                if (authentication != null && event.kind() == Event.Kind.AUTHENTICATION_REQUIRED) {
                    decideAuthentication(event);
                } else {
                    events.add(event);
                }
            }
        }
    }

    private void decideAuthentication(Event event) {
        AuthDecision decision;
        try {
            decision = authentication.decide(event.authenticationMechanism().orElse(""), event.body());
        } catch (RuntimeException e) {
            decision = AuthDecision.TRANSIENT_FAILURE;
        }
        endpoint.completeAuthentication(event.authenticationId(), decision, authentication.credentialLifetimeSeconds());
        answered = true;
    }
}
