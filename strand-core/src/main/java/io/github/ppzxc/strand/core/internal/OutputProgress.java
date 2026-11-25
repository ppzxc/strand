package io.github.ppzxc.strand.core.internal;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

final class OutputProgress {

    private final Outbox outbox;
    private final LivenessTimers timers;
    private final OwnRequestLedger ledger;

    private Outbox.@Nullable Node selected;
    private @Nullable EndpointOutput current;
    private long nextOutputId;
    private int written;
    private long waitId = LivenessTimers.NO_WAIT;
    private boolean disconnected;

    OutputProgress(Outbox outbox, LivenessTimers timers, OwnRequestLedger ledger) {
        this.outbox = outbox;
        this.timers = timers;
        this.ledger = ledger;
    }

    @Nullable
    EndpointOutput next() {
        if (disconnected) {
            return null;
        }
        Outbox.Node head = outbox.first();
        if (written == 0 && selected != head) {
            selected = head;
            current = head == null ? null : new EndpointOutput(++nextOutputId, head.message);
        }
        return current;
    }

    void progress(long id, int count, long now) {
        EndpointOutput output = next();
        if (output == null || output.id() != id || count < 0 || count > output.length() - written) {
            throw new IllegalArgumentException("유효하지 않은 출력 진행");
        }
        if (count == 0) {
            return;
        }
        if (written == 0) {
            start();
        }
        timers.outputProgressed(now);
        written += count;
        if (written == output.length()) {
            complete(now);
        }
    }

    long writingRemaining() {
        return current != null && written > 0 ? current.length() - written : 0;
    }

    boolean isPending() {
        return !disconnected && (written > 0 || outbox.size() > 0);
    }

    void disconnect() {
        disconnected = true;
        selected = null;
        current = null;
        written = 0;
        waitId = LivenessTimers.NO_WAIT;
        outbox.discard();
    }

    private void start() {
        Outbox.Node node = Objects.requireNonNull(selected);
        waitId = node.waitId;
        ledger.onWriteStarted(node);
        outbox.removeForWriting(node);
    }

    private void complete(long now) {
        if (waitId != LivenessTimers.NO_WAIT) {
            timers.started(waitId, now);
            waitId = LivenessTimers.NO_WAIT;
        }
        ledger.onWriteCompleted(Objects.requireNonNull(selected));
        written = 0;
        selected = null;
        current = null;
    }
}
