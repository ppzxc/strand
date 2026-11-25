package io.github.ppzxc.strand.core.internal;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import org.jspecify.annotations.Nullable;

final class LivenessTimers {

    static final long NO_WAIT = 0;

    enum Kind {
        HELLO,
        AUTH,
        PING
    }

    enum Expiry {
        NONE,

        UNAUTHENTICATED,

        UNRESPONSIVE
    }

    private static final int PING_PAYLOAD_SIZE = 8;

    private static final class Wait {
        final Kind kind;
        final byte @Nullable [] payload;
        boolean started;
        long deadline;

        Wait(Kind kind, byte @Nullable [] payload) {
            this.kind = kind;
            this.payload = payload;
        }
    }

    private final long ackTimeoutMs;
    private final long authTimeoutMs;

    private final long idleIntervalMs;

    private final Map<Long, Wait> waits = new LinkedHashMap<>();

    private int pingCount;

    private long lastId = NO_WAIT;

    private @Nullable Long authDeadline;

    private long unsentIdleId = NO_WAIT;

    private long unsentSince;

    private boolean ended;
    private long lastReceivedAt;
    private long idlePingCount;

    LivenessTimers(long ackTimeoutMs, long authTimeoutMs, long idleIntervalMs) {
        this.ackTimeoutMs = ackTimeoutMs;
        this.authTimeoutMs = authTimeoutMs;
        this.idleIntervalMs = idleIntervalMs;
    }

    void authWaitStarted(long now) {
        authDeadline = now + authTimeoutMs;
    }

    void authWaitEnded() {
        authDeadline = null;
    }

    void frameReceived(long now) {
        lastReceivedAt = now;
    }

    long await(Kind kind, byte @Nullable [] payload) {
        long id = ++lastId;
        waits.put(id, new Wait(kind, payload));
        if (kind == Kind.PING) {
            pingCount++;
        }
        return id;
    }

    void idlePingQueued(long id, long now) {
        unsentIdleId = id;
        unsentSince = now;
    }

    void outputProgressed(long now) {
        if (unsentIdleId != NO_WAIT) {
            unsentSince = now;
        }
    }

    void started(long id, long now) {
        if (id == unsentIdleId) {
            unsentIdleId = NO_WAIT;
        }
        Wait wait = waits.get(id);
        if (wait != null && !wait.started) {
            wait.started = true;
            wait.deadline = now + ackTimeoutMs;
        }
    }

    void ackReceived(Kind kind, byte @Nullable [] payload) {
        for (Iterator<Map.Entry<Long, Wait>> it = waits.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Wait> entry = it.next();
            Wait wait = entry.getValue();
            if (wait.kind == kind && Arrays.equals(wait.payload, payload)) {
                it.remove();
                if (entry.getKey() == unsentIdleId) {
                    unsentIdleId = NO_WAIT;
                }
                if (kind == Kind.PING) {
                    pingCount--;
                }
                return;
            }
        }
    }

    boolean hasPing() {
        return pingCount > 0;
    }

    Expiry expiry(long now) {
        if (ended) {
            return Expiry.NONE;
        }
        if (authDeadline != null && now > authDeadline) {
            return Expiry.UNAUTHENTICATED;
        }
        for (Wait wait : waits.values()) {
            if (wait.started && now > wait.deadline) {
                return Expiry.UNRESPONSIVE;
            }
        }
        if (unsentIdleId != NO_WAIT && now > Deadlines.plus(unsentSince, ackTimeoutMs)) {
            return Expiry.UNRESPONSIVE;
        }
        return Expiry.NONE;
    }

    OptionalLong nextAt(boolean mayIdlePing) {
        if (ended) {
            return OptionalLong.empty();
        }
        long earliest = Long.MAX_VALUE;
        boolean found = false;
        if (authDeadline != null) {
            earliest = Deadlines.after(authDeadline);
            found = true;
        }
        for (Wait wait : waits.values()) {
            if (wait.started) {
                earliest = Math.min(earliest, Deadlines.after(wait.deadline));
                found = true;
            }
        }
        if (unsentIdleId != NO_WAIT) {
            earliest = Math.min(earliest, Deadlines.after(Deadlines.plus(unsentSince, ackTimeoutMs)));
            found = true;
        }
        if (mayIdlePing && idleIntervalMs > 0 && !hasPing()) {
            earliest = Math.min(earliest, Deadlines.plus(lastReceivedAt, idleIntervalMs));
            found = true;
        }
        return found ? OptionalLong.of(earliest) : OptionalLong.empty();
    }

    byte @Nullable [] idlePing(long now) {
        if (ended || idleIntervalMs <= 0 || now - lastReceivedAt < idleIntervalMs || hasPing()) {
            return null;
        }
        byte[] payload = new byte[PING_PAYLOAD_SIZE];
        for (int i = PING_PAYLOAD_SIZE - 1; i >= 0; i--) {
            payload[i] = (byte) (idlePingCount >>> (8 * (PING_PAYLOAD_SIZE - 1 - i)));
        }
        idlePingCount++;
        return payload;
    }

    void clear() {
        ended = true;
        authDeadline = null;
        unsentIdleId = NO_WAIT;
        waits.clear();
        pingCount = 0;
    }
}
