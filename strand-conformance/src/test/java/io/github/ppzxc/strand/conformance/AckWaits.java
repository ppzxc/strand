package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

final class AckWaits {

    private static final int AUTH_REQUEST = 0x10005;

    private AckWaits() {}

    static boolean reachesLimitExactly(JsonObject vector) {
        for (Sample sample : samples(vector)) {
            if (sample.elapsed == sample.limit) {
                return true;
            }
        }
        return false;
    }

    static boolean startsAnyWait(JsonObject vector) {
        boolean server = isServerSide(vector);
        return anyStep(vector, step -> !startedBy(step, server).isEmpty());
    }

    static boolean startsAuthWait(JsonObject vector) {
        boolean server = isServerSide(vector);
        return anyStep(vector, step -> {
            for (Wait wait : startedBy(step, server)) {
                if (wait.type == Protocol.TYPE_AUTH || wait.type == AUTH_REQUEST) {
                    return true;
                }
            }
            return false;
        });
    }

    static boolean hasAdvance(JsonObject vector) {
        return anyStep(vector, step -> StepKind.of(step).advancesTime());
    }

    private static boolean anyStep(JsonObject vector, Predicate<JsonObject> matches) {
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            if (matches.test(step.getAsJsonObject())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isServerSide(JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        boolean client = setup.has("role") && setup.get("role").getAsString().equals("client");
        boolean established =
                setup.has("established") && setup.get("established").getAsBoolean();
        return !client && !established;
    }

    static List<Long> elapsedAtEachAdvance(JsonObject vector) {
        List<Long> result = new ArrayList<>();
        for (Sample sample : samples(vector)) {
            result.add(sample.elapsed);
        }
        return result;
    }

    static int maxLimitsCrossedInOneAdvance(JsonObject vector) {
        Map<Integer, Set<String>> crossed = new HashMap<>();
        int max = 0;
        for (Sample sample : samples(vector)) {
            if (sample.elapsed - sample.ms < sample.limit && sample.elapsed > sample.limit) {
                Set<String> reasons = crossed.computeIfAbsent(sample.step, k -> new HashSet<>());
                reasons.add(sample.reason);
                max = Math.max(max, reasons.size());
            }
        }
        return max;
    }

    private static List<Sample> samples(JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        long ackLimit = Protocol.configLimit(setup, Protocol.ACK_TIMEOUT_MS);
        long authLimit = Protocol.configLimit(setup, Protocol.AUTH_TIMEOUT_MS);
        boolean server = isServerSide(vector);
        List<Wait> running = new ArrayList<>();
        List<Sample> result = new ArrayList<>();
        InboundFrames stream = new InboundFrames();
        int index = -1;
        for (JsonElement element : vector.getAsJsonArray("steps")) {
            index++;
            JsonObject step = element.getAsJsonObject();

            for (Ack ack : acknowledgedBy(step, stream)) {
                endOldestMatching(running, ack);
            }
            running.addAll(startedBy(step, server));
            if (StepKind.of(step).advancesTime()) {
                long ms = step.get("ms").getAsLong();
                for (Wait wait : running) {
                    wait.elapsed += ms;
                    boolean authWait = wait.type == AUTH_REQUEST;
                    result.add(new Sample(
                            index,
                            wait.elapsed,
                            ms,
                            authWait ? authLimit : ackLimit,
                            authWait ? "UNAUTHENTICATED" : "UNRESPONSIVE"));
                }
            }
            if (Steps.endsConnection(step)) {

                running.clear();
                stream = new InboundFrames();
            }
        }
        return result;
    }

    private static List<Ack> acknowledgedBy(JsonObject step, InboundFrames stream) {
        List<Ack> acks = new ArrayList<>();
        for (InboundFrames.Frame frame : InboundFrames.receivedBy(step, stream)) {
            int type = frame.type;
            if (AuthGate.isAuthAck(frame)) {
                acks.add(new Ack(Protocol.TYPE_AUTH, null));
            } else if (AuthGate.isAuthRequest(frame)) {

                acks.add(new Ack(AUTH_REQUEST, null));
            } else if (frame.hasAckFlag() && (type == Protocol.TYPE_HELLO || type == Protocol.TYPE_PING)) {
                acks.add(new Ack(type, frame.pingSizedPayload()));
            }
        }
        return acks;
    }

    private static void endOldestMatching(List<Wait> running, Ack ack) {
        for (Iterator<Wait> it = running.iterator(); it.hasNext(); ) {
            if (ack.endsWait(it.next())) {
                it.remove();
                return;
            }
        }
    }

    private static List<Wait> startedBy(JsonObject step, boolean server) {
        List<Wait> waits = new ArrayList<>();
        for (JsonElement frame : step.getAsJsonArray("send")) {
            JsonObject f = frame.getAsJsonObject();
            String type = VectorSchema.frameType(f);
            if (type == null) {
                continue;
            }
            if (VectorSchema.hasFlag(f, "ACK")) {
                if (server && type.equals("HELLO")) {
                    waits.add(new Wait(AUTH_REQUEST, null));
                }
                continue;
            }
            if (type.equals("HELLO")) {
                waits.add(new Wait(Protocol.TYPE_HELLO, null));
            } else if (type.equals("PING")) {
                waits.add(new Wait(Protocol.TYPE_PING, pingPayload(step, f)));
            } else if (type.equals("AUTH")) {
                waits.add(new Wait(Protocol.TYPE_AUTH, null));
            }
        }
        return waits;
    }

    private static String pingPayload(JsonObject step, JsonObject frame) {
        JsonElement payload = StepKind.of(step).sendsPing() ? step.get("payload") : frame.get("payload");
        return payload != null
                        && payload.isJsonPrimitive()
                        && payload.getAsJsonPrimitive().isString()
                ? Steps.normalizedHex(payload.getAsString())
                : null;
    }

    private static final class Sample {
        final int step;
        final long elapsed;
        final long ms;
        final long limit;
        final String reason;

        Sample(int step, long elapsed, long ms, long limit, String reason) {
            this.step = step;
            this.elapsed = elapsed;
            this.ms = ms;
            this.limit = limit;
            this.reason = reason;
        }
    }

    private static final class Wait {
        final int type;
        final String payload;
        long elapsed;

        Wait(int type, String payload) {
            this.type = type;
            this.payload = payload;
        }
    }

    private static final class Ack {
        final int type;
        final String payload;

        Ack(int type, String payload) {
            this.type = type;
            this.payload = payload;
        }

        boolean endsWait(Wait wait) {
            if (wait.type != type) {
                return false;
            }
            return type != Protocol.TYPE_PING
                    || wait.payload == null
                    || (payload != null && payload.equals(wait.payload));
        }
    }
}
