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

/**
 * 벡터에서 확인 대기(HELLO·PING·AUTH를 보내고 ACK를 기다리는 시간, 규격 3.3절)와 서버의 AUTH 대기(상대 HELLO에 ACK를 보낸 때부터
 * AUTH를 기다리는 시간, 규격 3.3절)의 구간을 해석한다. 확인 대기는 HELLO/PING/AUTH 송신 단계에서 시작해 ACK를 받는 단계에서 끝난다.
 * HELLO ACK는 가장 먼저 시작한 HELLO 대기를, PING ACK는 payload가 같은 PING 대기를, AUTH(ACK)는 AUTH 대기를 끝낸다. AUTH 대기는
 * ACK HELLO를 보내는 단계에서 시작해(클라이언트 역할은 AUTH를 기다리지 않는다) AUTH를 받는 단계에서 끝난다. 끝나지 않으면 벡터
 * 끝까지 이어진다. 확인 대기의 한도는 `ack_timeout_ms`, AUTH 대기의 한도는 `auth_timeout_ms`다.
 *
 * <p>ACK를 알아보지 못하면 대기를 끝내지 않은 것으로 본다(대기가 길어지는 쪽이라 한도 검사가 더 엄격해진다). 바이트는 `recv`
 * 단계에 걸쳐 이어 읽고(프레임 경계와 무관하게 잘려 올 수 있다), 완결된 프레임에서만 ACK를 찾는다. `recv_message`는 binary
 * 메시지 하나를 따로 읽고 text 메시지는 읽지 않는다. 프레임 길이가 16 미만(규격 2.2.1절의 최솟값)이면 헤더를 읽을 수 없는 깨진 바이트이므로 그
 * 연결의 나머지는 읽지 않는다.
 */
final class AckWaits {

    /** 받은 `AUTH`(ACK 아님)와 그것을 기다리는 서버의 AUTH 대기를 나타내는 값. 프레임 `type`(16비트)에 없는 값이다. */
    private static final int AUTH_REQUEST = 0x10005;

    private AckWaits() {}

    /** 구조를 해석해 대기 구간별로 센 경과 시간 중 그 대기의 한도(`ack_timeout_ms`·`auth_timeout_ms`의 기본값은 {@link Protocol}에 있다)에 정확히 닿는 것이 있는가. */
    static boolean reachesLimitExactly(JsonObject vector) {
        for (Sample sample : samples(vector)) {
            if (sample.elapsed == sample.limit) {
                return true;
            }
        }
        return false;
    }

    /** 벡터가 확인 대기나 AUTH 대기를 하나라도 시작하는가. */
    static boolean startsAnyWait(JsonObject vector) {
        boolean server = isServerSide(vector);
        return anyStep(vector, step -> !startedBy(step, server).isEmpty());
    }

    /** 벡터가 AUTH 대기(클라이언트의 `AUTH(ACK)` 대기나 서버의 `AUTH` 대기)를 시작하는가. */
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

    /** 벡터에 advance 단계가 있는가. */
    static boolean hasAdvance(JsonObject vector) {
        return anyStep(vector, step -> InboundFrames.kindOf(step).equals("advance"));
    }

    private static boolean anyStep(JsonObject vector, Predicate<JsonObject> matches) {
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            if (matches.test(step.getAsJsonObject())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 이 벡터의 서버가 AUTH를 기다리는가. 서버 쪽(`role`이 `client`가 아니면, 없으면 포함)이고 AUTH 관문을 아직 지나지 않은 채
     * 시작하는(`established`가 `true`가 아닌) 벡터만 해당한다.
     */
    private static boolean isServerSide(JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        boolean client = setup.has("role") && setup.get("role").getAsString().equals("client");
        boolean established =
                setup.has("established") && setup.get("established").getAsBoolean();
        return !client && !established;
    }

    /**
     * 대기가 시작된 시점부터 센 경과 시간을 advance 단계마다 모아 돌려준다. 대기 여러 개가 겹치면 각각 따로 센다. 대기가 시작된
     * 단계 자체가 advance이면 그 단계의 시간을 대기 안에 센다(송신이 advance 앞에서 일어났다고 보는, 한도에 더 가까운 쪽의 해석).
     */
    static List<Long> elapsedAtEachAdvance(JsonObject vector) {
        List<Long> result = new ArrayList<>();
        for (Sample sample : samples(vector)) {
            result.add(sample.elapsed);
        }
        return result;
    }

    /**
     * advance 한 단계에서 자기 한도를 넘어서는 대기들이 낳는 CLOSE 사유의 가짓수의 최댓값. 확인 대기는 `UNRESPONSIVE`, AUTH 대기는
     * `UNAUTHENTICATED`다. 사유가 둘 이상이면 어느 CLOSE가 나갈지 정해지지 않는다. 같은 사유의 대기가 함께 넘는 것은 센 하나로 본다.
     */
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
            // 단계로 들어온 바이트의 ACK가 먼저이고, 이 단계에서 보낸 HELLO/PING/AUTH의 대기는 그 뒤에 시작한다.
            for (Ack ack : acknowledgedBy(step, stream)) {
                endOldestMatching(running, ack);
            }
            running.addAll(startedBy(step, server));
            if (InboundFrames.kindOf(step).equals("advance")) {
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
            if (InboundFrames.endsConnection(step)) {
                // 연결이 끝나면 구현은 타이머를 거두고, 새 연결의 바이트는 새 흐름이다. close 단계는 CLOSE를 보낼 뿐이라
                // 연결이 끝나지 않고 대기가 이어진다(규격 3.3절).
                running.clear();
                stream = new InboundFrames();
            }
        }
        return result;
    }

    /** 이 단계로 들어온 프레임들 중 대기를 끝낼 수 있는 ACK(와 AUTH 요청). */
    private static List<Ack> acknowledgedBy(JsonObject step, InboundFrames stream) {
        List<Ack> acks = new ArrayList<>();
        for (InboundFrames.Frame frame : InboundFrames.receivedBy(step, stream)) {
            int type = frame.type;
            if (type == Protocol.TYPE_AUTH) {
                // ACK가 없는 AUTH는 서버의 AUTH 대기를 끝낸다.
                acks.add(new Ack(AuthGate.isAuthAck(frame) ? Protocol.TYPE_AUTH : AUTH_REQUEST, null));
            } else if (frame.hasAckFlag() && (type == Protocol.TYPE_HELLO || type == Protocol.TYPE_PING)) {
                acks.add(new Ack(type, frame.pingSizedPayload()));
            }
        }
        return acks;
    }

    /** ACK 하나가 끝내는 대기는 가장 먼저 시작한 것 하나다. */
    private static void endOldestMatching(List<Wait> running, Ack ack) {
        for (Iterator<Wait> it = running.iterator(); it.hasNext(); ) {
            if (ack.endsWait(it.next())) {
                it.remove();
                return;
            }
        }
    }

    /**
     * 이 단계에서 보낸 프레임이 시작하는 대기. ACK 아닌 HELLO·PING·AUTH는 확인 대기를 시작하고, 서버 쪽의 ACK HELLO는 AUTH 대기를
     * 시작한다.
     */
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

    /** 보낸 PING의 payload. ping 단계는 단계의 payload이고, 아니면 프레임의 payload이며, 알 수 없으면 null이다. */
    private static String pingPayload(JsonObject step, JsonObject frame) {
        JsonElement payload = InboundFrames.kindOf(step).equals("ping") ? step.get("payload") : frame.get("payload");
        return payload != null
                        && payload.isJsonPrimitive()
                        && payload.getAsJsonPrimitive().isString()
                ? InboundFrames.normalizedHex(payload.getAsString())
                : null;
    }

    /** advance 단계 하나에서 대기 하나를 센 결과: 그 단계 번호, 대기가 시작된 뒤의 경과 시간, 그 단계의 시간, 그 대기의 한도, 한도를 넘으면 나갈 CLOSE 사유. */
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

    /** 대기 하나. PING이면 payload로 ACK와 맞춰 본다. */
    private static final class Wait {
        final int type;
        final String payload;
        long elapsed;

        Wait(int type, String payload) {
            this.type = type;
            this.payload = payload;
        }
    }

    /** 받은 ACK 프레임. `payload`는 헤더 20바이트와 확장 영역 뒤의 8바이트이고, 정확히 8바이트가 아니면 null이다. */
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
