package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
 * 메시지 하나를 따로 읽고 text 메시지는 읽지 않는다. 프레임 길이가 12 미만(규격 2.2.1절의 최솟값)이면 헤더를 읽을 수 없는 깨진 바이트이므로 그
 * 연결의 나머지는 읽지 않는다.
 */
final class AckWaits {

    private static final int HELLO = 0x0001;
    private static final int PING = 0x0002;
    private static final int AUTH = 0x0005;
    /** 받은 `AUTH`(ACK 아님)와 그것을 기다리는 서버의 AUTH 대기를 나타내는 값. 프레임 `type`(16비트)에 없는 값이다. */
    private static final int AUTH_REQUEST = 0x10005;

    private static final int ACK_FLAG = 0x01;
    /** 연결 서문의 식별자 6바이트. 뒤의 2바이트는 예약이라 값을 보지 않는다(규격 3.1절). */
    private static final String PREFACE_PREFIX_HEX = "c0de53545230";

    private static final int PREFACE_HEX_LENGTH = 16;
    private static final int LENGTH_HEX = 8;
    private static final int PING_PAYLOAD_HEX = 16;
    /** 프레임 `length`의 최솟값 12(규격 2.2.1절). 이보다 짧으면 헤더를 읽을 수 없는 깨진 바이트다. */
    private static final int MIN_FRAME_LENGTH = 12;
    /** 헤더 16바이트 가운데 `extensions_length`가 있는 hex 위치(바이트 12~13, 규격 2.1절). */
    private static final int EXTENSIONS_LENGTH_AT = 24;

    private static final int HEADER_HEX = 32;
    private static final long DEFAULT_ACK_TIMEOUT_MS = 5000;
    private static final long DEFAULT_AUTH_TIMEOUT_MS = 10000;

    private AckWaits() {}

    /** 구조를 해석해 대기 구간별로 센 경과 시간 중 그 대기의 한도(`ack_timeout_ms`는 5000, `auth_timeout_ms`는 10000이 기본)에 정확히 닿는 것이 있는가. */
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
                if (wait.type == AUTH || wait.type == AUTH_REQUEST) {
                    return true;
                }
            }
            return false;
        });
    }

    /** 벡터에 advance 단계가 있는가. */
    static boolean hasAdvance(JsonObject vector) {
        return anyStep(vector, step -> kindOf(step).equals("advance"));
    }

    private static boolean anyStep(JsonObject vector, Predicate<JsonObject> matches) {
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            if (matches.test(step.getAsJsonObject())) {
                return true;
            }
        }
        return false;
    }

    /** `role`이 `client`가 아니면(없으면 포함) 서버 쪽으로 본다. 서버만 AUTH를 기다린다. */
    private static boolean isServerSide(JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        return !(setup.has("role") && setup.get("role").getAsString().equals("client"));
    }

    /**
     * 각 단계가 끝난 시점까지 AUTH(ACK)를 받았는가. 클라이언트 역할의 AUTH 관문(규격 3.3절)이 열렸는지 보는 데 쓴다. 바이트를 읽는
     * 방법은 확인 대기와 같다.
     */
    static boolean[] authAckReceivedThrough(JsonObject vector) {
        int count = vector.getAsJsonArray("steps").size();
        boolean[] result = new boolean[count];
        Inbound stream = new Inbound();
        boolean received = false;
        for (int i = 0; i < count; i++) {
            JsonObject step = vector.getAsJsonArray("steps").get(i).getAsJsonObject();
            for (Ack ack : acknowledgedBy(step, stream)) {
                received |= ack.type == AUTH;
            }
            result[i] = received;
            if (endsConnection(step)) {
                stream = new Inbound();
                received = false;
            }
        }
        return result;
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
        java.util.Map<Integer, java.util.Set<String>> crossed = new java.util.HashMap<>();
        int max = 0;
        for (Sample sample : samples(vector)) {
            if (sample.elapsed - sample.ms < sample.limit && sample.elapsed > sample.limit) {
                java.util.Set<String> reasons = crossed.computeIfAbsent(sample.step, k -> new java.util.HashSet<>());
                reasons.add(sample.reason);
                max = Math.max(max, reasons.size());
            }
        }
        return max;
    }

    private static List<Sample> samples(JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        long ackLimit = limitOf(setup, "ack_timeout_ms", DEFAULT_ACK_TIMEOUT_MS);
        long authLimit = limitOf(setup, "auth_timeout_ms", DEFAULT_AUTH_TIMEOUT_MS);
        boolean server = isServerSide(vector);
        List<Wait> running = new ArrayList<>();
        List<Sample> result = new ArrayList<>();
        Inbound stream = new Inbound();
        int index = -1;
        for (JsonElement element : vector.getAsJsonArray("steps")) {
            index++;
            JsonObject step = element.getAsJsonObject();
            // 단계로 들어온 바이트의 ACK가 먼저이고, 이 단계에서 보낸 HELLO/PING/AUTH의 대기는 그 뒤에 시작한다.
            for (Ack ack : acknowledgedBy(step, stream)) {
                endOldestMatching(running, ack);
            }
            running.addAll(startedBy(step, server));
            if (kindOf(step).equals("advance")) {
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
            if (endsConnection(step)) {
                // 연결이 끝나면 구현은 타이머를 거두고, 새 연결의 바이트는 새 흐름이다. close 단계는 CLOSE를 보낼 뿐이라
                // 연결이 끝나지 않고 대기가 이어진다(규격 3.3절).
                running.clear();
                stream = new Inbound();
            }
        }
        return result;
    }

    private static long limitOf(JsonObject setup, String name, long fallback) {
        return setup.has("config") && setup.getAsJsonObject("config").has(name)
                ? setup.getAsJsonObject("config").get(name).getAsLong()
                : fallback;
    }

    /** 이 단계 뒤에 연결이 끝나는가: 상대가 끊었거나(disconnect) 연결이 닫혔음이 애플리케이션에 전달됐다(closed 이벤트). */
    private static boolean endsConnection(JsonObject step) {
        if (kindOf(step).equals("disconnect")) {
            return true;
        }
        for (JsonElement event : step.getAsJsonArray("events")) {
            JsonElement name = event.getAsJsonObject().get("event");
            if (name != null && name.isJsonPrimitive() && name.getAsString().equals("closed")) {
                return true;
            }
        }
        return false;
    }

    private static String kindOf(JsonObject step) {
        return step.get("kind").getAsString();
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
                waits.add(new Wait(HELLO, null));
            } else if (type.equals("PING")) {
                waits.add(new Wait(PING, pingPayload(step, f)));
            } else if (type.equals("AUTH")) {
                waits.add(new Wait(AUTH, null));
            }
        }
        return waits;
    }

    /** 보낸 PING의 payload. ping 단계는 단계의 payload이고, 아니면 프레임의 payload이며, 알 수 없으면 null이다. */
    private static String pingPayload(JsonObject step, JsonObject frame) {
        JsonElement payload = kindOf(step).equals("ping") ? step.get("payload") : frame.get("payload");
        return payload != null
                        && payload.isJsonPrimitive()
                        && payload.getAsJsonPrimitive().isString()
                ? normalizedHex(payload.getAsString())
                : null;
    }

    /** 이 단계로 들어온 바이트에서 새로 완결된 ACK 프레임들. */
    private static List<Ack> acknowledgedBy(JsonObject step, Inbound stream) {
        String kind = kindOf(step);
        if (kind.equals("recv")) {
            return stream.feed(hexOf(step));
        }
        if (kind.equals("recv_message") && step.get("opcode").getAsString().equals("binary")) {
            return new Inbound().feed(hexOf(step));
        }
        return new ArrayList<>();
    }

    private static String hexOf(JsonObject step) {
        return normalizedHex(step.get("hex").getAsString());
    }

    /** 공백을 지우고 소문자로 맞춘 hex. 로더를 거치지 않은 JSON을 읽으므로 표기를 여기서 맞춘다. */
    private static String normalizedHex(String hex) {
        return Bytes.compactHex(hex).toLowerCase(Locale.ROOT);
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

    /** 받은 ACK 프레임. `payload`는 헤더 16바이트와 확장 영역 뒤의 8바이트이고, 정확히 8바이트가 아니면 null이다. */
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
            return type != PING || wait.payload == null || (payload != null && payload.equals(wait.payload));
        }
    }

    /** 한 방향 바이트 흐름. 모은 hex에서 완결된 프레임만 읽고, 깨진 바이트를 만나면 더 읽지 않는다. */
    private static final class Inbound {
        private final StringBuilder buffer = new StringBuilder();
        private boolean prefaceChecked;
        private boolean broken;

        List<Ack> feed(String hex) {
            List<Ack> acks = new ArrayList<>();
            if (broken) {
                return acks;
            }
            buffer.append(hex);
            if (!prefaceChecked && !skipPreface()) {
                return acks;
            }
            while (!broken && buffer.length() >= LENGTH_HEX) {
                long length = Long.parseLong(buffer.substring(0, LENGTH_HEX), 16);
                if (length < MIN_FRAME_LENGTH) {
                    broken = true;
                    break;
                }
                // length는 최대 2^32 - 1이라 long에서 넘치지 않는다. 프레임이 다 오지 않았으면 다음 바이트를 기다린다.
                long frameHex = (4 + length) * 2;
                if (buffer.length() < frameHex) {
                    break;
                }
                String frame = buffer.substring(0, (int) frameHex);
                buffer.delete(0, (int) frameHex);
                collectAck(frame, acks);
            }
            return acks;
        }

        /** 흐름 맨 앞이 연결 서문이면 8바이트를 건너뛴다. 서문인지 알 만큼 바이트가 모였으면 true. */
        private boolean skipPreface() {
            int compared = Math.min(buffer.length(), PREFACE_PREFIX_HEX.length());
            if (!buffer.substring(0, compared).equals(PREFACE_PREFIX_HEX.substring(0, compared))) {
                prefaceChecked = true;
                return true;
            }
            if (buffer.length() < PREFACE_HEX_LENGTH) {
                return false;
            }
            buffer.delete(0, PREFACE_HEX_LENGTH);
            prefaceChecked = true;
            return true;
        }

        private static void collectAck(String frame, List<Ack> acks) {
            int type = Integer.parseInt(frame.substring(LENGTH_HEX, LENGTH_HEX + 4), 16);
            int flags = Integer.parseInt(frame.substring(LENGTH_HEX + 4, LENGTH_HEX + 6), 16);
            boolean ack = (flags & ACK_FLAG) != 0;
            if (type == AUTH) {
                // ACK가 없는 AUTH는 서버의 AUTH 대기를 끝낸다.
                acks.add(new Ack(ack ? AUTH : AUTH_REQUEST, null));
            } else if (ack && (type == HELLO || type == PING)) {
                acks.add(new Ack(type, payloadOf(frame)));
            }
        }

        /** 헤더 16바이트와 확장 영역 뒤의 payload. 정확히 8바이트가 아니면 null이다(PING 말고는 payload를 맞춰 보지 않는다). */
        private static String payloadOf(String frame) {
            int extensionsHex =
                    Integer.parseInt(frame.substring(EXTENSIONS_LENGTH_AT, EXTENSIONS_LENGTH_AT + 4), 16) * 2;
            int from = HEADER_HEX + extensionsHex;
            return frame.length() - from == PING_PAYLOAD_HEX ? frame.substring(from) : null;
        }
    }
}
