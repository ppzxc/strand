package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 벡터에서 확인 대기(HELLO나 PING을 보내고 ACK를 기다리는 시간, 규격 3.3절)의 구간을 해석한다. 대기는 HELLO/PING 송신
 * 단계에서 시작해 같은 종류의 ACK를 받는 단계에서 끝난다. 끝나지 않으면 벡터 끝까지 이어진다. recv의 바이트에서 ACK를 알아보지
 * 못하면 대기를 끝내지 않은 것으로 본다(대기가 길어지는 쪽이라 한도 검사가 더 엄격해진다).
 */
final class AckWaits {

    private static final int HELLO = 0x0001;
    private static final int PING = 0x0002;
    private static final String PREFACE_HEX = "c0de535452300000";

    private AckWaits() {}

    /** 벡터가 확인 대기를 하나라도 시작하는가. */
    static boolean startsAnyWait(JsonObject vector) {
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            if (!startedBy(step.getAsJsonObject()).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 확인 대기가 시작된 시점부터 센 경과 시간을 advance 단계마다 모아 돌려준다. 대기 여러 개가 겹치면 각각 따로 센다. 대기가
     * 시작된 단계 자체가 advance이면 그 단계의 시간을 대기 안에 센다(송신이 advance 앞에서 일어났다고 보는, 한도에 더 가까운
     * 쪽의 해석).
     */
    static List<Long> elapsedAtEachAdvance(JsonObject vector) {
        Map<Integer, Long> running = new HashMap<>();
        List<Long> result = new ArrayList<>();
        JsonArray steps = vector.getAsJsonArray("steps");
        for (JsonElement element : steps) {
            JsonObject step = element.getAsJsonObject();
            for (int type : startedBy(step)) {
                running.put(type, 0L);
            }
            if (step.get("kind").getAsString().equals("advance")) {
                long ms = step.get("ms").getAsLong();
                for (Map.Entry<Integer, Long> wait : running.entrySet()) {
                    wait.setValue(wait.getValue() + ms);
                    result.add(wait.getValue());
                }
            }
            for (int type : acknowledgedBy(step)) {
                running.remove(type);
            }
        }
        return result;
    }

    /** 이 단계에서 보낸 ACK 아닌 HELLO/PING. */
    private static List<Integer> startedBy(JsonObject step) {
        List<Integer> types = new ArrayList<>();
        for (JsonElement frame : step.getAsJsonArray("send")) {
            JsonObject f = frame.getAsJsonObject();
            JsonElement type = f.get("type");
            if (!type.getAsJsonPrimitive().isString() || isAck(f)) {
                continue;
            }
            if (type.getAsString().equals("HELLO")) {
                types.add(HELLO);
            } else if (type.getAsString().equals("PING")) {
                types.add(PING);
            }
        }
        return types;
    }

    private static boolean isAck(JsonObject frame) {
        if (!frame.has("flags")) {
            return false;
        }
        for (JsonElement flag : frame.getAsJsonArray("flags")) {
            if (flag.getAsString().equals("ACK")) {
                return true;
            }
        }
        return false;
    }

    /** recv 단계의 바이트에 들어 있는 ACK 프레임의 type. */
    private static List<Integer> acknowledgedBy(JsonObject step) {
        List<Integer> types = new ArrayList<>();
        if (!step.get("kind").getAsString().equals("recv")) {
            return types;
        }
        String hex = step.get("hex").getAsString().replaceAll("\\s", "");
        int at = hex.startsWith(PREFACE_HEX) ? PREFACE_HEX.length() : 0;
        while (hex.length() >= at + 14) {
            long length = Long.parseLong(hex.substring(at, at + 8), 16);
            int type = Integer.parseInt(hex.substring(at + 8, at + 12), 16);
            int flags = Integer.parseInt(hex.substring(at + 12, at + 14), 16);
            if ((flags & 1) != 0 && (type == HELLO || type == PING)) {
                types.add(type);
            }
            at += (int) ((4 + length) * 2);
        }
        return types;
    }
}
