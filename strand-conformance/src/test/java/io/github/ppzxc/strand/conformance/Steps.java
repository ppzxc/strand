package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Locale;

/** 벡터 JSON의 단계(step)를 읽는 도우미. */
final class Steps {

    private Steps() {}

    /** 이 단계 뒤에 연결이 끝나는가: 상대가 끊었거나(disconnect) 연결이 닫혔음이 애플리케이션에 전달됐다(closed 이벤트). */
    static boolean endsConnection(JsonObject step) {
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

    static String kindOf(JsonObject step) {
        return step.get("kind").getAsString();
    }

    /** 공백을 지우고 소문자로 맞춘 hex. 로더를 거치지 않은 JSON을 읽으므로 표기를 여기서 맞춘다. */
    static String normalizedHex(String hex) {
        return Bytes.compactHex(hex).toLowerCase(Locale.ROOT);
    }
}
