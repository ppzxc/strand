package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Locale;

final class Steps {

    private Steps() {}

    static boolean endsConnection(JsonObject step) {
        if (StepKind.of(step).endsConnection()) {
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

    static String normalizedHex(String hex) {
        return Bytes.compactHex(hex).toLowerCase(Locale.ROOT);
    }
}
