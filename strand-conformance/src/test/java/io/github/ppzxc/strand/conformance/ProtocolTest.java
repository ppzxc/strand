package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

/** 하네스의 프로토콜 상수 정의({@link Protocol}) 안의 불변식만 본다(internal seam). 값은 규격 쪽 리터럴로 확인한다. */
class ProtocolTest {

    @Test
    void helloParameterNumbersNamesAndDefaultsComeFromOneTable() {
        assertThat(Protocol.HELLO_PARAM_NAMES)
                .containsEntry(1, "max_frame_size")
                .containsEntry(2, "max_concurrent_streams")
                .containsEntry(3, "capabilities")
                .hasSize(3);
        assertThat(Protocol.HELLO_PARAM_NAMES.values()).containsExactlyElementsOf(Protocol.HELLO_DEFAULTS.keySet());
        assertThat(Protocol.HELLO_DEFAULTS.get("max_frame_size")).isEqualTo(65_536L);
        assertThat(VectorSchema.HELLO_PARAMS.names()).containsExactlyElementsOf(Protocol.HELLO_DEFAULTS.keySet());
    }

    @Test
    void configLimitReadsTheVectorsValueOrTheSpecifiedDefault() {
        JsonObject given =
                JsonParser.parseString("{\"config\":{\"ack_timeout_ms\":1000}}").getAsJsonObject();
        assertThat(Protocol.configLimit(given, Protocol.ACK_TIMEOUT_MS)).isEqualTo(1000L);
        assertThat(Protocol.configLimit(given, Protocol.AUTH_TIMEOUT_MS)).isEqualTo(10000L);
        assertThat(Protocol.configLimit(new JsonObject(), Protocol.ACK_TIMEOUT_MS))
                .isEqualTo(5000L);
        assertThat(VectorSchema.SETUP_CONFIG.names()).containsExactlyElementsOf(Protocol.CONFIG_DEFAULTS.keySet());
        assertThatThrownBy(() -> Protocol.configLimit(given, "ack_timeout"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ack_timeout");
    }

    @Test
    void statusAndErrorCodeNamesAreInCodeOrder() {
        assertThat(Protocol.STATUS_NAMES.indexOf("OK")).isZero();
        assertThat(Protocol.STATUS_NAMES.indexOf("UNAUTHENTICATED")).isEqualTo(16);
        assertThat(Protocol.ERROR_CODE_NAMES.indexOf("NO_ERROR")).isZero();
        assertThat(Protocol.ERROR_CODE_NAMES.indexOf("UNAUTHENTICATED")).isEqualTo(7);
    }
}
