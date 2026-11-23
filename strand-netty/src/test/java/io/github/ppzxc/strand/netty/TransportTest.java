package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TransportTest {

    @Test
    void tcpHasNoPath() {
        assertThat(Transport.tcp().isWebSocket()).isFalse();
        assertThat(Transport.tcp().path()).isNull();
        assertThat(Transport.tcp()).isEqualTo(Transport.tcp());
    }

    @Test
    void webSocketWithoutAPathUsesSlash() {
        assertThat(Transport.webSocket().isWebSocket()).isTrue();
        assertThat(Transport.webSocket().path()).isEqualTo("/");
        assertThat(Transport.webSocket()).isEqualTo(Transport.webSocket("/"));
    }

    @Test
    void webSocketKeepsTheGivenPath() {
        Transport transport = Transport.webSocket("/strand/v1");
        assertThat(transport.path()).isEqualTo("/strand/v1");
        assertThat(transport).isNotEqualTo(Transport.tcp()).isNotEqualTo(Transport.webSocket());
        assertThat(transport.hashCode())
                .isEqualTo(Transport.webSocket("/strand/v1").hashCode());
        assertThat(transport.toString()).contains("/strand/v1");
        assertThat(Transport.tcp().toString()).isEqualTo("tcp");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "strand", "/a b", "/a?x=1", "/a#f", "/한글", "/a\n"})
    void rejectsPathsThatAreNotRequestPaths(String path) {
        assertThatThrownBy(() -> Transport.webSocket(path)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullPath() {
        assertThatThrownBy(() -> Transport.webSocket(null)).isInstanceOf(NullPointerException.class);
    }
}
