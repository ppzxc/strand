package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EndpointBoundedReceiveTest {
    @Test
    void borrowedPartialInputIsCopiedAndResumed() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        byte[] first = EndpointTest.bytes("c0de5354");
        assertThat(server.receiveBounded(first, 0, first.length, 1)).isEqualTo(4);
        java.util.Arrays.fill(first, (byte) 0);
        byte[] rest = EndpointTest.bytes("52300000" + "0000001000010000000000000000000000000000");
        assertThat(server.receiveBounded(rest, 0, rest.length, 1)).isEqualTo(4);
        assertThat(server.drainMessages()).hasSize(1);
        assertThat(server.receiveBounded(rest, 4, rest.length - 4, 1)).isEqualTo(20);
        assertThat(server.drainMessages()).hasSize(1);
    }

    @Test
    void resumesAtTheUnconsumedUnit() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        byte[] input = EndpointTest.bytes("c0de535452300000" + "0000001000010000000000000000000000000000");

        int first = server.receiveBounded(input, 0, input.length, 1);
        assertThat(first).isEqualTo(8);
        assertThat(server.drainMessages()).hasSize(1);
        int second = server.receiveBounded(input, first, input.length - first, 1);
        assertThat(second).isEqualTo(20);
        assertThat(server.drainMessages()).hasSize(1);
    }
}
