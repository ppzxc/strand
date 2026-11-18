package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 전송 모듈이 수거 공간의 최소 크기와 읽기 멈춤을 셈할 때 기대는 core의 값(#316). */
class EndpointOutputBytesAndSpaceTest {

    @Test
    void neededSpaceMatchesTheConservativeBoundOfBoundedReceive() {
        // 열린 요청 + 미뤄 둔 요청 + 2, 열린 요청 + 미뤄 둔 요청 + PING + 받은 요청 + 제어 프레임 4개.
        assertThat(Endpoint.eventsNeeded(0)).isEqualTo(2);
        assertThat(Endpoint.eventsNeeded(7)).isEqualTo(9);
        assertThat(Endpoint.outputsNeeded(0, 0, 0)).isEqualTo(4);
        assertThat(Endpoint.outputsNeeded(7, 2, 3)).isEqualTo(16);
    }

    @Test
    void exactlyTheNeededSpaceSuffices() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        byte[] hello = Frames.hello(EndpointConfig.defaults());
        CollectionSpace space =
                CollectionSpace.of((int) Endpoint.eventsNeeded(0), (int) Endpoint.outputsNeeded(0, 0, 0) + 2);

        ReceiveProgress progress = client.receiveBounded(hello, 0, hello.length, 8, space);

        assertThat(progress.consumed()).isEqualTo(hello.length);
        assertThat(progress.collectionRequired()).isFalse();
    }

    @Test
    void pendingOutputBytesCountsQueuedAndTheUnwrittenRemainder() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        // 서문과 HELLO. 둘 다 아직 쓰지 않았다.
        long total = Frames.preface().length + Frames.hello(EndpointConfig.defaults()).length;
        assertThat(client.pendingOutputBytes()).isEqualTo(total);

        EndpointOutput first = client.nextOutput();
        client.outputProgress(first.id(), 3);
        assertThat(client.pendingOutputBytes()).isEqualTo(total - 3);

        client.outputProgress(first.id(), first.length() - 3);
        assertThat(client.pendingOutputBytes()).isEqualTo(total - first.length());
    }

    @Test
    void pendingOutputBytesIsZeroAfterDisconnectInProgressReportingMode() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        client.nextOutput();

        client.disconnect();

        assertThat(client.pendingOutputBytes()).isZero();
    }
}
