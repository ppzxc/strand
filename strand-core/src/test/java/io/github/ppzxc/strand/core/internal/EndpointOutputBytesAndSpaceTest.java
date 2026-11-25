package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EndpointOutputBytesAndSpaceTest {

    @Test
    void neededSpaceMatchesTheConservativeBoundOfBoundedReceive() {

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
