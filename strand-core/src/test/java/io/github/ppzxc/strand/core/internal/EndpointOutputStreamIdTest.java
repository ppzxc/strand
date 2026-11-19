package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 전송 모듈이 와이어 배치를 알지 않고도, 쓰고 있는 출력이 어느 스트림의 애플리케이션 프레임인지 안다. */
class EndpointOutputStreamIdTest {

    @Test
    void anApplicationFrameOutputReportsItsStreamAndControlOutputsReportNone() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        EndpointOutput preface = client.nextOutput();
        assertThat(preface.applicationStreamId()).isZero();
        client.outputProgress(preface.id(), preface.length());
        EndpointOutput hello = client.nextOutput();
        assertThat(hello.applicationStreamId()).isZero();
    }

    @Test
    void aRequestFrameReportsTheStreamItOpens() {
        Endpoint server = openServer();
        server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();

        assertThat(request.applicationStreamId()).isEqualTo(2);
    }

    @Test
    void theResetOfThatStreamIsNotAnApplicationFrame() {
        Endpoint server = openServer();
        long ticket = server.request(257, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), 1);
        server.cancelRequest(ticket);
        server.outputProgress(request.id(), request.length() - 1);
        EndpointOutput reset = server.nextOutput();

        assertThat(reset).isNotNull();
        assertThat(reset.applicationStreamId()).isZero();
    }

    private Endpoint openServer() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        server.drainEvents();
        server.drainMessages();
        return server;
    }
}
