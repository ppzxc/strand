package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 넘기지 않은 내 요청(번호만 받음, 쓰는 중)의 스트림에 온 응답과 RESET은 조용히 버리고 요청은 그대로 둔다(규격 4.3절). 넘긴 요청은 지금
 * 처럼 처리한다. 서버 쪽 엔드포인트의 내 요청은 짝수 번호이고 첫 요청은 2번이다.
 */
class EndpointResponseBeforeHandoffTest {

    private static final int REQUEST_TYPE = 257;
    private static final long STREAM = 2;

    private static Endpoint openServer() {
        Endpoint server = Drive.server(EndpointConfig.defaults());
        Drive.receive(server, EndpointTest.bytes("c0de535452300000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010000000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001000010100000000000000000000000000"));
        Drive.receive(server, EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        Drive.drainEvents(server);
        Drive.drainMessages(server);
        return server;
    }

    private static byte[] response() {
        return Frames.response(REQUEST_TYPE, STREAM, 0);
    }

    private static byte[] reset(int errorCode) {
        return Frames.reset(STREAM, errorCode);
    }

    private static String sent(Endpoint endpoint) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (OutboundMessage message : Drive.drainMessages(endpoint)) {
            out.write(message.bytes(), 0, message.bytes().length);
        }
        return EndpointTest.hex(out.toByteArray());
    }

    private static String requestFrameHex() {
        return EndpointTest.hex(Frames.request(REQUEST_TYPE, STREAM, new byte[0]));
    }

    private static void assertResult(Endpoint endpoint, int status, Certainty certainty) {
        List<Event> events = Drive.drainEvents(endpoint);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).kind()).isEqualTo(Event.Kind.RESULT);
        assertThat(events.get(0).streamId()).isEqualTo(STREAM);
        assertThat(events.get(0).status()).isEqualTo(status);
        assertThat(events.get(0).certainty()).contains(certainty);
    }

    /** 번호만 받은 요청에 온 응답은 결과를 내지 않고, 요청 프레임은 그대로 나가며, 이후의 진짜 응답이 결과가 된다. */
    @Test
    void responseToNumberedOnlyRequestIsDiscardedAndRequestStillGoesOut() {
        Endpoint server = openServer();
        long ticket = server.request(REQUEST_TYPE, new byte[0]);

        Drive.receive(server, response());

        assertThat(Drive.drainEvents(server)).isEmpty();
        assertThat(server.requestState(ticket)).isEqualTo(OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN);
        assertThat(server.openStreamCount()).isEqualTo(1);
        assertThat(sent(server)).isEqualTo(requestFrameHex());

        Drive.receive(server, response());
        assertResult(server, 0, Certainty.RESPONSE);
    }

    /** 번호만 받은 요청에 온 RESET도 버리고 요청은 나간다. */
    @Test
    void resetToNumberedOnlyRequestIsDiscardedAndRequestStillGoesOut() {
        Endpoint server = openServer();
        server.request(REQUEST_TYPE, new byte[0]);

        Drive.receive(server, reset(ErrorCode.REFUSED));

        assertThat(Drive.drainEvents(server)).isEmpty();
        assertThat(sent(server)).isEqualTo(requestFrameHex());

        Drive.receive(server, reset(ErrorCode.CANCEL));
        assertResult(server, Status.CANCELLED, Certainty.UNKNOWN);
    }

    /** 쓰는 중인 요청(첫 바이트는 넘겼고 마지막 바이트는 아직)에 온 응답과 RESET도 버리고, 쓰기를 끝까지 마친다. */
    @Test
    void responseAndResetToRequestBeingWrittenAreDiscardedAndWritingContinues() {
        Endpoint server = openServer();
        long ticket = server.request(REQUEST_TYPE, new byte[0]);
        EndpointOutput request = server.nextOutput();
        server.outputProgress(request.id(), 1);
        assertThat(server.requestState(ticket)).isEqualTo(OwnRequestLedger.RequestState.WRITING_STARTED);

        Drive.receive(server, response());
        Drive.receive(server, reset(ErrorCode.CANCEL));

        assertThat(Drive.drainEvents(server)).isEmpty();
        assertThat(server.openStreamCount()).isEqualTo(1);
        EndpointOutput rest = server.nextOutput();
        assertThat(rest.id()).isEqualTo(request.id());
        server.outputProgress(rest.id(), rest.length() - 1);

        Drive.receive(server, response());
        assertResult(server, 0, Certainty.RESPONSE);
    }

    /** 넘긴 요청에 온 응답은 결과가 된다. */
    @Test
    void responseToHandedOffRequestIsResult() {
        Endpoint server = openServer();
        server.request(REQUEST_TYPE, new byte[0]);
        Drive.drainMessages(server);

        Drive.receive(server, response());

        assertResult(server, 0, Certainty.RESPONSE);
    }

    /** 넘긴 요청에 온 RESET은 결과 모름으로 끝낸다. */
    @Test
    void resetToHandedOffRequestIsUnknownResult() {
        Endpoint server = openServer();
        server.request(REQUEST_TYPE, new byte[0]);
        Drive.drainMessages(server);

        Drive.receive(server, reset(ErrorCode.CANCEL));

        assertResult(server, Status.CANCELLED, Certainty.UNKNOWN);
    }

    /** 연결 오류 검사가 먼저다. 번호만 받은 요청의 스트림이어도 payload 크기가 틀린 RESET은 연결 오류다(규격 5.1절 3번). */
    @Test
    void connectionErrorCheckStillComesFirst() {
        Endpoint server = openServer();
        server.request(REQUEST_TYPE, new byte[0]);
        byte[] bad = Arrays.copyOf(reset(ErrorCode.CANCEL), Frames.reset(STREAM, 0).length + 1);
        bad[3] = (byte) (bad[3] + 1);

        Drive.receive(server, bad);

        assertThat(Drive.drainEvents(server)).extracting(Event::kind).contains(Event.Kind.CLOSED);
    }

    /** 종료 중인 연결은 내 요청이 남아 있는 동안 닫지 않는다. 버린 응답은 요청 수를 줄이지 않고, 진짜 응답이 오면 닫는다. */
    @Test
    void drainingConnectionStaysOpenWhileUnhandedRequestRemains() {
        Endpoint server = openServer();
        server.request(REQUEST_TYPE, new byte[0]);
        server.close(ErrorCode.NO_ERROR);
        Drive.receive(server, response());

        assertThat(Drive.drainEvents(server)).isEmpty();
        assertThat(server.openStreamCount()).isEqualTo(1);

        Drive.drainMessages(server);
        Drive.receive(server, response());
        assertThat(Drive.drainEvents(server))
                .extracting(Event::kind)
                .containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
    }
}
