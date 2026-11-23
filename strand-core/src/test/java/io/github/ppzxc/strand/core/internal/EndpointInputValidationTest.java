package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/** 애플리케이션 입력 검증(닫기의 error_code, 응답의 status)과 받은 CLOSE의 예약 error_code 취급(규격 2.5.1절, 2.5.2절). */
class EndpointInputValidationTest {

    private static final int REQUEST_TYPE = 257;

    private static Endpoint openServer() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        server.drainEvents();
        server.drainMessages();
        return server;
    }

    @Test
    void closeAcceptsEveryErrorCodeThatCloseMayCarry() {
        int[] allowed = {
            ErrorCode.NO_ERROR,
            ErrorCode.PROTOCOL_ERROR,
            ErrorCode.INTERNAL_ERROR,
            ErrorCode.EXCESSIVE_USE,
            ErrorCode.UNRESPONSIVE,
            ErrorCode.UNAUTHENTICATED
        };
        for (int code : allowed) {
            Endpoint server = openServer();

            server.close(code);

            assertThat(server.drainMessages()).as("code %d", code).hasSize(1);
        }
    }

    @Test
    void closeRejectsErrorCodesThatCloseMayNotCarryAndChangesNothing() {
        int[] rejected = {ErrorCode.REFUSED, ErrorCode.CANCEL, 8, 255, -1, Integer.MAX_VALUE};
        for (int code : rejected) {
            Endpoint server = openServer();

            assertThatThrownBy(() -> server.close(code)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> server.close(code, "reason")).isInstanceOf(IllegalArgumentException.class);
            assertThat(server.drainMessages()).as("code %d", code).isEmpty();
            assertThat(server.drainEvents()).isEmpty();
        }
    }

    @Test
    void respondAcceptsStatusZeroThroughSixteenOnly() {
        for (int status : new int[] {0, 16}) {
            Endpoint server = openServer();
            server.receive(Frames.request(REQUEST_TYPE, 1, new byte[0]));
            server.drainEvents();

            assertThat(server.respond(1, status, new byte[0]))
                    .as("status %d", status)
                    .isTrue();
        }
        for (int status : new int[] {17, 255, 256, -1}) {
            Endpoint server = openServer();
            server.receive(Frames.request(REQUEST_TYPE, 1, new byte[0]));
            server.drainEvents();
            server.drainMessages();

            assertThatThrownBy(() -> server.respond(1, status, new byte[0]))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(server.drainMessages()).as("status %d", status).isEmpty();
            // 스트림은 그대로 대기 중이므로 유효한 status로 다시 응답할 수 있다.
            assertThat(server.respond(1, 0, new byte[0])).isTrue();
        }
    }

    @Test
    void peerCloseWithReservedErrorCodeIsReportedAsInternalError() {
        // 부호 있는 int -1은 와이어에서 0xffffffff다.
        for (int code : new int[] {8, 255, -1}) {
            Endpoint server = openServer();

            server.receive(Frames.close(code, 0));

            List<Event> events = server.drainEvents();
            CloseReason reason = events.get(events.size() - 1).closeReason().orElseThrow(AssertionError::new);
            assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
            assertThat(reason.errorCode()).as("code %d", code).isEqualTo(ErrorCode.INTERNAL_ERROR);
            assertThat(reason.sender()).contains(CloseReason.Sender.PEER);
        }
    }
}
