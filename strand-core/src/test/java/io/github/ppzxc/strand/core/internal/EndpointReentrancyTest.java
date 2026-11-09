package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** 입력 처리 중에 애플리케이션 콜백이 같은 엔드포인트에 다시 입력하면 조용히 오동작하지 않고 즉시 실패한다. */
class EndpointReentrancyTest {

    private static final int REQUEST_TYPE = 0x0100;

    /** 콜백 안에서 같은 엔드포인트에 입력을 넣어 보고, 던져진 예외를 기록한다. */
    private static final class ReenteringAuthenticator implements Authenticator {
        private final AtomicReference<Endpoint> endpoint = new AtomicReference<>();
        private final AtomicReference<Throwable> thrown = new AtomicReference<>();
        private final boolean webSocket;

        ReenteringAuthenticator(boolean webSocket) {
            this.webSocket = webSocket;
        }

        @Override
        public AuthDecision authenticate(String mechanism, byte[] credentials) {
            try {
                if (webSocket) {
                    endpoint.get().receiveMessage(WebSocketMessageKind.BINARY, Frames.preface());
                } else {
                    endpoint.get().receive(new byte[] {0});
                }
            } catch (RuntimeException e) {
                thrown.set(e);
            }
            return AuthDecision.PASS;
        }
    }

    private static void establish(Endpoint server, boolean webSocket) {
        byte[][] frames = {
            Frames.preface(),
            Frames.hello(EndpointConfig.defaults()),
            Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]),
            Frames.auth("ANONYMOUS", new byte[0])
        };
        for (byte[] frame : frames) {
            if (webSocket) {
                server.receiveMessage(WebSocketMessageKind.BINARY, frame);
            } else {
                server.receive(frame);
            }
        }
    }

    @Test
    void receiveInsideCallbackFailsFastAndEndpointKeepsWorking() {
        ReenteringAuthenticator authenticator = new ReenteringAuthenticator(false);
        Endpoint server = Endpoint.server(EndpointConfig.defaults().authenticator(authenticator));
        authenticator.endpoint.set(server);

        establish(server, false);

        assertThat(authenticator.thrown.get()).isInstanceOf(IllegalStateException.class);
        // 막힌 입력이 상태를 망가뜨리지 않았다. 이어서 정상 요청을 받는다.
        server.drainMessages();
        server.drainEvents();
        server.receive(Frames.request(REQUEST_TYPE, 1, new byte[3]));
        assertThat(server.drainEvents()).hasSize(1);
    }

    @Test
    void receiveMessageInsideCallbackFailsFast() {
        ReenteringAuthenticator authenticator = new ReenteringAuthenticator(true);
        Endpoint server = Endpoint.server(EndpointConfig.defaults().webSocket().authenticator(authenticator));
        authenticator.endpoint.set(server);

        establish(server, true);

        assertThat(authenticator.thrown.get()).isInstanceOf(IllegalStateException.class);
        server.drainMessages();
        server.drainEvents();
        server.receiveMessage(WebSocketMessageKind.BINARY, Frames.request(REQUEST_TYPE, 1, new byte[3]));
        assertThat(server.drainEvents()).hasSize(1);
    }

    @Test
    void receiveAfterAnExceptionEscapedStillWorks() {
        // 인증 콜백이 던져도 입력 중 표시가 남아 다음 입력을 막으면 안 된다.
        Endpoint server = Endpoint.server(EndpointConfig.defaults().authenticator((m, c) -> {
            throw new IllegalArgumentException("boom");
        }));
        establish(server, false);
        server.receive(new byte[] {0});
        // 연결은 이미 닫혔고 입력은 무시된다. IllegalStateException이 나오면 안 된다.
        server.receive(new byte[] {0});
        assertThat(server.drainEvents()).isNotEmpty();
    }
}
