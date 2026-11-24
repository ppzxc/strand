package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.netty.NettySecurity;
import io.github.ppzxc.strand.netty.Transport;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.internal.TestTls;
import org.junit.jupiter.api.Test;

/**
 * 승인된 앱 공개 API 사용 예제(필수 한도·시간 설정 포함)를 실제 TCP와 TLS에서 NIO·Netty 각각 끝까지 실행한다(#328). 보안·연결 인증·팩터리 준비 뒤 양방향 요청, 취소, 요청 기한,
 * 연결 종료, 전체 종료를 거친 결과를 확인한다. 예제 코드가 이 시험이 도는 코드 그대로다.
 */
class AppExamplesTest {

    private static void assertFullFlow(AppExampleSupport.Outcome outcome) {
        assertThat(outcome.authenticatedPrincipal).isEqualTo(AppExampleSupport.PRINCIPAL);
        assertThat(outcome.echoStatus).isEqualTo(Status.OK);
        assertThat(outcome.echoBody).isEqualTo("echo:hello");
        // 서버가 같은 연결로 클라이언트에 낸 역방향 요청에 클라이언트 핸들러가 응답했다.
        assertThat(outcome.reverseStatus).isEqualTo(Status.OK);
        assertThat(outcome.reverseBody).isEqualTo("ack");
        assertThat(outcome.cancelledStatus).isEqualTo(Status.CANCELLED);
        assertThat(outcome.deadlineStatus).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(outcome.closeKind).isEqualTo("CLOSE");
        assertThat(outcome.serverShutdown).isEqualTo("COMPLETED");
        assertThat(outcome.clientShutdown).isEqualTo("COMPLETED");
        assertThat(outcome.stateReleased).isTrue();
    }

    @Test
    void nioExampleRunsOverPlainTcp() throws Exception {
        assertFullFlow(NioAppExample.run(NioSecurity.externallySecured(), NioSecurity.externallySecured()));
    }

    @Test
    void nioExampleRunsOverTls() throws Exception {
        assertFullFlow(NioAppExample.run(NioSecurity.tls(TestTls.server()), NioSecurity.tls(TestTls.client())));
    }

    @Test
    void nettyExampleRunsOverPlainTcp() throws Exception {
        AppExampleSupport.Outcome outcome =
                NettyAppExample.run(NettySecurity.externallySecured(), NettySecurity.externallySecured());
        assertFullFlow(outcome);
        assertThat(outcome.conversionCancelKeptRequest).isTrue();
    }

    @Test
    void nettyExampleRunsOverTls() throws Exception {
        AppExampleSupport.Outcome outcome =
                NettyAppExample.run(NettySecurity.tls(TestTls.server()), NettySecurity.tls(TestTls.client()));
        assertFullFlow(outcome);
        assertThat(outcome.conversionCancelKeptRequest).isTrue();
    }

    @Test
    void nettyExampleRunsTheFullFlowOverPlainWebSocket() throws Exception {
        AppExampleSupport.Outcome outcome = NettyAppExample.run(
                NettySecurity.externallySecured(), NettySecurity.externallySecured(), Transport.webSocket("/strand"));
        assertFullFlow(outcome);
        assertThat(outcome.conversionCancelKeptRequest).isTrue();
    }

    @Test
    void nettyExampleRunsTheFullFlowOverTlsWebSocket() throws Exception {
        AppExampleSupport.Outcome outcome = NettyAppExample.run(
                NettySecurity.tls(TestTls.server()), NettySecurity.tls(TestTls.client()), Transport.webSocket());
        assertFullFlow(outcome);
    }

    @Test
    void theNettyWebSocketExampleRunsOverPlainAndTlsWebSocket() throws Exception {
        assertThat(NettyWebSocketExample.run(NettySecurity.externallySecured(), NettySecurity.externallySecured()))
                .isEqualTo("echo:hello");
        assertThat(NettyWebSocketExample.run(NettySecurity.tls(TestTls.server()), NettySecurity.tls(TestTls.client())))
                .isEqualTo("echo:hello");
    }
}
