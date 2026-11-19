package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** 받은 요청의 ended()가 상대 RESET(오류 코드 포함)을 구별한다(규격 4.4절). 응답 없는 핸들러가 붙든 요청을 상대가 취소한다. */
class NettyStrandIncomingEndTest {
    private AppFixture fixture;

    @AfterEach
    void cleanUp() {
        if (fixture != null) {
            fixture.close();
        }
    }

    static Stream<String> transports() {
        return Stream.of("tcp", "tls");
    }

    @ParameterizedTest
    @MethodSource("transports")
    void aPeerCancelEndsTheReceivedRequestAsPeerResetAndReturnsItsReservations(String transport) throws Exception {
        fixture = transport.equals("tls") ? AppFixture.tls() : AppFixture.plain();
        CompletableFuture<IncomingRequest> received = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(received::complete)
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(AppFixture.TYPE, new byte[] {1}));
        IncomingRequest request = AppFixture.get(received);

        handle.cancel();
        RequestEnd end = fixture.result(request.ended());

        assertThat(end.isPeerReset()).isTrue();
        assertThat(end.errorCode()).isEqualTo(OptionalLong.of(4));
        assertThat(request.connection()
                        .resourceUsage(Resource.INCOMING_REQUESTS)
                        .current())
                .isZero();
        connection.abort();
        listener.close();
    }
}
