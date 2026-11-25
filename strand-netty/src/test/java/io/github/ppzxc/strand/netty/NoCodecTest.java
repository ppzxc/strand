package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import org.junit.jupiter.api.Test;

class NoCodecTest {

    @Test
    void theCodecIsNotOnThisClasspath() {
        assertThatThrownBy(() -> Class.forName("io.netty.handler.codec.http.HttpServerCodec"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void tcpWorksWithoutTheCodec() throws Exception {
        try (AppFixture fixture = AppFixture.plain()) {
            NettyStrandServer server = fixture.serverBuilder()
                    .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                    .handler(request -> request.respond(Status.OK, request.payload()))
                    .build();
            Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
            NettyStrandConnection connection =
                    AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
            try {
                RequestResult result = fixture.result(
                        connection.request(Request.of(TYPE, new byte[] {7})).result());
                long limit = System.nanoTime() + 15_000_000_000L;
                while (result.status() == Status.UNAVAILABLE && System.nanoTime() < limit) {
                    Thread.sleep(10);
                    result = fixture.result(
                            connection.request(Request.of(TYPE, new byte[] {7})).result());
                }
                assertThat(result.status()).isEqualTo(Status.OK);
                assertThat(result.payload()).containsExactly(7);
            } finally {
                connection.abort();
                listener.close();
            }
        }
    }

    @Test
    void choosingWebSocketFailsWithAClearError() throws Exception {
        try (AppFixture fixture = AppFixture.plain()) {
            NettyStrandServer server = fixture.serverBuilder()
                    .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                    .handler(request -> request.respond(Status.OK, request.payload()))
                    .build();
            NettyStrandClient client = fixture.clientBuilder().build();
            java.net.InetSocketAddress address = AppFixture.anyLoopback();

            assertThatThrownBy(() -> server.listen(address, Transport.webSocket()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("netty-codec-http");
            assertThatThrownBy(() -> client.connect(address, Transport.webSocket()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("netty-codec-http");
            assertThatThrownBy(() -> client.connectAttempt(address, Transport.webSocket("/strand")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("netty-codec-http");
        }
    }
}
