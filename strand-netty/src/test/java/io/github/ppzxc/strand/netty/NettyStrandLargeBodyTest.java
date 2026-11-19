package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** 동시에 큰 본문을 주고받아도 부분 쓰기·부분 소비로 모두 전달되고 예약이 정확히 돌아온다(TCP 쓰기량, TLS 평문 소비량 보고). */
class NettyStrandLargeBodyTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    static Stream<String> transports() {
        return Stream.of("tcp", "tls");
    }

    @ParameterizedTest
    @MethodSource("transports")
    void concurrentLargeBodiesAreEchoedIntactAndEveryReservationIsReturned(String transport) throws Exception {
        AppFixture.Spec spec = new AppFixture.Spec().maxFrameSize(1_048_576).bodyBytes(8L << 20);
        AppFixture fixture = transport.equals("tls") ? spec.tls() : spec.plain();
        closeables.add(fixture);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        closeables.add(connection::abort);
        byte[][] bodies = new byte[6][];
        RequestHandle[] handles = new RequestHandle[bodies.length];
        for (int i = 0; i < bodies.length; i++) {
            bodies[i] = new byte[900_000];
            new Random(i).nextBytes(bodies[i]);
        }

        fixture.requestWhenServed(connection, Request.of(TYPE, new byte[0]));
        for (int i = 0; i < bodies.length; i++) {
            handles[i] = connection.request(Request.of(TYPE, bodies[i]));
        }

        for (int i = 0; i < bodies.length; i++) {
            RequestResult result = fixture.result(handles[i].result());
            assertThat(result.status())
                    .as("status of %d (%s)", i, result.resource())
                    .isEqualTo(Status.OK);
            assertThat(java.util.Arrays.equals(result.payload(), bodies[i])).isTrue();
        }
        assertThat(connection.resourceUsage(Resource.OUTGOING_BYTES).current()).isZero();
        assertThat(connection.resourceUsage(Resource.OUTGOING_BYTES).highWater())
                .isGreaterThan(900_000);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isZero();
    }
}
