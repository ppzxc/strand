package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class NettyStrandBufferOwnershipTest {

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
    void everyInternalBufferIsReturnedWhenTheChannelIsCleanedUp(String transport) throws Exception {
        AppFixture.Spec spec = new AppFixture.Spec().maxFrameSize(1_048_576).bodyBytes(8L << 20);
        AppFixture fixture = transport.equals("tls") ? spec.tls() : spec.plain();
        closeables.add(fixture);
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        java.util.concurrent.CompletableFuture<NettyStrandConnection> accepted =
                new java.util.concurrent.CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .allocator(allocator)
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, request.payload()));
                })
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        NettyStrandClient client = fixture.clientBuilder().allocator(allocator).build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        byte[][] bodies = new byte[4][];
        RequestHandle[] handles = new RequestHandle[bodies.length];
        fixture.requestWhenServed(connection, Request.of(TYPE, new byte[0]));
        for (int i = 0; i < bodies.length; i++) {
            bodies[i] = new byte[700_000];
            new Random(i).nextBytes(bodies[i]);
            handles[i] = connection.request(Request.of(TYPE, bodies[i]));
        }
        for (int i = 0; i < bodies.length; i++) {
            RequestResult result = fixture.result(handles[i].result());
            assertThat(result.status()).isEqualTo(Status.OK);
            assertThat(java.util.Arrays.equals(result.payload(), bodies[i])).isTrue();
        }
        NettyStrandConnection peer = AppFixture.get(accepted);

        connection.close();
        fixture.result(connection.closed());
        fixture.result(peer.closed());

        assertThat(allocator.metric().usedDirectMemory()).isZero();
        assertThat(allocator.metric().usedHeapMemory()).isZero();
    }
}
