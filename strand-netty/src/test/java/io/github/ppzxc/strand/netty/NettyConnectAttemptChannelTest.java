package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class NettyConnectAttemptChannelTest {

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

    private static void awaitTrue(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("조건이 시간 안에 참이 되지 않았다");
            }
            Thread.sleep(10);
        }
    }

    @ParameterizedTest
    @MethodSource("transports")
    void aCancelledAttemptReturnsItsBuffersAndLeavesTheAppEventLoopGroupAndExecutorRunning(String transport)
            throws Exception {
        AppFixture fixture = transport.equals("tls") ? AppFixture.tls() : AppFixture.plain();
        closeables.add(fixture);
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> {
                    entered.countDown();
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        NettyStrandClient client = fixture.clientBuilder().allocator(allocator).build();
        NettyConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        assertThat(entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(attempt.cancel()).isTrue();
        AppFixture.cf(attempt.result()).handle((connection, failure) -> failure).get(15, TimeUnit.SECONDS);
        AppFixture.get(attempt.transportReleased());

        assertThat(allocator.metric().usedDirectMemory()).isZero();
        assertThat(allocator.metric().usedHeapMemory()).isZero();
        assertThat(client.resourceUsage(Resource.TLS_CIPHERTEXT_BYTES).current())
                .isZero();
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        hold.countDown();
        awaitTrue(() -> server.resourceUsage(Resource.AUTHENTICATION_TASKS).current() == 0);
        assertThat(fixture.group.isShuttingDown()).isFalse();
        assertThat(fixture.pool.isShutdown()).isFalse();
        NettyStrandConnection again = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        assertThat(fixture.result(again.request(Request.of(AppFixture.TYPE, new byte[0]))
                                .result())
                        .status())
                .isEqualTo(Status.OK);
        again.abort();
    }
}
