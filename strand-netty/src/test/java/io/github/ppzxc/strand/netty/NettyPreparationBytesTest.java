package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NettyPreparationBytesTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private AppFixture track(AppFixture fixture) {
        closeables.add(fixture);
        return fixture;
    }

    private Listener listen(NettyStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
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

    private static PreparationFailure failureOf(CompletableFuture<?> stage) throws Exception {
        try {
            stage.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(PreparationFailedException.class);
            return ((PreparationFailedException) e.getCause()).failure();
        }
        throw new AssertionError("연결이 성공했다");
    }

    @Test
    void theClientReservesPreparationBytesBeforeResolvingAndReturnsThemWhenTheConnectionEnds() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = server.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(request -> {})
                .build();
        Listener listener = listen(app);
        byte[] secret = "client-secret".getBytes(StandardCharsets.UTF_8);
        NettyStrandClient client = clientFixture
                .clientBuilder()
                .credentials(Credentials.of("TOKEN", secret))
                .build();

        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        assertThat(client.resourceUsage(Resource.PREPARATION_BYTES).highWater()).isGreaterThanOrEqualTo(secret.length);
        connection.abort();
        clientFixture.result(connection.closed());
        awaitTrue(() -> client.resourceUsage(Resource.PREPARATION_BYTES).current() == 0);
    }

    @Test
    void aClientWithoutEnoughPreparationBytesFailsLocallyAndReservesNothing() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().preparationBytes(4, 4).plain());
        NettyStrandClient client = fixture.clientBuilder()
                .credentials(Credentials.of("TOKEN", "far-too-long-for-four-bytes".getBytes(StandardCharsets.UTF_8)))
                .build();

        PreparationFailure failure =
                failureOf(AppFixture.cf(client.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9))));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARATION_BYTES);
        assertThat(client.resourceUsage(Resource.PREPARATION_BYTES).current()).isZero();
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();
    }

    @Test
    void resolvedAddressesAreReservedAsPreparationBytesBeforeTheyAreKept() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().preparationBytes(4, 4).plain());
        NettyStrandClient client = fixture.clientBuilder()
                .resolver(host -> new InetAddress[] {InetAddress.getLoopbackAddress()})
                .build();

        PreparationFailure failure =
                failureOf(AppFixture.cf(client.connect(InetSocketAddress.createUnresolved("h", 9))));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARATION_BYTES);
        awaitTrue(() -> client.resourceUsage(Resource.PREPARATION_BYTES).current() == 0);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void theResolvedAddressBytesAreReturnedOnceTheConnectionIsOpenWhileTheNameAndCredentialsStayReserved()
            throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = server.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(request -> {})
                .build();
        Listener listener = listen(app);
        int port = ((InetSocketAddress) listener.localAddress()).getPort();
        byte[] secret = "secret".getBytes(StandardCharsets.UTF_8);
        NettyStrandClient client = clientFixture
                .clientBuilder()
                .credentials(Credentials.of("TOKEN", secret))
                .resolver(host -> new InetAddress[] {InetAddress.getLoopbackAddress()})
                .build();

        NettyStrandConnection connection =
                AppFixture.get(client.connect(InetSocketAddress.createUnresolved("name", port)));

        awaitTrue(() -> client.resourceUsage(Resource.PREPARATION_BYTES).current() == 4 + 5 + secret.length);
        connection.abort();
        awaitTrue(() -> client.resourceUsage(Resource.PREPARATION_BYTES).current() == 0);
    }

    @Test
    void theServerReservesTheCredentialBytesBeforeCopyingThemAndReturnsThemWhenTheVerifierReturns() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1);
        byte[] secret = "server-side-secret".getBytes(StandardCharsets.UTF_8);
        NettyStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    entered.countDown();
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture
                .clientBuilder()
                .credentials(Credentials.of("TOKEN", secret))
                .build();
        CompletableFuture<NettyStrandConnection> attempt = AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        assertThat(entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(app.resourceUsage(Resource.PREPARATION_BYTES).current()).isEqualTo(secret.length + "TOKEN".length());
        hold.countDown();
        AppFixture.get(attempt).abort();
        awaitTrue(() -> app.resourceUsage(Resource.PREPARATION_BYTES).current() == 0);
    }

    @Test
    void theServerRefusesPreparationWhenTheCredentialBytesDoNotFit() throws Exception {
        AppFixture server = track(new AppFixture.Spec().preparationBytes(4, 4).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        AtomicInteger verified = new AtomicInteger();
        NettyStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    verified.incrementAndGet();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture
                .clientBuilder()
                .credentials(Credentials.of("TOKEN", "far-too-long-for-four-bytes".getBytes(StandardCharsets.UTF_8)))
                .build();
        CompletableFuture<NettyStrandConnection> attempt = AppFixture.cf(client.connect(AppFixture.loopback(listener)));

        PreparationFailure failure = failures.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARATION_BYTES);
        assertThat(verified).hasValue(0);
        assertThat(app.resourceUsage(Resource.AUTHENTICATION_TASKS).current()).isZero();
        assertThat(failureOf(attempt)).isNotNull();
    }
}
