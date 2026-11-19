package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.Status;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 바인딩·인증·보안·기한 실패가 단계와 원인으로 관찰되고, 자격 증명 원문을 노출하지 않으며, 실패 뒤 자원이 돌아오는지 본다(#316). */
class NettyStrandPreparationFailureTest {

    private static final String SECRET = "TOP-SECRET-TOKEN";

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private AppFixture fixture(boolean tls) throws Exception {
        AppFixture fixture = tls ? AppFixture.tls() : AppFixture.plain();
        closeables.add(fixture);
        return fixture;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private Listener listen(NettyStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private static PreparationFailure failureOf(CompletableFuture<?> stage) throws Exception {
        try {
            stage.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof java.util.concurrent.CompletionException) {
                cause = cause.getCause();
            }
            assertThat(cause).isInstanceOf(PreparationFailedException.class);
            return ((PreparationFailedException) cause).failure();
        }
        throw new AssertionError("연결이 성공했다");
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

    private static void assertNoSecret(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            assertThat(String.valueOf(t.getMessage())).doesNotContain(SECRET);
            assertThat(t.toString()).doesNotContain(SECRET);
        }
    }

    @Test
    void listenFailureIsAnAsynchronousFailureOfTheStage() throws Exception {
        AppFixture fixture = fixture(false);
        NettyStrandServer first = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();
        Listener listener = listen(first);
        NettyStrandServer second = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();

        CompletableFuture<Listener> stage = AppFixture.cf(second.listen(listener.localAddress()));

        assertThatThrownBy(() -> stage.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(BindException.class);
    }

    @Test
    void listenSucceedsAtBindingWithoutAnyConnectionAndReportsTheBoundAddress() throws Exception {
        AppFixture fixture = fixture(false);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();

        Listener listener = listen(server);

        assertThat(((InetSocketAddress) listener.localAddress()).getPort()).isPositive();
        listener.close();
        listener.close();
    }

    @Test
    void rejectedCredentialsFailBothSidesAtTheAuthenticationStageWithoutLeakingTheSecret() throws Exception {
        AppFixture fixture = fixture(false);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.rejected())
                .handler(r -> {})
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder()
                .credentials(Credentials.of("BEARER", bytes(SECRET)))
                .build();

        PreparationFailure clientFailure = failureOf(AppFixture.cf(client.connect(AppFixture.loopback(listener))));
        PreparationFailure serverFailure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(clientFailure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(serverFailure).isNotNull();
        assertThat(serverFailure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(serverFailure.cause()).isInstanceOf(AuthenticationRejectedException.class);
        assertThat(serverFailure.remoteAddress()).isInstanceOf(InetSocketAddress.class);
        assertNoSecret(clientFailure.cause());
        assertNoSecret(serverFailure.cause());
        assertThat(clientFailure.toString()).doesNotContain(SECRET);
        assertThat(Credentials.of("BEARER", bytes(SECRET)).toString()).doesNotContain(SECRET);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        awaitTrue(() -> server.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        assertThat(client.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();
        assertThat(server.resourceUsage(Resource.AUTHENTICATION_TASKS).current())
                .isZero();
    }

    @Test
    void aVerifierThatThrowsIsAPreparationFailureWithThatCause() throws Exception {
        AppFixture fixture = fixture(false);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        IllegalStateException boom = new IllegalStateException("인증 저장소 오류");
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> {
                    throw boom;
                })
                .handler(r -> {})
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);

        PreparationFailure clientFailure =
                failureOf(AppFixture.cf(fixture.clientBuilder().build().connect(AppFixture.loopback(listener))));
        PreparationFailure serverFailure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(clientFailure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(serverFailure).isNotNull();
        assertThat(serverFailure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(serverFailure.cause()).isSameAs(boom);
    }

    @Test
    void anUnavailableAuthenticationServiceFailsThePreparation() throws Exception {
        AppFixture fixture = fixture(false);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.unavailable())
                .handler(r -> {})
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);

        PreparationFailure clientFailure =
                failureOf(AppFixture.cf(fixture.clientBuilder().build().connect(AppFixture.loopback(listener))));

        assertThat(clientFailure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(serverFailures.poll(15, TimeUnit.SECONDS)).isNotNull();
    }

    @Test
    void aFactoryFailureIsReportedAtTheFactoryStage() throws Exception {
        AppFixture fixture = fixture(false);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        IllegalStateException boom = new IllegalStateException("상태 생성 실패");
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handlerFactory(connection -> {
                    throw boom;
                })
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();

        // 클라이언트는 AUTH(ACK)로 준비를 마치므로 연결은 성공하고, 곧 서버가 CLOSE(INTERNAL_ERROR)로 닫는다.
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        PreparationFailure serverFailure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(serverFailure).isNotNull();
        assertThat(serverFailure.stage()).isEqualTo(PreparationStage.HANDLER_FACTORY);
        assertThat(serverFailure.cause()).isSameAs(boom);
        assertThat(fixture.result(connection.closed()).kind())
                .isEqualTo(io.github.ppzxc.strand.api.CloseNotice.Kind.ERROR_CLOSE);
        awaitTrue(() -> server.resourceUsage(Resource.FACTORY_TASKS).current() == 0);
    }

    @Test
    void aFactoryResultThatCouldNotBeActivatedIsDisposedAndNeverHandlesRequests() throws Exception {
        AppFixture fixture = fixture(false);
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        AtomicBoolean disposed = new AtomicBoolean();
        AtomicBoolean handled = new AtomicBoolean();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handlerFactory(connection -> {
                    factoryEntered.countDown();
                    releaseFactory.await();
                    return PreparedHandler.of(request -> handled.set(true), () -> disposed.set(true));
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        assertThat(factoryEntered.await(15, TimeUnit.SECONDS)).isTrue();

        // 팩터리가 끝나기 전에 연결이 끝난다. 늦게 돌아온 결과는 활성화되지 못했으므로 폐기된다.
        connection.abort();
        fixture.result(connection.closed());
        awaitTrue(() -> server.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        releaseFactory.countDown();

        awaitTrue(disposed::get);
        assertThat(handled).isFalse();
        awaitTrue(() -> server.resourceUsage(Resource.FACTORY_TASKS).current() == 0);
    }

    @Test
    void tlsHandshakeFailureForAnUntrustedServerIsReportedAtTheSecurityStage() throws Exception {
        AppFixture fixture = fixture(true);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder()
                .security(NettySecurity.tls(io.github.ppzxc.strand.netty.internal.TestTls.stranger()))
                .build();

        PreparationFailure clientFailure = failureOf(AppFixture.cf(client.connect(AppFixture.loopback(listener))));
        PreparationFailure serverFailure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(clientFailure.stage()).isEqualTo(PreparationStage.SECURITY);
        assertThat(clientFailure.cause()).isInstanceOf(SSLException.class);
        assertThat(serverFailure).isNotNull();
        assertThat(serverFailure.stage()).isEqualTo(PreparationStage.SECURITY);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void tlsHostNameVerificationFailureIsReportedAtTheSecurityStage() throws Exception {
        AppFixture fixture = fixture(true);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        InetSocketAddress wrongName = new InetSocketAddress(
                InetAddress.getByAddress("wrong.example.invalid", new byte[] {127, 0, 0, 1}),
                ((InetSocketAddress) listener.localAddress()).getPort());

        PreparationFailure failure =
                failureOf(AppFixture.cf(fixture.clientBuilder().build().connect(wrongName)));

        assertThat(failure.stage()).isEqualTo(PreparationStage.SECURITY);
        assertThat(failure.cause()).isInstanceOf(SSLException.class);
    }

    @Test
    void aPlaintextClientNeverTalksToATlsServerAndFailsWithoutFallingBackToPlaintext() throws Exception {
        AppFixture tlsFixture = fixture(true);
        AppFixture plainFixture = fixture(false);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        NettyStrandServer server = tlsFixture
                .serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);

        PreparationFailure clientFailure =
                failureOf(AppFixture.cf(plainFixture.clientBuilder().build().connect(AppFixture.loopback(listener))));
        PreparationFailure serverFailure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(clientFailure.stage()).isIn(PreparationStage.HELLO, PreparationStage.AUTHENTICATION);
        assertThat(serverFailure).isNotNull();
        assertThat(serverFailure.stage()).isEqualTo(PreparationStage.SECURITY);
    }

    @Test
    void aTlsClientNeverFallsBackToPlaintextAgainstAPlaintextServer() throws Exception {
        AppFixture tlsFixture = fixture(true);
        AppFixture plainFixture = fixture(false);
        NettyStrandServer server = plainFixture
                .serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);

        PreparationFailure clientFailure =
                failureOf(AppFixture.cf(tlsFixture.clientBuilder().build().connect(AppFixture.loopback(listener))));

        assertThat(clientFailure.stage()).isEqualTo(PreparationStage.SECURITY);
    }

    @Test
    void theClientPreparationDeadlineCountsFromTheConnectCallAndEndsAStuckAuthentication() throws Exception {
        AppFixture fixture = fixture(false);
        CountDownLatch never = new CountDownLatch(1);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> {
                    never.await();
                    return AuthenticationResult.accepted("late");
                })
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder()
                .preparationDeadline(Duration.ofMillis(300))
                .build();

        PreparationFailure clientFailure = failureOf(AppFixture.cf(client.connect(AppFixture.loopback(listener))));

        assertThat(clientFailure.cause()).isInstanceOf(PreparationDeadlineExceededException.class);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        never.countDown();
    }

    @Test
    void theServerPreparationDeadlineCountsFromTheTcpAcceptAndEndsAStuckAuthentication() throws Exception {
        AppFixture fixture = fixture(false);
        CountDownLatch never = new CountDownLatch(1);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        NettyStrandServer server = fixture.serverBuilder()
                .preparationDeadline(Duration.ofMillis(300))
                .authenticate(credentials -> {
                    never.await();
                    return AuthenticationResult.accepted("late");
                })
                .handler(r -> {})
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();

        CompletableFuture<NettyStrandConnection> attempt = AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        PreparationFailure serverFailure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(serverFailure).isNotNull();
        assertThat(serverFailure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(serverFailure.cause()).isInstanceOf(PreparationDeadlineExceededException.class);
        // 반환하지 않는 인증이 있어도 기한 실패는 관찰되고, 연결 슬롯은 돌아온다.
        awaitTrue(() -> server.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        never.countDown();
        awaitTrue(() -> server.resourceUsage(Resource.AUTHENTICATION_TASKS).current() == 0);
        assertThat(failureOf(attempt).cause()).isNotNull();
    }

    @Test
    void connectToAClosedPortFailsAtTheConnectionStageAndReturnsTheSlots() throws Exception {
        AppFixture fixture = fixture(false);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        InetSocketAddress closedPort = AppFixture.loopback(listener);
        listener.close();
        // 닫기는 비동기다. 리스닝 소켓이 실제로 풀린 뒤에 연결해야 거절(ConnectException)을 결정적으로 본다.
        AppFixture.get(listener.closed());
        NettyStrandClient client = fixture.clientBuilder().build();

        PreparationFailure failure = failureOf(AppFixture.cf(client.connect(closedPort)));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(ConnectException.class);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        assertThat(client.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();
        assertThat(client.resourceUsage(Resource.FRAME_ASSEMBLY_BYTES).current())
                .isZero();
    }

    @Test
    void anUnresolvableHostFailsAtTheConnectionStageAndReturnsTheResolveSlot() throws Exception {
        AppFixture fixture = fixture(false);
        NettyStrandClient client = fixture.clientBuilder().build();

        PreparationFailure failure =
                failureOf(AppFixture.cf(client.connect(InetSocketAddress.createUnresolved("no-such-host.invalid", 9))));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(UnknownHostException.class);
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).highWater()).isEqualTo(1);
    }

    @Test
    void whenConnectionSlotsRunOutTheServerClosesTheSocketAtOnceAndReportsTheLimit() throws Exception {
        AppFixture serverFixture = AppFixture.plain(8, 8, 1);
        closeables.add(serverFixture);
        AppFixture clientFixture = fixture(false);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        NettyStrandServer server = serverFixture
                .serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> request(r))
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        NettyStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        PreparationFailure clientFailure = failureOf(AppFixture.cf(client.connect(AppFixture.loopback(listener))));
        PreparationFailure serverFailure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(clientFailure.stage()).isIn(PreparationStage.HELLO, PreparationStage.CONNECTION);
        assertThat(serverFailure).isNotNull();
        assertThat(serverFailure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(serverFailure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) serverFailure.cause()).resource())
                .isIn(Resource.TOTAL_CONNECTIONS, Resource.PREPARING_CONNECTIONS);
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).rejections()
                        + server.resourceUsage(Resource.PREPARING_CONNECTIONS).rejections())
                .isGreaterThanOrEqualTo(1);
        // 슬롯이 돌아오면 다시 받는다.
        first.close();
        clientFixture.result(first.closed());
        awaitTrue(() -> server.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        NettyStrandConnection again = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        again.abort();
    }

    private static void request(IncomingRequest request) {
        request.respond(Status.OK, new byte[0]);
    }

    @Test
    void cancellingTheReturnedStageDoesNotCancelTheConnectionAttempt() throws Exception {
        AppFixture fixture = fixture(false);
        CountDownLatch established = new CountDownLatch(1);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handlerFactory(connection -> {
                    established.countDown();
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();

        CompletableFuture<NettyStrandConnection> stage = AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        stage.cancel(true);

        assertThat(established.await(15, TimeUnit.SECONDS)).isTrue();
        awaitTrue(() -> client.resourceUsage(Resource.PREPARING_CONNECTIONS).current() == 0);
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(1);
    }

    @Test
    void anAddressThatIsNotTcpIsRejectedAtOnce() throws Exception {
        AppFixture fixture = fixture(false);
        NettyStrandClient client = fixture.clientBuilder().build();

        assertThatThrownBy(() -> client.connect(new java.net.SocketAddress() {
                    private static final long serialVersionUID = 1L;
                }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aServerThatCannotBeReachedReportsAnUnusedRequestResultOnlyThroughRetainedResults() throws Exception {
        AppFixture fixture = fixture(false);
        RequestResult rejected;
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        connection.abort();
        fixture.result(connection.closed());

        rejected =
                fixture.result(connection.request(Request.of(TYPE, bytes("x"))).result());

        assertThat(rejected.status()).isEqualTo(Status.UNAVAILABLE);
    }

    @Test
    void aPreparationDeadlineThatHasAlreadyPassedFailsAtTheConnectionStage() throws Exception {
        AppFixture fixture = fixture(false);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandClient client =
                fixture.clientBuilder().preparationDeadline(Duration.ofNanos(1)).build();

        PreparationFailure failure = failureOf(AppFixture.cf(client.connect(AppFixture.loopback(listener))));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(PreparationDeadlineExceededException.class);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void aFactoryThatReturnsNothingIsAFactoryStageFailure() throws Exception {
        AppFixture fixture = fixture(false);
        BlockingQueue<PreparationFailure> serverFailures = new LinkedBlockingQueue<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handlerFactory(connection -> null)
                .onConnectionFailure(serverFailures::add)
                .build();
        Listener listener = listen(server);

        AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        PreparationFailure failure = serverFailures.poll(15, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.HANDLER_FACTORY);
        assertThat(failure.cause()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void thePeerEndedDuringPreparationIsKeptAsTheCloseNotice() throws Exception {
        AppFixture fixture = fixture(false);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.rejected())
                .handler(r -> {})
                .build();
        Listener listener = listen(server);

        PreparationFailure failure =
                failureOf(AppFixture.cf(fixture.clientBuilder().build().connect(AppFixture.loopback(listener))));

        assertThat(failure.cause()).isInstanceOf(ConnectionEndedException.class);
        io.github.ppzxc.strand.api.CloseNotice notice = ((ConnectionEndedException) failure.cause()).notice();
        assertThat(notice.kind()).isEqualTo(io.github.ppzxc.strand.api.CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(7);
        assertThat(notice.sender()).hasValue(io.github.ppzxc.strand.api.CloseNotice.Sender.PEER);
    }
}
