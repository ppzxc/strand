package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.TestTls;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 필수 한도·준비 및 종료 시간·보안 설정은 build()에서 검증한다. 필수 누락, 범위 오류, 최소 진행 공간 부족을 거절한다(#316). */
class NettyStrandBuilderTest {

    private ExecutorService executor;
    private AppFixture fixture;

    @BeforeEach
    void setUp() {
        executor = Executors.newCachedThreadPool();
        fixture = AppFixture.plain();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        fixture.close();
    }

    private NettyStrandClient.Builder complete() {
        return NettyStrandClient.builder()
                .eventLoopGroup(fixture.group)
                .executor(executor)
                .resourceLimits(fixture.limits)
                .preparationDeadline(Duration.ofSeconds(1))
                .shutdownGrace(Duration.ofMillis(500))
                .shutdownDeadline(Duration.ofSeconds(2))
                .security(NettySecurity.externallySecured());
    }

    @Test
    void aClientWithEveryRequiredSettingBuildsAndWritesTheDocumentedDefaults() {
        NettyStrandClient client = complete().build();

        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).limit()).isEqualTo(4);
        assertThat(CommonSettings.DEFAULT_WRITE_QUEUE_BYTES).isEqualTo(64L * 1024 * 1024);
        assertThat(CommonSettings.DEFAULT_CLOSE_FINISH_DEADLINE).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void everyMissingRequiredSettingIsListedTogether() {
        assertThatThrownBy(() -> NettyStrandClient.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("eventLoopGroup")
                .hasMessageContaining("executor")
                .hasMessageContaining("resourceLimits")
                .hasMessageContaining("preparationDeadline")
                .hasMessageContaining("shutdownGrace")
                .hasMessageContaining("shutdownDeadline")
                .hasMessageContaining("security");
        assertThatThrownBy(() -> NettyStrandServer.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("authenticate")
                .hasMessageContaining("handler 또는 handlerFactory");
    }

    @Test
    void preparationDeadlineMustBePositiveAndFinite() {
        assertThatThrownBy(() -> complete().preparationDeadline(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("preparationDeadline");
        assertThatThrownBy(() ->
                        complete().preparationDeadline(Duration.ofMillis(-1)).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> complete()
                        .preparationDeadline(Duration.ofSeconds(Long.MAX_VALUE / 2))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("너무 크다");
    }

    @Test
    void shutdownGraceIsNonNegativeAndTheShutdownDeadlineIsPositiveAndLargerThanGrace() {
        complete().shutdownGrace(Duration.ZERO).build();
        assertThatThrownBy(() -> complete().shutdownGrace(Duration.ofMillis(-1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownGrace");
        assertThatThrownBy(() -> complete().shutdownDeadline(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shutdownDeadline");
        assertThatThrownBy(() -> complete()
                        .shutdownGrace(Duration.ofSeconds(2))
                        .shutdownDeadline(Duration.ofSeconds(2))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("보다 커야");
    }

    @Test
    void writeQueueBytesAndCloseFinishDeadlineAreValidated() {
        assertThatThrownBy(() -> complete().writeQueueBytes(1).build()).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> complete().closeFinishDeadline(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class);
        complete()
                .writeQueueBytes(EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT)
                .closeFinishDeadline(Duration.ofMillis(1))
                .build();
    }

    @Test
    void idlePingIsOnEvery30SecondsAndAckTimeoutIs5SecondsByDefault() {
        CommonSettings.Common common = complete().validate();

        assertThat(common.idleIntervalMs).isEqualTo(30_000L);
        assertThat(common.ackTimeoutMs).isEqualTo(5_000L);
    }

    @Test
    void subMillisecondDurationsAreRejectedInsteadOfTruncated() {
        assertThatThrownBy(() ->
                        complete().idlePingInterval(Duration.ofNanos(500_000)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idlePingInterval");
        assertThatThrownBy(() -> complete().ackTimeout(Duration.ofNanos(1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ackTimeout");
    }

    @Test
    void idlePingIntervalAndAckTimeoutAreCarriedToTheCoreConfig() {
        CommonSettings.Common common = complete()
                .idlePingInterval(Duration.ofSeconds(7))
                .ackTimeout(Duration.ofMillis(1500))
                .validate();

        assertThat(common.idleIntervalMs).isEqualTo(7_000L);
        assertThat(common.ackTimeoutMs).isEqualTo(1_500L);
    }

    @Test
    void anIdlePingIntervalOfZeroTurnsIdlePingOff() {
        CommonSettings.Common common =
                complete().idlePingInterval(Duration.ZERO).validate();

        assertThat(common.idleIntervalMs).isZero();
    }

    @Test
    void outOfRangeIdlePingIntervalAndAckTimeoutAreRejectedAtBuild() {
        assertThatThrownBy(() ->
                        complete().idlePingInterval(Duration.ofSeconds(-1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idlePingInterval");
        assertThatThrownBy(() -> complete()
                        .idlePingInterval(Duration.ofSeconds(Long.MAX_VALUE))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idlePingInterval");
        assertThatThrownBy(() -> complete().ackTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ackTimeout");
        assertThatThrownBy(() -> complete().ackTimeout(Duration.ofSeconds(-1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ackTimeout");
        assertThatThrownBy(() -> complete()
                        .ackTimeout(Duration.ofSeconds(Long.MAX_VALUE))
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ackTimeout");
    }

    @Test
    void maxFrameSizeIsCheckedByTheCore() {
        assertThatThrownBy(() -> complete().maxFrameSize(10).build()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aDirectExecutorIsNotSupported() {
        assertThatThrownBy(() -> complete().executor(Runnable::run).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("바로 실행");
    }

    @Test
    void anExecutorThatRejectsEvenTheProbeTaskIsRejected() {
        ExecutorService shut = Executors.newSingleThreadExecutor();
        shut.shutdown();

        assertThatThrownBy(() -> complete().executor(shut).build()).isInstanceOf(IllegalArgumentException.class);
    }

    private NettyStrandServer.Builder serverBase() {
        return NettyStrandServer.builder()
                .eventLoopGroup(fixture.group)
                .executor(executor)
                .resourceLimits(fixture.limits)
                .preparationDeadline(Duration.ofSeconds(1))
                .shutdownGrace(Duration.ZERO)
                .shutdownDeadline(Duration.ofSeconds(1))
                .security(NettySecurity.externallySecured())
                .authenticate(c -> AuthenticationResult.accepted("p"));
    }

    @Test
    void aServerNeedsExactlyOneOfAHandlerOrAFactory() {
        assertThatThrownBy(() -> serverBase()
                        .handler(r -> {})
                        .handlerFactory(c -> PreparedHandler.of(r -> {}))
                        .build())
                .isInstanceOf(IllegalArgumentException.class);
        serverBase().handlerFactory(c -> PreparedHandler.of(r -> {})).build();
        serverBase().handler(r -> {}).build();
    }

    @Test
    void minimumProgressSpaceIsDerivedAndRefusedPerScopeAtBuildTime() {
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        long[] min = space.minimums();

        assertThatThrownBy(() ->
                        complete().resourceLimits(limitsWith(min, 0, -1, 0)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RESULT_EVENT_BYTES");
        assertThatThrownBy(() ->
                        complete().resourceLimits(limitsWith(min, 1, 0, -1)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CONTROL_OUTPUT_BYTES");
    }

    /** {@code connectionResultDelta}는 연결 범위 결과 사건 공간, {@code ownerControlDelta}는 소유 객체 제어 출력 공간에 더하는 값이다. */
    private static ResourceLimits limitsWith(
            long[] min, long unused, long connectionResultDelta, long ownerControlDelta) {
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(8, 1 << 20)
                        .incomingRequests(8, 1 << 20)
                        .responseBytes(1 << 20)
                        .handlerTasks(8)
                        .observations(8)
                        .preparationBytes(1 << 16)
                        .progressBytes(min[0], min[1], min[2] + connectionResultDelta, min[3], min[4], min[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(4)
                        .totalConnections(4)
                        .preparationTasks(4, 4, 4)
                        .outgoingRequests(32, 1 << 24)
                        .incomingRequests(32, 1 << 24)
                        .responseBytes(1 << 24)
                        .handlerTasks(32)
                        .observations(32)
                        .preparationBytes(1 << 20)
                        .progressBytes(min[0], min[1] + ownerControlDelta, min[2], min[3], min[4], min[5])
                        .build())
                .build();
    }

    @Test
    void tlsRaisesTheMinimumToTheEngineBufferSizesSoPlainLimitsAreRefused() throws Exception {
        assertThatThrownBy(() ->
                        complete().security(NettySecurity.tls(TestTls.client())).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TLS_");
        complete()
                .security(NettySecurity.tls(TestTls.client()))
                .resourceLimits(AppFixture.limits(8, 8, 4, NettySecurity.tls(TestTls.client())))
                .build();
    }

    @Test
    void connectionCredentialsMustFollowTheMechanismGrammar() {
        assertThatThrownBy(() -> Credentials.of("lower", new byte[0])).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Credentials.of("", new byte[0])).isInstanceOf(IllegalArgumentException.class);
        Credentials credentials = Credentials.of("BEARER", new byte[] {1});
        byte[] copy = credentials.secret();
        copy[0] = 9;
        assertThat(credentials.secret()).containsExactly(1);
        assertThat(credentials.mechanism()).isEqualTo("BEARER");
    }

    private static final Resource[] PROGRESS = {
        Resource.FRAME_ASSEMBLY_BYTES,
        Resource.CONTROL_OUTPUT_BYTES,
        Resource.RESULT_EVENT_BYTES,
        Resource.TLS_PLAINTEXT_BYTES,
        Resource.TLS_CIPHERTEXT_BYTES,
        Resource.TLS_CONTROL_BYTES
    };

    private static ResourceLimits limitsFrom(MinimumProgressSpace space, Resource shortKind, boolean shortOwner) {
        long[] c = new long[6];
        long[] o = new long[6];
        for (int i = 0; i < 6; i++) {
            c[i] = o[i] = space.get(PROGRESS[i]);
            if (PROGRESS[i] == shortKind) {
                if (shortOwner) {
                    o[i]--;
                } else {
                    c[i]--;
                }
            }
        }
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(8, 1 << 20)
                        .incomingRequests(8, 1 << 20)
                        .responseBytes(1 << 20)
                        .handlerTasks(8)
                        .observations(8)
                        .preparationBytes(1 << 16)
                        .progressBytes(c[0], c[1], c[2], c[3], c[4], c[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(4)
                        .totalConnections(4)
                        .preparationTasks(4, 4, 4)
                        .outgoingRequests(32, 1 << 24)
                        .incomingRequests(32, 1 << 24)
                        .responseBytes(1 << 24)
                        .handlerTasks(32)
                        .observations(32)
                        .preparationBytes(1 << 20)
                        .progressBytes(o[0], o[1], o[2], o[3], o[4], o[5])
                        .build())
                .build();
    }

    @Test
    void queriedMinimumProgressSpaceBuildsForPlainAndTlsAndOneByteLessIsNamed() throws Exception {
        for (NettySecurity security :
                new NettySecurity[] {NettySecurity.externallySecured(), NettySecurity.tls(TestTls.client())}) {
            MinimumProgressSpace space = NettyMinimumProgress.of(security, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, 8, 8);
            complete()
                    .security(security)
                    .resourceLimits(limitsFrom(space, null, false))
                    .build();
            for (Resource kind : PROGRESS) {
                long required = space.get(kind);
                if (required == 0) {
                    continue;
                }
                for (boolean owner : new boolean[] {false, true}) {
                    assertThatThrownBy(() -> complete()
                                    .security(security)
                                    .resourceLimits(limitsFrom(space, kind, owner))
                                    .build())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining(kind.name())
                            .hasMessageContaining(owner ? "per-owner" : "per-connection")
                            .hasMessageContaining("required=" + required)
                            .hasMessageContaining("configured=" + (required - 1));
                }
            }
        }
    }

    @Test
    void tlsMinimumProgressSpaceIsPositiveAndPlainIsZero() throws Exception {
        MinimumProgressSpace plain =
                NettyMinimumProgress.of(NettySecurity.externallySecured(), EndpointConfig.DEFAULT_MAX_FRAME_SIZE, 8, 8);
        MinimumProgressSpace tls = NettyMinimumProgress.of(
                NettySecurity.tls(TestTls.client()), EndpointConfig.DEFAULT_MAX_FRAME_SIZE, 8, 8);
        assertThat(plain.get(Resource.TLS_CIPHERTEXT_BYTES)).isZero();
        assertThat(tls.get(Resource.TLS_CIPHERTEXT_BYTES)).isPositive();
        assertThat(tls.get(Resource.TLS_PLAINTEXT_BYTES)).isPositive();
        assertThat(tls.get(Resource.TLS_CONTROL_BYTES)).isPositive();
    }
}
