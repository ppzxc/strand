package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.TestTls;
import io.github.ppzxc.strand.nio.internal.TlsSizes;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 필수 한도·준비 및 종료 시간·보안 설정은 build()에서 검증한다. 필수 누락, 범위 오류, 최소 진행 공간 부족을 거절한다(#316). */
class NioStrandBuilderTest {

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

    private NioStrandClient.Builder complete() {
        return NioStrandClient.builder()
                .executor(executor)
                .resourceLimits(fixture.limits)
                .preparationDeadline(Duration.ofSeconds(1))
                .shutdownGrace(Duration.ofMillis(500))
                .shutdownDeadline(Duration.ofSeconds(2))
                .security(NioSecurity.externallySecured());
    }

    @Test
    void aClientWithEveryRequiredSettingBuildsAndWritesTheDocumentedDefaults() {
        NioStrandClient client = complete().build();

        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).limit()).isEqualTo(4);
        assertThat(CommonSettings.DEFAULT_WRITE_QUEUE_BYTES).isEqualTo(64L * 1024 * 1024);
        assertThat(CommonSettings.DEFAULT_CLOSE_FINISH_DEADLINE).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void everyMissingRequiredSettingIsListedTogether() {
        assertThatThrownBy(() -> NioStrandClient.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("executor")
                .hasMessageContaining("resourceLimits")
                .hasMessageContaining("preparationDeadline")
                .hasMessageContaining("shutdownGrace")
                .hasMessageContaining("shutdownDeadline")
                .hasMessageContaining("security");
        assertThatThrownBy(() -> NioStrandServer.builder().build())
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

    private NioStrandServer.Builder serverBase() {
        return NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(fixture.limits)
                .preparationDeadline(Duration.ofSeconds(1))
                .shutdownGrace(Duration.ZERO)
                .shutdownDeadline(Duration.ofSeconds(1))
                .security(NioSecurity.externallySecured())
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
                        complete().security(NioSecurity.tls(TestTls.client())).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TLS_");
        TlsSizes sizes = TlsSizes.of(TestTls.client());
        complete()
                .security(NioSecurity.tls(TestTls.client()))
                .resourceLimits(AppFixture.limits(8, 8, 4, sizes))
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
}
