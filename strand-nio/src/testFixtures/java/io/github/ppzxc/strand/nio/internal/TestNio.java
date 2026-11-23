package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.AuthenticationResult;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandServer;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 제품 공개 API(NioStrandServer, NioStrandClient) 위에서 도는 시험이 함께 쓰는 설정 도구. 필수 한도와 시간을 넉넉하게 채운 평문 TCP builder를 만든다. 요청 핸들러는
 * 시험이 정한다.
 */
public final class TestNio {

    /** 연결별로 한꺼번에 낼 수 있는 요청 수 한도. */
    public static final long REQUESTS_PER_CONNECTION = 128;

    /** 소유 객체(서버 또는 클라이언트 하나)가 동시에 가질 수 있는 연결 수 한도. */
    public static final long CONNECTIONS = 64;

    private static final long BODY_BYTES = 1 << 20;
    private static final long OWNER_BODY_BYTES = 1 << 24;

    /**
     * 종료 마무리 기한. 양쪽이 동시에 CLOSE를 보내면 제품 엔진에서 한쪽이 상대의 끊김을 보지 못하고 마무리 기한까지 기다릴 수 있어, 기본 5초 대신 짧게 둔다.
     */
    public static final Duration CLOSE_FINISH_DEADLINE = Duration.ofSeconds(1);

    private TestNio() {}

    /**
     * 앱용 실행기. 핸들러와 알림이 여기서 돌아 전송 스레드를 막지 않는다. 종료는 부른 쪽 책임이다.
     *
     * @param name 스레드 이름
     * @return 데몬 스레드를 쓰는 캐시 스레드 풀
     */
    public static ExecutorService newExecutor(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * 평문 TCP용 넉넉한 한도. 진행 공간은 최소 필요량이다.
     *
     * @return 자원 한도
     */
    public static ResourceLimits limits() {
        return limits(NioMinimumProgress.of(
                NioSecurity.externallySecured(),
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                REQUESTS_PER_CONNECTION,
                REQUESTS_PER_CONNECTION));
    }

    /**
     * 넉넉한 한도. 진행 공간은 부른 쪽이 정한 최소 필요량이다.
     *
     * @param space 엔진이 계산한 최소 진행 공간
     * @return 자원 한도
     */
    public static ResourceLimits limits(MinimumProgressSpace space) {
        long[] min = {
            space.get(Resource.FRAME_ASSEMBLY_BYTES),
            space.get(Resource.CONTROL_OUTPUT_BYTES),
            space.get(Resource.RESULT_EVENT_BYTES),
            space.get(Resource.TLS_PLAINTEXT_BYTES),
            space.get(Resource.TLS_CIPHERTEXT_BYTES),
            space.get(Resource.TLS_CONTROL_BYTES)
        };
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .incomingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .responseBytes(BODY_BYTES)
                        .handlerTasks(REQUESTS_PER_CONNECTION)
                        .observations(64)
                        .preparationBytes(1 << 16)
                        .progressBytes(min[0], min[1], min[2], min[3], min[4], min[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(CONNECTIONS)
                        .totalConnections(CONNECTIONS)
                        .preparationTasks(CONNECTIONS, CONNECTIONS, CONNECTIONS)
                        .outgoingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, OWNER_BODY_BYTES)
                        .incomingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, OWNER_BODY_BYTES)
                        .responseBytes(OWNER_BODY_BYTES)
                        .handlerTasks(REQUESTS_PER_CONNECTION * CONNECTIONS)
                        .observations(64 * CONNECTIONS)
                        .preparationBytes(1 << 20)
                        .progressBytes(
                                min[0] * CONNECTIONS,
                                min[1] * CONNECTIONS,
                                min[2] * CONNECTIONS,
                                min[3] * CONNECTIONS,
                                min[4] * CONNECTIONS,
                                min[5] * CONNECTIONS)
                        .build())
                .build();
    }

    /**
     * 필수 설정을 채운 서버 builder. 모든 연결을 통과시킨다. 요청 핸들러는 부른 쪽이 정한다.
     *
     * @param executor 앱용 실행기
     * @return 서버 builder
     */
    public static NioStrandServer.Builder serverBuilder(ExecutorService executor) {
        return NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(limits())
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .closeFinishDeadline(CLOSE_FINISH_DEADLINE)
                .security(NioSecurity.externallySecured())
                .authenticate(credentials -> AuthenticationResult.accepted("peer"));
    }

    /**
     * 필수 설정을 채운 클라이언트 builder. 요청 핸들러는 부른 쪽이 정한다.
     *
     * @param executor 앱용 실행기
     * @return 클라이언트 builder
     */
    public static NioStrandClient.Builder clientBuilder(ExecutorService executor) {
        return NioStrandClient.builder()
                .executor(executor)
                .resourceLimits(limits())
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .closeFinishDeadline(CLOSE_FINISH_DEADLINE)
                .security(NioSecurity.externallySecured());
    }
}
