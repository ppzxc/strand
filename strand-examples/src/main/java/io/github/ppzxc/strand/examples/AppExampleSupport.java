package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.ResourceLimits;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 앱 공개 API 예제({@link NioAppExample}, {@link NettyAppExample})가 함께 쓰는 설정값과 결과 값. 아래 값은 모두 배포 환경에서 앱이 정해 넣는 명시적인 설정이며 제품 기본값이
 * 아니다. 예제는 로컬 한 대에서 돌도록 작게 잡았다.
 */
final class AppExampleSupport {
    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. */
    static final int ECHO_TYPE = 256;
    /** 서버가 응답하지 않고 붙들어 두는 요청의 type. 취소와 기한을 보이는 데 쓴다. */
    static final int HOLD_TYPE = 257;
    /** 서버가 받은 요청 안에서 같은 연결로 클라이언트에 역방향 요청을 내게 하는 type. */
    static final int REVERSE_TYPE = 258;

    static final String AUTH_MECHANISM = "BEARER";
    static final String PRINCIPAL = "demo-client";
    static final byte[] AUTH_SECRET = "demo-token".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    // 연결 수와 요청 수 한도. 연결별 한도는 소유 객체 전체 한도 안이어야 한다.
    static final long CONNECTIONS = 4;
    static final long REQUESTS_PER_CONNECTION = 8;
    // 요청·응답 본문 바이트 한도(연결별). 소유 객체 전체는 연결 수를 곱한다.
    static final long BODY_BYTES = 1 << 20;
    static final long PREPARATION_BYTES = 1 << 16;
    static final long OBSERVATIONS_PER_CONNECTION = 64;

    // 시간 설정: 준비 기한(양수·유한), 종료 유예(0 이상), 종료 기한(유예보다 큼), 쓰기 대기열과 CLOSE 마무리(기존 기본값).
    static final Duration PREPARATION_DEADLINE = Duration.ofSeconds(10);
    static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(1);
    static final Duration SHUTDOWN_DEADLINE = Duration.ofSeconds(5);
    static final long WRITE_QUEUE_BYTES = 64L << 20;
    static final Duration CLOSE_FINISH_DEADLINE = Duration.ofSeconds(5);

    /** 취소와 기한을 보이는 요청의 기한. 서버가 응답하지 않으므로 반드시 만료한다. */
    static final Duration REQUEST_DEADLINE = Duration.ofMillis(300);

    /** 예제가 기다리는 상한(초). 응답이 없어도 예제가 끝난다. */
    static final long WAIT_SECONDS = 15;

    private AppExampleSupport() {}

    /**
     * 필수 한도를 모두 채운 {@link ResourceLimits}. 진행 공간은 전송 모듈이 알려 주는 최소 필요량이다(최소 진행 공간보다 작으면 {@code build()}가 거절한다).
     *
     * @param progress 프레임 조립·제어 출력·결과 사건·TLS 평문·암호문·TLS 제어 순서의 연결별 최소 공간
     */
    static ResourceLimits limits(long[] progress) {
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .incomingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .responseBytes(BODY_BYTES)
                        .handlerTasks(REQUESTS_PER_CONNECTION)
                        .observations(OBSERVATIONS_PER_CONNECTION)
                        .preparationBytes(PREPARATION_BYTES)
                        .progressBytes(progress[0], progress[1], progress[2], progress[3], progress[4], progress[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(CONNECTIONS)
                        .totalConnections(CONNECTIONS)
                        .preparationTasks(CONNECTIONS, CONNECTIONS, CONNECTIONS)
                        .outgoingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, BODY_BYTES * CONNECTIONS)
                        .incomingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, BODY_BYTES * CONNECTIONS)
                        .responseBytes(BODY_BYTES * CONNECTIONS)
                        .handlerTasks(REQUESTS_PER_CONNECTION * CONNECTIONS)
                        .observations(OBSERVATIONS_PER_CONNECTION * CONNECTIONS)
                        .preparationBytes(PREPARATION_BYTES * CONNECTIONS)
                        .progressBytes(
                                progress[0] * CONNECTIONS,
                                progress[1] * CONNECTIONS,
                                progress[2] * CONNECTIONS,
                                progress[3] * CONNECTIONS,
                                progress[4] * CONNECTIONS,
                                progress[5] * CONNECTIONS)
                        .build())
                .build();
    }

    /** 앱이 소유하는 실행기. 인증·팩터리·핸들러·알림이 여기서 돌고 전송 스레드를 막지 않는다. 종료는 앱 책임이다. */
    static ExecutorService newAppExecutor(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** 예제가 끝난 뒤 돌려주는 관찰 결과. 시험이 단언한다. */
    static final class Outcome {
        volatile int echoStatus = -1;
        volatile String echoBody = "";
        volatile String reverseBody = "";
        volatile int reverseStatus = -1;
        volatile int cancelledStatus = -1;
        volatile int deadlineStatus = -1;
        volatile boolean conversionCancelKeptRequest;
        volatile String closeKind = "";
        volatile String serverShutdown = "";
        volatile String clientShutdown = "";
        volatile String authenticatedPrincipal = "";
        volatile boolean stateReleased;
    }
}
