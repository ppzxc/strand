package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import java.time.Duration;
import java.util.concurrent.Callable;
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
    /** 서버가 게이트가 열릴 때까지 응답을 미루는 요청의 type. 결과가 확정되기 전에 변환 Future를 취소하는 순서를 만드는 데 쓴다. */
    static final int GATED_TYPE = 259;

    static final String AUTH_MECHANISM = "BEARER";
    static final String PRINCIPAL = "demo-client";
    static final byte[] AUTH_SECRET = "demo-token".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    /** 협상할 최대 프레임 크기(바이트). 규격 기본값과 같다. */
    static final long MAX_FRAME_SIZE = 65_536;

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
    static ResourceLimits limits(io.github.ppzxc.strand.api.MinimumProgressSpace space) {
        return limits(new long[] {
            space.get(io.github.ppzxc.strand.api.Resource.FRAME_ASSEMBLY_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.CONTROL_OUTPUT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.RESULT_EVENT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.TLS_PLAINTEXT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.TLS_CIPHERTEXT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.TLS_CONTROL_BYTES)
        });
    }

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

    /**
     * 연결 직후의 첫 요청을 보낸다. 서버의 연결 인증 응답은 핸들러 팩터리 실행 전에 나가므로, 서버 핸들러가 준비되기 전에 온 요청은 거절되어 {@code UNAVAILABLE}·처리
     * 안 됨으로 돌아올 수 있다(규격 4.5절). 거절은 다시 보내도 안전하므로 점점 길게 기다리며 유한하게 다시 보낸다. 거절이 아닌 답은 그대로 돌려준다.
     *
     * @param attempt 요청 한 번을 보내고 결과를 기다린다
     */
    static RequestResult requestAfterRefusal(Callable<RequestResult> attempt) throws Exception {
        long limit = System.nanoTime() + Duration.ofSeconds(WAIT_SECONDS).toNanos();
        long pauseMillis = 5;
        RequestResult result = attempt.call();
        while (result.status() == Status.UNAVAILABLE
                && result.certainty() == Certainty.NOT_PROCESSED
                && System.nanoTime() < limit) {
            Thread.sleep(pauseMillis);
            pauseMillis = Math.min(pauseMillis * 2, 100);
            result = attempt.call();
        }
        return result;
    }
}
