package io.github.ppzxc.strand.load;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** 서버와 부하 생성기가 함께 쓰는 고정 설정. 두 전송에 같은 값을 주어 서버 전송이 유일한 변수가 되게 한다. */
final class LoadSupport {
    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. */
    static final int ECHO_TYPE = 256;

    static final String AUTH_MECHANISM = "BEARER";
    static final String PRINCIPAL = "load-client";
    static final byte[] AUTH_SECRET = "load-token".getBytes(StandardCharsets.UTF_8);

    static final Duration PREPARATION_DEADLINE = Duration.ofSeconds(10);
    static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(1);
    static final Duration SHUTDOWN_DEADLINE = Duration.ofSeconds(5);
    static final long WRITE_QUEUE_BYTES = 64L << 20;
    static final Duration CLOSE_FINISH_DEADLINE = Duration.ofSeconds(5);
    static final long WAIT_SECONDS = 30;

    private LoadSupport() {}

    /** 협상할 최대 프레임 크기. 본문이 한 프레임에 들어가도록 기본 64KiB와 본문 두 배 중 큰 값을 쓴다. */
    static long maxFrameSize(int bodyBytes) {
        return Math.max(65_536L, 2L * bodyBytes);
    }

    static ResourceLimits limits(MinimumProgressSpace space, int connections, int inflight, int bodyBytes) {
        long[] progress = {
            space.get(Resource.FRAME_ASSEMBLY_BYTES),
            space.get(Resource.CONTROL_OUTPUT_BYTES),
            space.get(Resource.RESULT_EVENT_BYTES),
            space.get(Resource.TLS_PLAINTEXT_BYTES),
            space.get(Resource.TLS_CIPHERTEXT_BYTES),
            space.get(Resource.TLS_CONTROL_BYTES)
        };
        long requests = inflight;
        long bodyBudget = requests * (bodyBytes + 1024L) * 2;
        long preparationBytes = 1 << 16;
        long observations = Math.max(64, 2L * inflight);
        long owners = connections;
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(requests, bodyBudget)
                        .incomingRequests(requests, bodyBudget)
                        .responseBytes(bodyBudget)
                        .handlerTasks(requests)
                        .observations(observations)
                        .preparationBytes(preparationBytes)
                        .progressBytes(progress[0], progress[1], progress[2], progress[3], progress[4], progress[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(owners)
                        .totalConnections(owners)
                        .preparationTasks(owners, owners, owners)
                        .outgoingRequests(requests * owners, bodyBudget * owners)
                        .incomingRequests(requests * owners, bodyBudget * owners)
                        .responseBytes(bodyBudget * owners)
                        .handlerTasks(requests * owners)
                        .observations(observations * owners)
                        .preparationBytes(preparationBytes * owners)
                        .progressBytes(
                                progress[0] * owners,
                                progress[1] * owners,
                                progress[2] * owners,
                                progress[3] * owners,
                                progress[4] * owners,
                                progress[5] * owners)
                        .build())
                .build();
    }

    /** 앱이 소유하는 실행기. 핸들러와 알림이 여기서 돌고 전송 스레드를 막지 않는다. */
    static ExecutorService newExecutor(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }
}
