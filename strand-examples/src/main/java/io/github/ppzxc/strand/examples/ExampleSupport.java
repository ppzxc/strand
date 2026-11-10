package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.ConnectionListener;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** 예제가 함께 쓰는 값과 출력 도우미. */
final class ExampleSupport {

    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. 예제는 256을 쓴다. */
    static final int DEMO_REQUEST_TYPE = 256;

    /** 규격 2.5.1절 status {@code OK}. */
    static final int STATUS_OK = 0;

    /** 클라이언트 예제가 기다리는 상한(초). 상대가 응답하지 않아도 예제가 끝난다. */
    static final long TIMEOUT_SECONDS = 5;

    /** 규격 3.3.1절: AUTH 메커니즘 이름. 예제는 {@code BEARER}를 쓴다. 코어는 메커니즘을 구현하지 않는다. */
    static final String AUTH_MECHANISM = "BEARER";

    /** 예제용 자격 증명. 실제로는 TLS 같은 보호된 채널 위에서 발급받은 토큰을 쓴다(규격 6.1절). */
    static final byte[] AUTH_CREDENTIALS = "demo-token".getBytes(StandardCharsets.UTF_8);

    private ExampleSupport() {}

    /** 클라이언트 설정에 AUTH 메커니즘과 자격 증명을 넣는다. 연결 수립 뒤 첫 프레임이 AUTH로 나간다. */
    static EndpointConfig withAuth(EndpointConfig config) {
        return config.auth(AUTH_MECHANISM, AUTH_CREDENTIALS);
    }

    /** 서버 설정에 AUTH 판단을 넣는다. 예제의 토큰만 통과시키고 나머지는 거부한다. */
    static EndpointConfig withAuthenticator(EndpointConfig config) {
        return config.authenticator((mechanism, credentials) ->
                AUTH_MECHANISM.equals(mechanism) && Arrays.equals(AUTH_CREDENTIALS, credentials)
                        ? AuthDecision.PASS
                        : AuthDecision.REJECT);
    }

    /** 핸들러와 알림을 돌리는 실행기. 전송 스레드를 막지 않으려고 연결 드라이버에 따로 준다. */
    static ExecutorService newHandlerExecutor(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** 연결 알림을 출력하고, 연결이 끝나면 {@code closed}를 완료한다. */
    static ConnectionListener printing(String who, CompletableFuture<ConnectionClosed> closed) {
        return new ConnectionListener() {
            @Override
            public void onAuthenticated(OptionalLong credentialLifetime) {
                System.out.println("[" + who + "] AUTH 통과");
            }

            @Override
            public void onClosed(ConnectionClosed event) {
                System.out.println("[" + who + "] 연결 종료 " + event.reason().kind());
                closed.complete(event);
            }
        };
    }

    /**
     * 결과 확정성(규격 4.4절)에 따라 다음 동작을 정한다. 재시도해도 안전한 것은 {@code NOT_PROCESSED}뿐이다.
     */
    static void handleResult(RequestResult result) {
        switch (result.certainty()) {
            case RESPONSE:
                System.out.println("  -> 응답을 받았다. status=" + result.status() + " body="
                        + new String(result.body(), StandardCharsets.UTF_8));
                break;
            case NOT_PROCESSED:
                // 상대가 처리를 시작하지 않았음이 확실하므로 다시 보내도 안전하다.
                System.out.println("  -> 처리되지 않았다. 다시 보내도 안전하다");
                break;
            case UNKNOWN:
            default:
                // 처리됐는지 알 수 없다. 다시 보내려면 요청에 멱등 키를 실어 상대가 중복을 걸러내게 해야 한다(규격 6.2절).
                System.out.println("  -> 처리 여부를 알 수 없다. 멱등 키 없이 다시 보내지 않는다");
                break;
        }
    }

    static String arg(String[] args, int index, String fallback) {
        return args.length > index ? args[index] : fallback;
    }
}
