package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.AuthenticationResult;
import io.github.ppzxc.strand.netty.Credentials;
import io.github.ppzxc.strand.netty.Listener;
import io.github.ppzxc.strand.netty.NettyConnectAttempt;
import io.github.ppzxc.strand.netty.NettyMinimumProgress;
import io.github.ppzxc.strand.netty.NettyResults;
import io.github.ppzxc.strand.netty.NettySecurity;
import io.github.ppzxc.strand.netty.NettyStrandClient;
import io.github.ppzxc.strand.netty.NettyStrandConnection;
import io.github.ppzxc.strand.netty.NettyStrandServer;
import io.github.ppzxc.strand.netty.PreparedHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Netty 전송의 앱 공개 API 사용 예제. 한 프로세스에서 서버와 클라이언트를 만들어 보안·연결 인증·핸들러 팩터리 준비 뒤 양방향 요청, 취소, 요청 기한, 연결 종료, 전체 종료를 순서대로
 * 실행하고 관찰한 결과를 돌려준다. 필수 한도·시간 설정을 모두 표시한다({@link AppExampleSupport}). 사용법: {@code NettyAppExample} (평문 TCP. 보안은 {@link
 * NettySecurity#tls}로 바꾼다)
 */
public final class NettyAppExample {
    private NettyAppExample() {}

    public static void main(String[] args) throws Exception {
        AppExampleSupport.Outcome outcome = run(NettySecurity.externallySecured(), NettySecurity.externallySecured());
        System.out.println(
                "echo=" + outcome.echoBody + " reverse=" + outcome.reverseBody + " close=" + outcome.closeKind);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * @param serverSecurity 서버 쪽 채널 보안(TLS 또는 외부에서 보호된 채널)
     * @param clientSecurity 클라이언트 쪽 채널 보안
     */
    static AppExampleSupport.Outcome run(NettySecurity serverSecurity, NettySecurity clientSecurity) throws Exception {
        AppExampleSupport.Outcome outcome = new AppExampleSupport.Outcome();
        // 앱이 소유하는 실행기: 인증·팩터리·핸들러·알림이 여기서 돈다. 끝나면 앱이 종료한다.
        ExecutorService appExecutor = AppExampleSupport.newAppExecutor("strand-netty-app-example");
        // Netty에서는 앱이 EventLoopGroup도 소유한다. 라이브러리는 앱이 준 그룹과 실행기를 종료하지 않는다.
        EventLoopGroup appEventLoops = new NioEventLoopGroup(2);
        try {
            // 1) 필수 설정: 한도와 시간. 진행 공간은 전송 모듈이 알려 주는 최소 필요량이다.
            MinimumProgressSpace space = NettyMinimumProgress.of(
                    clientSecurity,
                    EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                    AppExampleSupport.REQUESTS_PER_CONNECTION,
                    AppExampleSupport.REQUESTS_PER_CONNECTION);
            ResourceLimits limits = AppExampleSupport.limits(space);

            // 2) 서버: 보안 → 연결 인증 → 팩터리 준비. 팩터리는 연결별 업무 상태를 만들고 종료 알림에서 정리한다.
            CompletableFuture<Void> stateReleased = new CompletableFuture<>();
            CompletableFuture<Void> reversed = new CompletableFuture<>();
            CompletableFuture<Void> holding = new CompletableFuture<>();
            NettyStrandServer server = NettyStrandServer.builder()
                    .eventLoopGroup(appEventLoops)
                    .executor(appExecutor)
                    .resourceLimits(limits)
                    .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                    .writeQueueBytes(AppExampleSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(AppExampleSupport.CLOSE_FINISH_DEADLINE)
                    .security(serverSecurity)
                    .authenticate(credentials -> AppExampleSupport.AUTH_MECHANISM.equals(credentials.mechanism())
                                    && Arrays.equals(AppExampleSupport.AUTH_SECRET, credentials.secret())
                            ? AuthenticationResult.accepted(AppExampleSupport.PRINCIPAL)
                            : AuthenticationResult.rejected())
                    .handlerFactory(connection -> {
                        outcome.authenticatedPrincipal =
                                String.valueOf(connection.authenticatedPeer().principal());
                        connection.closed().observe(notice -> stateReleased.complete(null));
                        return PreparedHandler.of(
                                request -> {
                                    if (request.type() == AppExampleSupport.HOLD_TYPE) {
                                        // 응답하지 않는다: 상대가 취소하거나 기한이 지나면 받은 요청의 종료를 알린다.
                                        request.ended().observe(end -> holding.complete(null));
                                        return;
                                    }
                                    request.respond(
                                                    Status.OK,
                                                    bytes("echo:"
                                                            + new String(request.payload(), StandardCharsets.UTF_8)))
                                            .observe(acceptance -> {
                                                if (acceptance.kind() == AcceptanceKind.RESOURCE_EXHAUSTED) {
                                                    // 앱이 정한 유한한 재시도 정책을 적용한다. 응답 권한은 소비되지 않았다.
                                                    System.err.println("응답 자원 부족: " + acceptance.resource());
                                                }
                                            });
                                    if (request.type() == AppExampleSupport.REVERSE_TYPE) {
                                        // 같은 연결에서 서버도 요청을 개시한다(양방향).
                                        request.connection()
                                                .request(Request.of(AppExampleSupport.ECHO_TYPE, bytes("from-server")))
                                                .result()
                                                .observe(result -> {
                                                    outcome.reverseStatus = result.status();
                                                    outcome.reverseBody =
                                                            new String(result.payload(), StandardCharsets.UTF_8);
                                                    reversed.complete(null);
                                                });
                                    }
                                },
                                // 활성화되지 못한 결과 전용 정리. 활성화 뒤 정리는 위 종료 알림이 한다.
                                () -> stateReleased.complete(null));
                    })
                    .onConnectionFailure(failure -> System.err.println("준비 실패: " + failure))
                    .build();
            Listener listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            InetSocketAddress address = new InetSocketAddress(
                    InetAddress.getLoopbackAddress(), ((InetSocketAddress) listener.localAddress()).getPort());

            // 3) 클라이언트: 같은 필수 설정에 자격 증명과 역방향 요청 핸들러를 더한다.
            NettyStrandClient client = NettyStrandClient.builder()
                    .eventLoopGroup(appEventLoops)
                    .executor(appExecutor)
                    .resourceLimits(limits)
                    .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                    .writeQueueBytes(AppExampleSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(AppExampleSupport.CLOSE_FINISH_DEADLINE)
                    .security(clientSecurity)
                    .credentials(Credentials.of(AppExampleSupport.AUTH_MECHANISM, AppExampleSupport.AUTH_SECRET))
                    .handler(request -> request.respond(Status.OK, bytes("ack")))
                    .build();

            // 4) 취소 가능한 연결 시도. 성공은 보안·HELLO·연결 인증·서버 팩터리 준비 뒤다.
            NettyConnectAttempt attempt = client.connectAttempt(address);
            NettyStrandConnection connection = attempt.result().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            attempt.transportReleased().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);

            // 5) 요청·응답. 호출부터 시작하는 기한을 줄 수 있다.
            RequestResult echoed = connection
                    .request(
                            Request.of(AppExampleSupport.ECHO_TYPE, bytes("hello")),
                            RequestOptions.deadlineAfter(AppExampleSupport.PREPARATION_DEADLINE))
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            outcome.echoStatus = echoed.status();
            outcome.echoBody = new String(echoed.payload(), StandardCharsets.UTF_8);

            // 6) 서버가 같은 연결로 클라이언트에 요청을 낸다. 응답은 서버 쪽 관찰이 기록한다.
            connection
                    .request(Request.of(AppExampleSupport.REVERSE_TYPE, bytes("reverse")))
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            reversed.get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);

            // 6-1) Netty 변환: 변환 Future를 취소해도 실제 요청은 취소되지 않고 결과는 그대로 확정된다.
            RequestHandle converted = connection.request(Request.of(AppExampleSupport.ECHO_TYPE, bytes("converted")));
            Future<RequestResult> conversion = NettyResults.toFuture(converted.result());
            conversion.cancel(false);
            RequestResult kept = converted
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            outcome.conversionCancelKeptRequest = conversion.isCancelled() && kept.status() == Status.OK;

            // 7) 요청 취소. 서버가 응답하지 않는 요청을 실제로 취소한다.
            RequestHandle held = connection.request(Request.of(AppExampleSupport.HOLD_TYPE, bytes("hold")));
            held.cancel();
            outcome.cancelledStatus = held.result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS)
                    .status();

            // 8) 요청 기한. 서버가 응답하지 않으므로 기한이 지나 결과가 확정된다.
            outcome.deadlineStatus = connection
                    .request(
                            Request.of(AppExampleSupport.HOLD_TYPE, bytes("hold")),
                            RequestOptions.deadlineAfter(AppExampleSupport.REQUEST_DEADLINE))
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS)
                    .status();
            holding.get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);

            // 9) 연결 종료: close()는 CLOSE 절차, 필요하면 abort()로 즉시 끊는다. closed()는 결과 보존과 실제 통로 종료 뒤 완료한다.
            connection.close();
            outcome.closeKind = connection
                    .closed()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS)
                    .kind()
                    .name();

            // 10) 리스닝 정리와 전체 종료. 외부 실행 자원(appExecutor)은 앱이 종료한다.
            listener.close();
            listener.closed().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            client.shutdown().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            server.shutdown().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            client.resourcesTerminated().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            server.resourcesTerminated().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            outcome.serverShutdown = server.shutdownResult().resultNow().kind().name();
            outcome.clientShutdown = client.shutdownResult().resultNow().kind().name();
            stateReleased.get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            outcome.stateReleased = true;
            return outcome;
        } finally {
            appExecutor.shutdownNow();
            appEventLoops.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        }
    }
}
