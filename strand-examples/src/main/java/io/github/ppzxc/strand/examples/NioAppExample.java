package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.nio.AuthenticationResult;
import io.github.ppzxc.strand.nio.Credentials;
import io.github.ppzxc.strand.nio.Listener;
import io.github.ppzxc.strand.nio.NioConnectAttempt;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandConnection;
import io.github.ppzxc.strand.nio.NioStrandServer;
import io.github.ppzxc.strand.nio.PreparedHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class NioAppExample {
    private NioAppExample() {}

    public static void main(String[] args) throws Exception {
        AppExampleSupport.Outcome outcome = run(NioSecurity.externallySecured(), NioSecurity.externallySecured());
        System.out.println(
                "echo=" + outcome.echoBody + " reverse=" + outcome.reverseBody + " close=" + outcome.closeKind);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static AppExampleSupport.Outcome run(NioSecurity serverSecurity, NioSecurity clientSecurity) throws Exception {
        AppExampleSupport.Outcome outcome = new AppExampleSupport.Outcome();

        ExecutorService appExecutor = AppExampleSupport.newAppExecutor("strand-nio-app-example");
        try {

            MinimumProgressSpace space = NioMinimumProgress.of(
                    clientSecurity,
                    AppExampleSupport.MAX_FRAME_SIZE,
                    AppExampleSupport.REQUESTS_PER_CONNECTION,
                    AppExampleSupport.REQUESTS_PER_CONNECTION);
            ResourceLimits limits = AppExampleSupport.limits(space);

            CompletableFuture<Void> stateReleased = new CompletableFuture<>();
            CompletableFuture<Void> reversed = new CompletableFuture<>();
            CompletableFuture<Void> holding = new CompletableFuture<>();
            NioStrandServer server = NioStrandServer.builder()
                    .executor(appExecutor)
                    .resourceLimits(limits)
                    .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                    .writeQueueBytes(AppExampleSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(AppExampleSupport.CLOSE_FINISH_DEADLINE)
                    .maxFrameSize(AppExampleSupport.MAX_FRAME_SIZE)
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

                                        request.ended().observe(end -> holding.complete(null));
                                        return;
                                    }
                                    request.respond(
                                                    Status.OK,
                                                    bytes("echo:"
                                                            + new String(request.payload(), StandardCharsets.UTF_8)))
                                            .observe(acceptance -> {
                                                if (acceptance.kind() == AcceptanceKind.RESOURCE_EXHAUSTED) {

                                                    System.err.println("응답 자원 부족: " + acceptance.resource());
                                                }
                                            });
                                    if (request.type() == AppExampleSupport.REVERSE_TYPE) {

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
                                () -> stateReleased.complete(null));
                    })
                    .onConnectionFailure(failure -> System.err.println("준비 실패: " + failure))
                    .build();
            Listener listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            InetSocketAddress address = new InetSocketAddress(
                    InetAddress.getLoopbackAddress(), ((InetSocketAddress) listener.localAddress()).getPort());

            NioStrandClient client = NioStrandClient.builder()
                    .executor(appExecutor)
                    .resourceLimits(limits)
                    .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                    .writeQueueBytes(AppExampleSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(AppExampleSupport.CLOSE_FINISH_DEADLINE)
                    .maxFrameSize(AppExampleSupport.MAX_FRAME_SIZE)
                    .security(clientSecurity)
                    .credentials(Credentials.of(AppExampleSupport.AUTH_MECHANISM, AppExampleSupport.AUTH_SECRET))
                    .handler(request -> request.respond(Status.OK, bytes("ack")))
                    .build();

            NioConnectAttempt attempt = client.connectAttempt(address);
            NioStrandConnection connection =
                    attempt.result().toCompletableFuture().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            attempt.transportReleased().toCompletableFuture().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);

            RequestResult echoed = AppExampleSupport.requestAfterRefusal(() -> connection
                    .request(
                            Request.of(AppExampleSupport.ECHO_TYPE, bytes("hello")),
                            RequestOptions.deadlineAfter(AppExampleSupport.PREPARATION_DEADLINE))
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS));
            outcome.echoStatus = echoed.status();
            outcome.echoBody = new String(echoed.payload(), StandardCharsets.UTF_8);

            connection
                    .request(Request.of(AppExampleSupport.REVERSE_TYPE, bytes("reverse")))
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            reversed.get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);

            RequestHandle held = connection.request(Request.of(AppExampleSupport.HOLD_TYPE, bytes("hold")));
            held.cancel();
            outcome.cancelledStatus = held.result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS)
                    .status();

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

            connection.close();
            outcome.closeKind = connection
                    .closed()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS)
                    .kind()
                    .name();

            listener.close();
            listener.closed().toCompletableFuture().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            client.shutdown().toCompletableFuture().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            server.shutdown().toCompletableFuture().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            client.resourcesTerminated().toCompletableFuture().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            server.resourcesTerminated().toCompletableFuture().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            outcome.serverShutdown = server.shutdownResult().resultNow().kind().name();
            outcome.clientShutdown = client.shutdownResult().resultNow().kind().name();
            stateReleased.get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            outcome.stateReleased = true;
            return outcome;
        } finally {
            appExecutor.shutdownNow();
        }
    }
}
