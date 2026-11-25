package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
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
import io.github.ppzxc.strand.netty.Transport;
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

    static AppExampleSupport.Outcome run(NettySecurity serverSecurity, NettySecurity clientSecurity) throws Exception {
        return run(serverSecurity, clientSecurity, Transport.tcp());
    }

    static AppExampleSupport.Outcome run(
            NettySecurity serverSecurity, NettySecurity clientSecurity, Transport transport) throws Exception {
        AppExampleSupport.Outcome outcome = new AppExampleSupport.Outcome();

        ExecutorService appExecutor = AppExampleSupport.newAppExecutor("strand-netty-app-example");

        EventLoopGroup appEventLoops = new NioEventLoopGroup(2);
        try {

            MinimumProgressSpace space = NettyMinimumProgress.of(
                    clientSecurity,
                    AppExampleSupport.MAX_FRAME_SIZE,
                    AppExampleSupport.REQUESTS_PER_CONNECTION,
                    AppExampleSupport.REQUESTS_PER_CONNECTION);
            ResourceLimits limits = AppExampleSupport.limits(space);

            CompletableFuture<Void> stateReleased = new CompletableFuture<>();
            CompletableFuture<Void> reversed = new CompletableFuture<>();
            CompletableFuture<Void> holding = new CompletableFuture<>();
            CompletableFuture<Void> gate = new CompletableFuture<>();
            NettyStrandServer server = NettyStrandServer.builder()
                    .eventLoopGroup(appEventLoops)
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
                                    if (request.type() == AppExampleSupport.GATED_TYPE) {

                                        gate.thenRunAsync(
                                                () -> request.respond(Status.OK, bytes("gated")), appExecutor);
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
            Listener listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), transport)
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            InetSocketAddress address = new InetSocketAddress(
                    InetAddress.getLoopbackAddress(), ((InetSocketAddress) listener.localAddress()).getPort());

            NettyStrandClient client = NettyStrandClient.builder()
                    .eventLoopGroup(appEventLoops)
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

            NettyConnectAttempt attempt = client.connectAttempt(address, transport);
            NettyStrandConnection connection = attempt.result().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            attempt.transportReleased().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);

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

            RequestHandle converted = connection.request(Request.of(AppExampleSupport.GATED_TYPE, bytes("converted")));
            Future<RequestResult> conversion = NettyResults.toFuture(converted.result());
            conversion.cancel(false);
            gate.complete(null);
            RequestResult kept = converted
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            outcome.conversionCancelKeptRequest = conversion.isCancelled() && kept.status() == Status.OK;

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
