package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.netty.AuthenticationResult;
import io.github.ppzxc.strand.netty.Credentials;
import io.github.ppzxc.strand.netty.Listener;
import io.github.ppzxc.strand.netty.NettyMinimumProgress;
import io.github.ppzxc.strand.netty.NettySecurity;
import io.github.ppzxc.strand.netty.NettyStrandClient;
import io.github.ppzxc.strand.netty.NettyStrandConnection;
import io.github.ppzxc.strand.netty.NettyStrandServer;
import io.github.ppzxc.strand.netty.Transport;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class NettyWebSocketExample {

    static final String PATH = "/strand";

    private NettyWebSocketExample() {}

    public static void main(String[] args) throws Exception {
        System.out.println("echo=" + run(NettySecurity.externallySecured(), NettySecurity.externallySecured()));
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static String run(NettySecurity serverSecurity, NettySecurity clientSecurity) throws Exception {
        ExecutorService appExecutor = AppExampleSupport.newAppExecutor("strand-netty-websocket-example");
        EventLoopGroup appEventLoops = new NioEventLoopGroup(2);
        try {
            MinimumProgressSpace space = NettyMinimumProgress.of(
                    clientSecurity,
                    AppExampleSupport.MAX_FRAME_SIZE,
                    AppExampleSupport.REQUESTS_PER_CONNECTION,
                    AppExampleSupport.REQUESTS_PER_CONNECTION);
            ResourceLimits limits = AppExampleSupport.limits(space);

            NettyStrandServer server = NettyStrandServer.builder()
                    .eventLoopGroup(appEventLoops)
                    .executor(appExecutor)
                    .resourceLimits(limits)
                    .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                    .maxFrameSize(AppExampleSupport.MAX_FRAME_SIZE)
                    .security(serverSecurity)
                    .authenticate(credentials -> AppExampleSupport.AUTH_MECHANISM.equals(credentials.mechanism())
                                    && Arrays.equals(AppExampleSupport.AUTH_SECRET, credentials.secret())
                            ? AuthenticationResult.accepted(AppExampleSupport.PRINCIPAL)
                            : AuthenticationResult.rejected())
                    .handler(request -> request.respond(
                            Status.OK, bytes("echo:" + new String(request.payload(), StandardCharsets.UTF_8))))
                    .build();

            Listener listener = server.listen(
                            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), Transport.webSocket(PATH))
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
                    .maxFrameSize(AppExampleSupport.MAX_FRAME_SIZE)
                    .security(clientSecurity)
                    .credentials(Credentials.of(AppExampleSupport.AUTH_MECHANISM, AppExampleSupport.AUTH_SECRET))
                    .build();

            NettyStrandConnection connection = client.connect(address, Transport.webSocket(PATH))
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            RequestResult echoed = AppExampleSupport.requestAfterRefusal(() -> connection
                    .request(Request.of(AppExampleSupport.ECHO_TYPE, bytes("hello")))
                    .result()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS));

            connection.close();
            connection
                    .closed()
                    .toCompletionStage(appExecutor)
                    .toCompletableFuture()
                    .get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            listener.close();
            client.shutdown().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            server.shutdown().get(AppExampleSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            return new String(echoed.payload(), StandardCharsets.UTF_8);
        } finally {
            appExecutor.shutdownNow();
            appEventLoops.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        }
    }
}
