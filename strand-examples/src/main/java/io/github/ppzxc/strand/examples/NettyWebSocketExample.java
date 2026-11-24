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

/**
 * Netty WebSocket 전송 사용 예제. 서버는 {@code Transport.webSocket("/strand")}로 listener를 열어 그 경로의 업그레이드만 받고, 클라이언트는 같은 전송 값으로 연결한다. 나머지
 * API(핸들러, 인증, 자원 한도, 종료)는 TCP와 같다. 보안을 {@link NettySecurity#tls}로 바꾸면 TLS 위의 WebSocket(wss)이다. 앱이 {@code netty-codec-http}를 Netty와 같은 버전으로
 * 제공해야 한다. 사용법: {@code NettyWebSocketExample}
 */
public final class NettyWebSocketExample {
    /** 서버가 업그레이드를 받는 경로와 클라이언트가 요청하는 경로. 리버스 프록시의 경로 규칙에 맞춰 정한다. */
    static final String PATH = "/strand";

    private NettyWebSocketExample() {}

    public static void main(String[] args) throws Exception {
        System.out.println("echo=" + run(NettySecurity.externallySecured(), NettySecurity.externallySecured()));
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 한 프로세스에서 WebSocket 서버와 클라이언트를 만들어 요청 하나를 주고받고 종료한다.
     *
     * @param serverSecurity 서버 쪽 채널 보안(TLS 또는 외부에서 보호된 채널)
     * @param clientSecurity 클라이언트 쪽 채널 보안
     * @return 서버가 되돌려준 본문
     */
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
            // 전송은 listener마다 고른다. TCP 포트를 따로 열려면 같은 서버에서 Transport.tcp()로 listen을 한 번 더 부른다.
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
            // 연결 주소에서 Host 헤더가 만들어지고, 하위 프로토콜 strand가 제시된다.
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
