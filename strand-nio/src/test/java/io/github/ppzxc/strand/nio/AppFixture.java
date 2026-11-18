package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.TestTls;
import io.github.ppzxc.strand.nio.internal.TlsSizes;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** 앱 공개 API 테스트의 공통 도구: 앱용 실행기, 필수 설정을 모두 채운 builder, 실제 TCP/TLS 보안 설정. */
final class AppFixture implements AutoCloseable {
    static final int TYPE = 0x0100;
    static final long WAIT_SECONDS = 15;

    final ExecutorService executor;
    final NioSecurity serverSecurity;
    final NioSecurity clientSecurity;
    final boolean tls;
    final ResourceLimits limits;

    private AppFixture(boolean tls, ResourceLimits limits, NioSecurity serverSecurity, NioSecurity clientSecurity) {
        this.tls = tls;
        this.limits = limits;
        this.serverSecurity = serverSecurity;
        this.clientSecurity = clientSecurity;
        AtomicInteger count = new AtomicInteger();
        ThreadFactory factory = task -> {
            Thread thread = new Thread(task, "app-executor-" + count.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.executor = Executors.newCachedThreadPool(factory);
    }

    static AppFixture plain() {
        return plain(8, 8, 4);
    }

    static AppFixture plain(long perConnection, long owner, long connections) {
        return new AppFixture(
                false,
                limits(perConnection, owner, connections, null),
                NioSecurity.externallySecured(),
                NioSecurity.externallySecured());
    }

    static AppFixture tls() throws Exception {
        TlsSizes sizes = TlsSizes.of(TestTls.client());
        return new AppFixture(
                true, limits(8, 8, 4, sizes), NioSecurity.tls(TestTls.server()), NioSecurity.tls(TestTls.client()));
    }

    /**
     * 진행 공간은 도출한 최소치를 그대로 쓰고(연결 범위), 소유 객체 전체는 최대 연결 수만큼 곱한다. 나머지는 테스트가 넉넉하게 정한 한도다.
     */
    static ResourceLimits limits(long perConnection, long owner, long connections, TlsSizes sizes) {
        ProgressSpace space =
                ProgressSpace.derive(perConnection, perConnection, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, sizes);
        long[] min = space.minimums();
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(perConnection, 1 << 20)
                        .incomingRequests(perConnection, 1 << 20)
                        .responseBytes(1 << 20)
                        .handlerTasks(perConnection)
                        .observations(64)
                        .preparationBytes(1 << 16)
                        .progressBytes(min[0], min[1], min[2], min[3], min[4], min[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(connections)
                        .totalConnections(connections)
                        .preparationTasks(connections, connections, connections)
                        .outgoingRequests(owner * connections, 1 << 24)
                        .incomingRequests(owner * connections, 1 << 24)
                        .responseBytes(1 << 24)
                        .handlerTasks(owner * connections)
                        .observations(64 * connections)
                        .preparationBytes(1 << 20)
                        .progressBytes(
                                min[0] * connections,
                                min[1] * connections,
                                min[2] * connections,
                                min[3] * connections,
                                min[4] * connections,
                                min[5] * connections)
                        .build())
                .build();
    }

    NioStrandServer.Builder serverBuilder() {
        return NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(limits)
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .security(serverSecurity);
    }

    NioStrandClient.Builder clientBuilder() {
        return NioStrandClient.builder()
                .executor(executor)
                .resourceLimits(limits)
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .security(clientSecurity);
    }

    static InetSocketAddress loopback(Listener listener) {
        return new InetSocketAddress(
                InetAddress.getLoopbackAddress(), ((InetSocketAddress) listener.localAddress()).getPort());
    }

    static InetSocketAddress anyLoopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    static <T> T get(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    <T> T result(Result<T> result) throws Exception {
        return result.toCompletionStage(executor).toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * 팩터리가 있는 서버는 인증을 통과시킨 뒤 팩터리가 끝나기 전에 온 요청을 {@code REFUSED}로 거절한다(처리 안 됨, {@code UNAVAILABLE}). 클라이언트의
     * connect 성공과 서버의 핸들러 활성화 사이에는 이 짧은 틈이 있으므로, 처리 안 됨 결과는 다시 낸다(규격이 허용하는 재시도).
     */
    io.github.ppzxc.strand.api.RequestResult requestWhenServed(
            NioStrandConnection connection, io.github.ppzxc.strand.api.Request request) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            io.github.ppzxc.strand.api.RequestResult result =
                    result(connection.request(request).result());
            if (result.status() != io.github.ppzxc.strand.api.Status.UNAVAILABLE) {
                return result;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("서버가 핸들러를 활성화하지 않았다");
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
