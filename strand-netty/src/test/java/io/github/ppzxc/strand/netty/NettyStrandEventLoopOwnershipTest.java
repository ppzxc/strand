package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.Status;
import io.netty.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Netty 사용자의 EventLoop 환경: 앱이 소유한 EventLoopGroup·실행기를 종료하지 않고, listen·connect의 Future는 EventLoop에서 완료된다(#322). */
class NettyStrandEventLoopOwnershipTest {
    private AppFixture fixture;

    @AfterEach
    void cleanUp() {
        if (fixture != null) {
            fixture.close();
        }
    }

    private static CompletableFuture<String> completionThread(Future<?> future) {
        CompletableFuture<String> thread = new CompletableFuture<>();
        future.addListener(done -> thread.complete(Thread.currentThread().getName()));
        return thread;
    }

    @Test
    void listenAndConnectFuturesCompleteOnAnEventLoopOfTheAppGroup() throws Exception {
        fixture = AppFixture.plain();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        Future<Listener> listening = server.listen(AppFixture.anyLoopback());
        CompletableFuture<String> listenThread = completionThread(listening);
        Listener listener = AppFixture.get(listening);
        Future<NettyStrandConnection> connecting =
                fixture.clientBuilder().build().connect(AppFixture.loopback(listener));
        CompletableFuture<String> connectThread = completionThread(connecting);
        NettyStrandConnection connection = AppFixture.get(connecting);

        assertThat(listenThread.get(15, TimeUnit.SECONDS)).startsWith("nioEventLoopGroup");
        assertThat(connectThread.get(15, TimeUnit.SECONDS)).startsWith("nioEventLoopGroup");
        connection.abort();
        listener.close();
    }

    @Test
    void closingEverythingLeavesTheAppEventLoopGroupAndExecutorRunning() throws Exception {
        fixture = AppFixture.plain();
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, request.payload()));
                })
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        fixture.requestWhenServed(connection, Request.of(TYPE, new byte[0]));

        connection.close();
        CloseNotice notice = fixture.result(connection.closed());
        fixture.result(AppFixture.get(accepted).closed());
        listener.close();

        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(fixture.group.isShuttingDown()).isFalse();
        assertThat(fixture.pool.isShutdown()).isFalse();
        // 앱의 EventLoop는 여전히 일을 받는다.
        assertThat(fixture.group.submit(() -> "alive").get(15, TimeUnit.SECONDS))
                .isEqualTo("alive");
    }

    @Test
    void anAbortedConnectionAlsoLeavesTheAppResourcesAlone() throws Exception {
        fixture = AppFixture.plain();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));

        connection.abort();

        assertThat(fixture.result(connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(fixture.group.isShuttingDown()).isFalse();
        assertThat(fixture.pool.isShutdown()).isFalse();
        listener.close();
    }
}
