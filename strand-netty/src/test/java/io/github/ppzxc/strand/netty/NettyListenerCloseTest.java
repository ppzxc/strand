package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.StrandServerChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** listener.close()/closed()는 리스닝·경합 수락 소켓만 정리하고 이미 등록된 준비·활성 연결을 유지한다(#326). */
class NettyListenerCloseTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private AppFixture track(AppFixture fixture) {
        closeables.add(fixture);
        return fixture;
    }

    private NettyStrandServer.Builder acceptingServer(AppFixture fixture) {
        return fixture.serverBuilder().authenticate(c -> AuthenticationResult.accepted("p"));
    }

    private Listener listen(NettyStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void closedIsNotDoneBeforeCloseAndCompletesAfterTheListeningSocketIsReleased() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(fixture).handler(r -> {}).build());
        int port = ((InetSocketAddress) listener.localAddress()).getPort();

        assertThat(listener.closed().isDone()).isFalse();
        listener.close();
        AppFixture.get(listener.closed());

        // 리스닝 소켓이 실제로 풀렸으므로 같은 포트에 다시 바인딩할 수 있다.
        try (ServerSocketChannel again = ServerSocketChannel.open()) {
            again.setOption(java.net.StandardSocketOptions.SO_REUSEADDR, true);
            again.bind(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port));
            assertThat(again.isOpen()).isTrue();
        }
        // 닫힌 뒤에도 로컬 주소를 조회할 수 있다.
        assertThat(((InetSocketAddress) listener.localAddress()).getPort()).isEqualTo(port);
    }

    @Test
    void closeIsIdempotentAndClosedKeepsTheSameCompletion() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(fixture).handler(r -> {}).build());

        listener.close();
        listener.close();
        AppFixture.get(listener.closed());
        listener.close();

        assertThat(listener.closed()).isSameAs(listener.closed());
        assertThat(listener.closed().isSuccess()).isTrue();
    }

    @Test
    void closedCannotBeCancelledByTheApp() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(fixture).handler(r -> {}).build());

        assertThat(listener.closed().cancel(true)).isFalse();
        assertThat(listener.closed().isCancelled()).isFalse();
        listener.close();
        AppFixture.get(listener.closed());
    }

    @Test
    void closedCompletesEvenWhenTheAppExecutorRejectsEverything() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(fixture).handler(r -> {}).build());
        fixture.executor.rejecting = true;

        listener.close();

        AppFixture.get(listener.closed());
    }

    @Test
    void registeredActiveConnectionsSurviveListenerCloseAndClosedDoesNotWaitForThem() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(server)
                .handler(request -> request.respond(Status.OK, bytes("still")))
                .build());
        NettyStrandClient client = clientFixture.clientBuilder().build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        closeables.add(connection::abort);

        listener.close();
        AppFixture.get(listener.closed());

        assertThat(connection.closed().isDone()).isFalse();
        RequestResult result = clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("q")));
        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(new String(result.payload(), StandardCharsets.UTF_8)).isEqualTo("still");
    }

    @Test
    void aConnectionStillPreparingWhenTheListenerClosesIsKept() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch authStarted = new CountDownLatch(1);
        CompletableFuture<Void> finishAuth = new CompletableFuture<>();
        Listener listener = listen(server.serverBuilder()
                .authenticate(c -> {
                    authStarted.countDown();
                    finishAuth.get(30, TimeUnit.SECONDS);
                    return AuthenticationResult.accepted("p");
                })
                .handler(r -> {})
                .build());
        NettyStrandClient client = clientFixture.clientBuilder().build();
        CompletableFuture<NettyStrandConnection> connecting =
                AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        assertThat(authStarted.await(15, TimeUnit.SECONDS)).isTrue();

        listener.close();
        AppFixture.get(listener.closed());
        assertThat(connecting.isDone()).isFalse();
        finishAuth.complete(null);

        NettyStrandConnection connection = connecting.get(15, TimeUnit.SECONDS);
        closeables.add(connection::abort);
        assertThat(connection.closed().isDone()).isFalse();
    }

    @Test
    void aNewConnectionAfterCloseIsNotAccepted() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(server).handler(r -> {}).build());
        listener.close();
        AppFixture.get(listener.closed());
        NettyStrandClient client = clientFixture.clientBuilder().build();

        assertThatThrownBy(() -> AppFixture.get(client.connect(AppFixture.loopback(listener))))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PreparationFailedException.class);
    }

    @Test
    void anAcceptedChannelThatRacesTheCloseIsCleanedUpAndNeverRegistered() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandServer server = acceptingServer(fixture).handler(r -> {}).build();
        NettyStrandServer.Acceptor acceptor = server.new Acceptor();
        EmbeddedChannel listening = new EmbeddedChannel(acceptor);
        closeables.add(listening::finishAndReleaseAll);
        ChannelHandlerContext context = listening.pipeline().context(acceptor);
        listening.close();
        SocketChannel socket = SocketChannel.open();
        closeables.add(socket::close);

        // 닫힌 리스닝 채널에 이미 수락돼 있던 채널이 읽기로 도착한다. 연결로 등록하지 않고 소켓을 정리한다.
        acceptor.channelRead(context, new StrandChannel(listening, socket));

        assertThat(socket.isOpen()).isFalse();
        assertThat(server.resourceUsage(io.github.ppzxc.strand.api.Resource.TOTAL_CONNECTIONS)
                        .highWater())
                .isZero();
    }

    @Test
    void aListenerThatWasNeverRegisteredIsClosedAtOnceAndKeepsOneCompletion() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        StrandServerChannel channel = new StrandServerChannel();
        Listener listener = new Listener(channel, fixture.group.next());

        assertThat(listener.closed().isDone()).isFalse();
        listener.close();
        listener.close();

        assertThat(listener.closed().isSuccess()).isTrue();
        assertThat(channel.isOpen()).isFalse();
    }
}
