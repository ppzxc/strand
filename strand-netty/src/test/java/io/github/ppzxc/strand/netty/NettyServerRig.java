package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * 연결 엔진 하나(서버)를 쓰기량을 제한하는 실제 채널에 꽂고, 맞은편 실제 소켓에서 core 클라이언트 {@link Endpoint}를 {@link PeerLink}로 굴린다. 서버 쪽 채널의 쓰기
 * 한도로 응답 쓰기 0·부분·전체를 결정적으로 만든다. 앱용 실행기를 직접 줄 수 있어 실행하지 않는 실행기 같은 앱 쪽 작업의 진행을 시험이 쥘 수 있다.
 */
final class NettyServerRig implements AutoCloseable {
    private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);

    final AppFixture fixture = AppFixture.plain();
    final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1_000));
    final NettyRig.LimitedChannel channel;
    final EventLoop loop = fixture.group.next();
    final PeerLink peer;
    final NettyStrandConnection connection;
    final OwnerBudgets owner;
    private final ServerSocketChannel listener;
    private final Socket socket;

    private java.util.concurrent.CountDownLatch gateRelease;

    NettyServerRig(RequestHandler handler) throws Exception {
        this(handler, null);
    }

    /**
     * @param wrapExecutor 앱용 실행기를 감싸는 함수(실행하지 않는 실행기처럼 앱 쪽 작업의 진행을 시험이 쥐어야 할 때). 없으면 fixture의 실행기를 그대로 쓴다
     */
    NettyServerRig(RequestHandler handler, java.util.function.UnaryOperator<Executor> wrapExecutor) throws Exception {
        EndpointConfig config = EndpointConfig.defaults()
                .knownTypes(Collections.singleton(AppFixture.TYPE))
                .maxConcurrentStreams(8)
                .ackTimeoutMs(HOUR_MS)
                .idleIntervalMs(HOUR_MS * 24);
        owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        listener = ServerSocketChannel.open();
        listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        int port = ((InetSocketAddress) listener.getLocalAddress()).getPort();
        socket = new Socket(InetAddress.getLoopbackAddress(), port);
        SocketChannel accepted = listener.accept();
        channel = new NettyRig.LimitedChannel(accepted);
        channel.config().setTcpNoDelay(true);
        Executor executor = wrapExecutor == null ? fixture.executor : wrapExecutor.apply(fixture.executor);
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.SERVER,
                config,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), socket.getLocalPort()),
                loop,
                new SerialExecutor(executor),
                executor,
                budgets,
                space,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                clock.get() + TimeUnit.HOURS.toNanos(1),
                TimeUnit.SECONDS.toNanos(5),
                64L << 20,
                clock::get,
                new ConnectionParams.PreparationObserver() {
                    @Override
                    public void ready(NettyStrandConnection value) {}

                    @Override
                    public void failed(PreparationFailure failure) {}
                },
                handler,
                null,
                credentials -> AuthenticationResult.accepted("peer"),
                null,
                ByteBufAllocator.DEFAULT);
        connection = new NettyStrandConnection(params, channel);
        connection.startServer();
        peer = new PeerLink(Endpoint.client(EndpointConfig.defaults().maxConcurrentStreams(8)), socket);
        peer.run(endpoint -> {});
        awaitTrue(() -> peer.events().stream().anyMatch(e -> e.kind() == Event.Kind.AUTHENTICATED));
    }

    /** 상대가 요청을 낸다. */
    long peerRequest(byte[] body) {
        long[] ticket = new long[1];
        peer.run(endpoint -> ticket[0] = endpoint.request(AppFixture.TYPE, body));
        return ticket[0];
    }

    void peerCancel(long ticket) {
        peer.run(endpoint -> endpoint.cancelRequest(ticket));
    }

    void writeCapacity(long bytes) {
        channel.capacity(bytes);
    }

    /** 서버 쪽 채널이 응답 바이트를 받아 주지 못한(일부라도 남긴) 횟수. */
    long stalls() {
        return channel.stallCount();
    }

    /** 이벤트 루프를 막아, 그동안의 앱 호출이 core 등록 전 대기에 머물게 한다. */
    void closeGate() throws InterruptedException {
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        gateRelease = release;
        loop.execute(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        if (!entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)) {
            throw new AssertionError("이벤트 루프가 문 앞에 서지 않았다");
        }
    }

    void openGate() {
        java.util.concurrent.CountDownLatch release = gateRelease;
        gateRelease = null;
        if (release != null) {
            release.countDown();
        }
    }

    long used(Resource resource) {
        return connection.resourceUsage(resource).current();
    }

    static void awaitTrue(BooleanSupplier condition) throws Exception {
        NettyRig.awaitTrue(condition);
    }

    @Override
    public void close() {
        openGate();
        connection.abort();
        peer.close();
        try {
            listener.close();
        } catch (IOException ignored) {
            // 정리 중의 오류는 할 수 있는 일이 없다.
        }
        fixture.close();
    }
}
