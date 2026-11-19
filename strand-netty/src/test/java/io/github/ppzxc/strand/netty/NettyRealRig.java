package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.TestTls;
import io.github.ppzxc.strand.netty.internal.TlsSizes;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 실제 TCP(또는 TLS) 연결 위에서 요청 개시자의 쓰기 진행을 결정적으로 막는 시험대. 클라이언트와 서버 사이에 중계기를 두고, 중계기가 클라이언트의 쓰기를 읽지 않으면
 * 클라이언트 소켓 버퍼가 차서 실제 쓰기가 0이나 부분 진행에서 멈춘다. 클라이언트 채널의 {@code writeAttempt}가 받아 준 양(TLS에서는 암호문)을 기록한다. 서버는
 * 실제 {@link NettyStrandServer}이고 받은 요청을 {@link #received}에 쌓는다.
 */
final class NettyRealRig implements AutoCloseable {
    /** {@code writeAttempt}가 받아 준 바이트와 받아 주지 못한(일부라도 남긴) 횟수를 기록하는 채널. */
    static final class RecordingChannel extends StrandChannel {
        final AtomicLong consumed = new AtomicLong();
        final AtomicLong stalls = new AtomicLong();

        @Override
        public int writeAttempt(ByteBuffer bytes) throws IOException {
            int wanted = bytes.remaining();
            int count = super.writeAttempt(bytes);
            consumed.addAndGet(count);
            if (wanted > 0 && count < wanted) {
                stalls.incrementAndGet();
            }
            return count;
        }
    }

    /** 클라이언트가 보낸 방향만 멈출 수 있는 중계기. */
    static final class PausableRelay implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket();
        private final int targetPort;
        private final List<Socket> sockets = new ArrayList<>();
        private volatile boolean paused;
        private volatile boolean closed;

        PausableRelay(int targetPort) throws IOException {
            this.targetPort = targetPort;
            listener.setReceiveBufferSize(2048);
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            Thread accept = new Thread(this::acceptLoop, "relay-accept");
            accept.setDaemon(true);
            accept.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        void pause() {
            paused = true;
        }

        void resume() {
            paused = false;
        }

        private void acceptLoop() {
            try {
                while (!closed) {
                    Socket client = listener.accept();
                    Socket server = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                    client.setTcpNoDelay(true);
                    server.setTcpNoDelay(true);
                    synchronized (sockets) {
                        sockets.add(client);
                        sockets.add(server);
                    }
                    pump(client, server, true);
                    pump(server, client, false);
                }
            } catch (IOException e) {
                // 닫는 중이다.
            }
        }

        private void pump(Socket from, Socket to, boolean upstream) {
            Thread thread = new Thread(
                    () -> {
                        byte[] buffer = new byte[4096];
                        try {
                            InputStream in = from.getInputStream();
                            OutputStream out = to.getOutputStream();
                            for (; ; ) {
                                while (upstream && paused && !closed) {
                                    Thread.sleep(2);
                                }
                                int count = in.read(buffer);
                                if (count < 0) {
                                    break;
                                }
                                out.write(buffer, 0, count);
                            }
                        } catch (IOException | InterruptedException e) {
                            // 어느 쪽이 끊겼다.
                        } finally {
                            closeQuietly(from);
                            closeQuietly(to);
                        }
                    },
                    upstream ? "relay-up" : "relay-down");
            thread.setDaemon(true);
            thread.start();
        }

        /** 연결을 끊는다. 클라이언트는 끊김을 본다. */
        void killConnections() {
            synchronized (sockets) {
                sockets.forEach(PausableRelay::closeQuietly);
            }
        }

        private static void closeQuietly(Socket socket) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // 닫는 중의 오류는 할 수 있는 일이 없다.
            }
        }

        @Override
        public void close() {
            closed = true;
            killConnections();
            try {
                listener.close();
            } catch (IOException ignored) {
                // 닫는 중의 오류는 할 수 있는 일이 없다.
            }
        }
    }

    final AppFixture fixture;
    final BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
    final NettyStrandServer server;
    final Listener listener;
    final PausableRelay relay;
    final RecordingChannel recording = new RecordingChannel();
    final NettyStrandConnection connection;

    NettyRealRig(boolean tls) throws Exception {
        AppFixture.Spec spec = new AppFixture.Spec().maxFrameSize(1L << 20).bodyBytes(1 << 20);
        this.fixture = tls ? spec.tls() : spec.plain();
        this.server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(received::add)
                .build();
        this.listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        this.relay = new PausableRelay(((InetSocketAddress) listener.localAddress()).getPort());
        InetSocketAddress relayAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), relay.port());
        OwnerBudgets owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        TlsSizes sizes = tls ? TlsSizes.of(TestTls.client()) : null;
        ProgressSpace space = ProgressSpace.derive(8, 8, fixture.maxFrameSize, sizes);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        ConnectionParams.Tls security = tls
                ? new ConnectionParams.Tls(TestTls.client(), sizes, relayAddress.getHostString(), relay.port())
                : null;
        CompletableFuture<NettyStrandConnection> ready = new CompletableFuture<>();
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                EndpointConfig.defaults().maxFrameSize(fixture.maxFrameSize).maxConcurrentStreams(8),
                relayAddress,
                fixture.group.next(),
                new SerialExecutor(fixture.executor),
                fixture.executor,
                budgets,
                space,
                fixture.maxFrameSize,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(10),
                TimeUnit.SECONDS.toNanos(5),
                64L << 20,
                System::nanoTime,
                new ConnectionParams.PreparationObserver() {
                    @Override
                    public void ready(NettyStrandConnection value) {
                        ready.complete(value);
                    }

                    @Override
                    public void failed(PreparationFailure failure) {
                        ready.completeExceptionally(failure.cause());
                    }
                },
                null,
                null,
                null,
                security,
                ByteBufAllocator.DEFAULT);
        recording.config().setOption(ChannelOption.SO_SNDBUF, 4096);
        recording.config().setTcpNoDelay(true);
        NettyStrandConnection created = new NettyStrandConnection(params, recording);
        created.startClient(relayAddress);
        this.connection = ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        awaitServerHandler();
    }

    /**
     * 클라이언트 준비는 AUTH(ACK)에서 끝나므로, 서버가 핸들러를 활성화하기 전에 온 요청은 REFUSED(처리 안 됨)로 거절될 수 있다. 시험이 요청을 내기 전에 서버가
     * 요청을 받기 시작했음을 응답이 오가는 요청 하나로 확인한다.
     */
    private void awaitServerHandler() throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            RequestHandle probe = connection.request(Request.of(AppFixture.TYPE, new byte[0]));
            IncomingRequest request = null;
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
            while (request == null && !probe.result().isDone() && System.nanoTime() < limit) {
                request = received.poll(5, TimeUnit.MILLISECONDS);
            }
            if (request != null) {
                request.respond(Status.OK, new byte[0]);
            }
            awaitTrue(() -> probe.result().isDone());
            if (request != null) {
                return;
            }
        }
        throw new AssertionError("서버가 핸들러를 활성화하지 않았다");
    }

    /** 쓰기 보고가 {@code count}번째 멈춤까지 오기를 기다린다(부분 진행 뒤 소켓이 더 받지 못함). */
    void awaitStalls(long count) throws Exception {
        awaitTrue(() -> recording.stalls.get() >= count);
    }

    static void awaitTrue(java.util.function.BooleanSupplier condition) throws Exception {
        NettyRig.awaitTrue(condition);
    }

    long used(Resource resource) {
        return connection.resourceUsage(resource).current();
    }

    @Override
    public void close() {
        connection.abort();
        relay.close();
        try {
            listener.close();
        } catch (Exception ignored) {
            // 정리 중의 오류는 할 수 있는 일이 없다.
        }
        fixture.close();
    }
}
