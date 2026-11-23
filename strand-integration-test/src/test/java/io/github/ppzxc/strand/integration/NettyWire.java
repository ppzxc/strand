package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.AuthenticationResult;
import io.github.ppzxc.strand.netty.IncomingRequest;
import io.github.ppzxc.strand.netty.Listener;
import io.github.ppzxc.strand.netty.NettyMinimumProgress;
import io.github.ppzxc.strand.netty.NettySecurity;
import io.github.ppzxc.strand.netty.NettyStrandClient;
import io.github.ppzxc.strand.netty.NettyStrandConnection;
import io.github.ppzxc.strand.netty.NettyStrandServer;
import io.github.ppzxc.strand.netty.PreparedHandler;
import io.github.ppzxc.strand.netty.RequestHandler;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import io.github.ppzxc.strand.nio.internal.TestNio;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * strand-netty 제품 공개 API를 중립 모양으로 감싼다. 설정 값은 {@code TestNio}와 같다. 종료 마무리 기한도 같다: 양쪽이 동시에 CLOSE를 보내면 한쪽이 상대의 끊김을 보지 못하고
 * 마무리 기한까지 기다리는 경우가 있어 기본값 대신 짧게 둔다.
 */
final class NettyWire implements Wire {

    /** 시험 전체가 함께 쓰는 이벤트 루프. 데몬 스레드라 종료를 막지 않는다. */
    private static final EventLoopGroup GROUP =
            new NioEventLoopGroup(4, new DefaultThreadFactory("integration-netty", true));

    private static final long BODY_BYTES = 1 << 20;
    private static final long OWNER_BODY_BYTES = 1 << 24;

    @Override
    public @Nullable ControlledPassage claimPassage(boolean server) {
        return null;
    }

    private static ResourceLimits limits() {
        long requests = TestNio.REQUESTS_PER_CONNECTION;
        long connections = TestNio.CONNECTIONS;
        MinimumProgressSpace space = NettyMinimumProgress.of(
                NettySecurity.externallySecured(), EndpointConfig.DEFAULT_MAX_FRAME_SIZE, requests, requests);
        long[] min = {
            space.get(Resource.FRAME_ASSEMBLY_BYTES),
            space.get(Resource.CONTROL_OUTPUT_BYTES),
            space.get(Resource.RESULT_EVENT_BYTES),
            space.get(Resource.TLS_PLAINTEXT_BYTES),
            space.get(Resource.TLS_CIPHERTEXT_BYTES),
            space.get(Resource.TLS_CONTROL_BYTES)
        };
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(requests, BODY_BYTES)
                        .incomingRequests(requests, BODY_BYTES)
                        .responseBytes(BODY_BYTES)
                        .handlerTasks(requests)
                        .observations(64)
                        .preparationBytes(1 << 16)
                        .progressBytes(min[0], min[1], min[2], min[3], min[4], min[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(connections)
                        .totalConnections(connections)
                        .preparationTasks(connections, connections, connections)
                        .outgoingRequests(requests * connections, OWNER_BODY_BYTES)
                        .incomingRequests(requests * connections, OWNER_BODY_BYTES)
                        .responseBytes(OWNER_BODY_BYTES)
                        .handlerTasks(requests * connections)
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

    private static <T> CompletableFuture<T> cf(Future<T> future) {
        CompletableFuture<T> result = new CompletableFuture<>();
        future.addListener(done -> {
            if (done.isSuccess()) {
                result.complete(future.getNow());
            } else {
                result.completeExceptionally(done.cause());
            }
        });
        return result;
    }

    private static final class In implements Incoming {
        private final IncomingRequest request;

        In(IncomingRequest request) {
            this.request = request;
        }

        @Override
        public int type() {
            return request.type();
        }

        @Override
        public byte[] payload() {
            return request.payload();
        }

        @Override
        public Result<ResponseAcceptance> respond(int status, byte[] body) {
            return request.respond(status, body);
        }
    }

    private static final class Conn implements Wire.Conn {
        private final NettyStrandConnection connection;

        Conn(NettyStrandConnection connection) {
            this.connection = connection;
        }

        @Override
        public RequestHandle request(Request request) {
            return connection.request(request);
        }

        @Override
        public RequestHandle request(Request request, RequestOptions options) {
            return connection.request(request, options);
        }

        @Override
        public void close() {
            connection.close();
        }

        @Override
        public void abort() {
            connection.abort();
        }

        @Override
        public Result<CloseNotice> closed() {
            return connection.closed();
        }

        @Override
        public OptionalLong credentialLifetime() {
            return connection.credentialLifetime();
        }
    }

    private static RequestHandler handler(Handler handler) {
        return request -> handler.handle(new In(request));
    }

    @Override
    public Server startServer(Function<Wire.Conn, Handler> handlers) throws Exception {
        NettyStrandServer server = NettyStrandServer.builder()
                .eventLoopGroup(GROUP)
                .executor(Peer.HANDLERS)
                .resourceLimits(limits())
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .closeFinishDeadline(Duration.ofSeconds(1))
                .security(NettySecurity.externallySecured())
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handlerFactory(connection -> PreparedHandler.of(handler(handlers.apply(new Conn(connection)))))
                .build();
        Listener listener = cf(server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)))
                .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
        int port = ((InetSocketAddress) listener.localAddress()).getPort();
        return new Server() {
            @Override
            public int port() {
                return port;
            }

            @Override
            public ResourceUsage usage(Resource resource) {
                return server.resourceUsage(resource);
            }

            @Override
            public void closeListener() {
                listener.close();
            }

            @Override
            public CompletableFuture<Void> shutdown() {
                return cf(server.shutdown());
            }
        };
    }

    @Override
    public Client newClient(Handler handler, @Nullable Duration idlePingInterval) {
        NettyStrandClient.Builder builder = NettyStrandClient.builder()
                .eventLoopGroup(GROUP)
                .executor(Peer.HANDLERS)
                .resourceLimits(limits())
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .closeFinishDeadline(Duration.ofSeconds(1))
                .security(NettySecurity.externallySecured())
                .handler(handler(handler));
        if (idlePingInterval != null) {
            builder.idlePingInterval(idlePingInterval);
        }
        NettyStrandClient client = builder.build();
        return new Client() {
            @Override
            public CompletableFuture<Wire.Conn> connect(InetSocketAddress address) {
                return cf(client.connect(address)).thenApply(Conn::new);
            }

            @Override
            public CompletableFuture<Void> shutdown() {
                return cf(client.shutdown());
            }
        };
    }
}
