package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.CloseNotice;
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

final class NettyWire implements Wire {

    private static final EventLoopGroup GROUP =
            new NioEventLoopGroup(4, new DefaultThreadFactory("integration-netty", true));

    @Override
    public @Nullable ControlledPassage claimPassage(boolean server) {
        return null;
    }

    private static ResourceLimits limits() {
        return TestNio.limits(NettyMinimumProgress.of(
                NettySecurity.externallySecured(),
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                TestNio.REQUESTS_PER_CONNECTION,
                TestNio.REQUESTS_PER_CONNECTION));
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
