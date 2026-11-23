package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.nio.IncomingRequest;
import io.github.ppzxc.strand.nio.Listener;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandConnection;
import io.github.ppzxc.strand.nio.NioStrandServer;
import io.github.ppzxc.strand.nio.PreparedHandler;
import io.github.ppzxc.strand.nio.RequestHandler;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
import io.github.ppzxc.strand.nio.internal.TestNio;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/** strand-nio 제품 공개 API를 중립 모양으로 감싼다. */
final class NioWire implements Wire {

    @Override
    public @Nullable ControlledPassage claimPassage(boolean server) {
        return PassageObservation.claim(server ? PassageWrapping.Side.SERVER : PassageWrapping.Side.CLIENT);
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
        private final NioStrandConnection connection;

        Conn(NioStrandConnection connection) {
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
        NioStrandServer server = TestNio.serverBuilder(Peer.HANDLERS)
                .handlerFactory(connection -> PreparedHandler.of(handler(handlers.apply(new Conn(connection)))))
                .build();
        Listener listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .toCompletableFuture()
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
                return server.shutdown().toCompletableFuture();
            }
        };
    }

    @Override
    public Client newClient(Handler handler, @Nullable Duration idlePingInterval) {
        NioStrandClient.Builder builder = TestNio.clientBuilder(Peer.HANDLERS).handler(handler(handler));
        if (idlePingInterval != null) {
            builder.idlePingInterval(idlePingInterval);
        }
        NioStrandClient client = builder.build();
        return new Client() {
            @Override
            public CompletableFuture<Wire.Conn> connect(InetSocketAddress address) {
                return client.connect(address).toCompletableFuture().thenApply(Conn::new);
            }

            @Override
            public CompletableFuture<Void> shutdown() {
                return client.shutdown().toCompletableFuture();
            }
        };
    }
}
