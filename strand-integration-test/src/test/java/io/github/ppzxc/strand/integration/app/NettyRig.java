package io.github.ppzxc.strand.integration.app;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.netty.AuthenticationResult;
import io.github.ppzxc.strand.netty.IncomingRequest;
import io.github.ppzxc.strand.netty.Listener;
import io.github.ppzxc.strand.netty.NettyConnectAttempt;
import io.github.ppzxc.strand.netty.NettyMinimumProgress;
import io.github.ppzxc.strand.netty.NettySecurity;
import io.github.ppzxc.strand.netty.NettyStrandClient;
import io.github.ppzxc.strand.netty.NettyStrandConnection;
import io.github.ppzxc.strand.netty.NettyStrandServer;
import io.github.ppzxc.strand.netty.PreparedHandler;
import io.github.ppzxc.strand.netty.UnreleasedHandler;
import io.github.ppzxc.strand.nio.internal.TestTls;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** strand-netty의 앱 공개 API를 중립 모양으로 감싼다. */
final class NettyRig extends Rig {
    final EventLoopGroup group = new NioEventLoopGroup(2);
    private final NettySecurity serverSecurity;
    private final NettySecurity clientSecurity;
    private final io.github.ppzxc.strand.api.ResourceLimits limits;

    NettyRig(boolean tls, Spec spec) throws Exception {
        super(tls, spec);
        this.serverSecurity = tls ? NettySecurity.tls(TestTls.server()) : NettySecurity.externallySecured();
        this.clientSecurity = tls ? NettySecurity.tls(TestTls.client()) : NettySecurity.externallySecured();
        io.github.ppzxc.strand.api.MinimumProgressSpace space =
                NettyMinimumProgress.of(clientSecurity, spec.maxFrameSize, spec.perConnection, spec.perConnection);
        this.limits = spec.build(new long[] {
            space.get(Resource.FRAME_ASSEMBLY_BYTES),
            space.get(Resource.CONTROL_OUTPUT_BYTES),
            space.get(Resource.RESULT_EVENT_BYTES),
            space.get(Resource.TLS_PLAINTEXT_BYTES),
            space.get(Resource.TLS_CIPHERTEXT_BYTES),
            space.get(Resource.TLS_CONTROL_BYTES)
        });
    }

    @Override
    String name() {
        return "netty";
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

    private static final class Conn implements App.Conn {
        final NettyStrandConnection connection;

        Conn(NettyStrandConnection connection) {
            this.connection = connection;
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
        public void expireCredential() {
            connection.expireCredential();
        }

        @Override
        public void revokeCredential() {
            connection.revokeCredential();
        }

        @Override
        public void closeExcessiveUse() {
            connection.closeExcessiveUse();
        }

        @Override
        public Result<CloseNotice> closed() {
            return connection.closed();
        }

        @Override
        public ResourceUsage usage(Resource resource) {
            return connection.resourceUsage(resource);
        }

        @Override
        public Object principal() {
            return connection.authenticatedPeer().principal();
        }
    }

    private static final class In implements App.In {
        final IncomingRequest request;

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

        @Override
        public Result<RequestEnd> ended() {
            return request.ended();
        }

        @Override
        public App.Conn connection() {
            return new Conn(request.connection());
        }
    }

    private static io.github.ppzxc.strand.netty.RequestHandler handler(App.Handler handler) {
        return request -> handler.handle(new In(request));
    }

    private static PreparedHandler prepared(App.Prepared prepared) {
        return PreparedHandler.of(handler(prepared.handler), prepared.disposal);
    }

    private static App.Prepared neutral(PreparedHandler prepared) {
        return new App.Prepared(request -> prepared.handler().handle(((In) request).request), prepared::dispose);
    }

    @Override
    App.Server server(ServerSetup setup) {
        NettyStrandServer.Builder builder = NettyStrandServer.builder()
                .eventLoopGroup(group)
                .executor(executor)
                .resourceLimits(limits)
                .preparationDeadline(java.time.Duration.ofSeconds(10))
                .shutdownGrace(setup.grace)
                .shutdownDeadline(setup.deadline)
                .maxFrameSize(spec.maxFrameSize)
                .security(serverSecurity)
                .authenticate(credentials -> {
                    Object principal = setup.auth.verify(credentials.mechanism());
                    return principal == null
                            ? AuthenticationResult.rejected()
                            : AuthenticationResult.accepted(principal);
                });
        if (setup.factory != null) {
            builder.handlerFactory(connection -> prepared(setup.factory.create(new Conn(connection))));
        } else {
            builder.handler(handler(setup.handler));
        }
        NettyStrandServer server = builder.build();
        return new App.Server() {
            @Override
            public CompletableFuture<App.Lsn> listen() {
                return cf(server.listen(anyLoopback())).thenApply(listener -> new Lsn(listener));
            }

            @Override
            public CompletableFuture<Void> shutdown() {
                return cf(server.shutdown());
            }

            @Override
            public Result<ShutdownResult> shutdownResult() {
                return server.shutdownResult();
            }

            @Override
            public CompletableFuture<Void> resourcesTerminated() {
                return cf(server.resourcesTerminated());
            }

            @Override
            public List<App.Unreleased> unreleased() {
                List<App.Unreleased> list = new ArrayList<>();
                for (UnreleasedHandler item : server.unreleasedHandlers()) {
                    list.add(() -> {
                        Optional<PreparedHandler> claimed = item.claim();
                        return claimed.map(NettyRig::neutral);
                    });
                }
                return list;
            }

            @Override
            public ResourceUsage usage(Resource resource) {
                return server.resourceUsage(resource);
            }
        };
    }

    private static final class Lsn implements App.Lsn {
        final Listener listener;

        Lsn(Listener listener) {
            this.listener = listener;
        }

        @Override
        public InetSocketAddress address() {
            SocketAddress bound = listener.localAddress();
            return new InetSocketAddress(
                    java.net.InetAddress.getLoopbackAddress(), ((InetSocketAddress) bound).getPort());
        }

        @Override
        public void close() {
            listener.close();
        }

        @Override
        public CompletableFuture<Void> closed() {
            return cf(listener.closed());
        }
    }

    @Override
    App.Client client(ClientSetup setup) {
        NettyStrandClient client = NettyStrandClient.builder()
                .eventLoopGroup(group)
                .executor(executor)
                .resourceLimits(limits)
                .preparationDeadline(java.time.Duration.ofSeconds(10))
                .shutdownGrace(setup.grace)
                .shutdownDeadline(setup.deadline)
                .maxFrameSize(spec.maxFrameSize)
                .security(clientSecurity)
                .handler(handler(setup.handler))
                .build();
        return new App.Client() {
            @Override
            public App.Attempt connectAttempt(InetSocketAddress address) {
                NettyConnectAttempt attempt = client.connectAttempt(address);
                return new App.Attempt() {
                    @Override
                    public CompletableFuture<App.Conn> result() {
                        return cf(attempt.result()).thenApply(Conn::new);
                    }

                    @Override
                    public boolean cancel() {
                        return attempt.cancel();
                    }

                    @Override
                    public CompletableFuture<Void> transportReleased() {
                        return cf(attempt.transportReleased());
                    }
                };
            }

            @Override
            public CompletableFuture<App.Conn> connect(InetSocketAddress address) {
                return cf(client.connect(address)).thenApply(Conn::new);
            }

            @Override
            public CompletableFuture<Void> shutdown() {
                return cf(client.shutdown());
            }

            @Override
            public Result<ShutdownResult> shutdownResult() {
                return client.shutdownResult();
            }

            @Override
            public CompletableFuture<Void> resourcesTerminated() {
                return cf(client.resourcesTerminated());
            }

            @Override
            public ResourceUsage usage(Resource resource) {
                return client.resourceUsage(resource);
            }
        };
    }

    @Override
    public void close() {
        super.close();
        group.shutdownGracefully(0, 2, java.util.concurrent.TimeUnit.SECONDS)
                .awaitUninterruptibly(10, java.util.concurrent.TimeUnit.SECONDS);
    }
}
