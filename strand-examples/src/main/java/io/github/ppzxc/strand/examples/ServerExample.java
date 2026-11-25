package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.nio.AuthenticationResult;
import io.github.ppzxc.strand.nio.Listener;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ServerExample {

    private ServerExample() {}

    public static void main(String[] args) throws Exception {
        Server server = start(Integer.parseInt(ExampleSupport.arg(args, 0, "9000")));
        System.out.println("대기 중 " + server.port());
        Thread.currentThread().join();
    }

    static Server start(int port) throws Exception {

        ExecutorService executor = AppExampleSupport.newAppExecutor("strand-server-example");
        NioSecurity security = NioSecurity.externallySecured();
        MinimumProgressSpace space = NioMinimumProgress.of(
                security,
                AppExampleSupport.MAX_FRAME_SIZE,
                AppExampleSupport.REQUESTS_PER_CONNECTION,
                AppExampleSupport.REQUESTS_PER_CONNECTION);
        NioStrandServer server = NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(AppExampleSupport.limits(space))
                .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                .maxFrameSize(AppExampleSupport.MAX_FRAME_SIZE)
                .security(security)
                .authenticate(credentials -> ExampleSupport.AUTH_MECHANISM.equals(credentials.mechanism())
                                && Arrays.equals(ExampleSupport.AUTH_CREDENTIALS, credentials.secret())
                        ? AuthenticationResult.accepted(AppExampleSupport.PRINCIPAL)
                        : AuthenticationResult.rejected())
                .handler(request -> {
                    String body = new String(request.payload(), StandardCharsets.UTF_8);
                    System.out.println("[서버] 요청 type=" + request.type() + " body=" + body);
                    request.respond(Status.OK, ("echo:" + body).getBytes(StandardCharsets.UTF_8));
                })
                .build();
        try {
            Listener listener = server.listen(new InetSocketAddress(port))
                    .toCompletableFuture()
                    .get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return new Server(server, listener, executor);
        } catch (Exception | Error e) {
            executor.shutdownNow();
            throw e;
        }
    }

    static final class Server implements AutoCloseable {
        private final NioStrandServer server;
        private final Listener listener;
        private final ExecutorService executor;

        private Server(NioStrandServer server, Listener listener, ExecutorService executor) {
            this.server = server;
            this.listener = listener;
            this.executor = executor;
        }

        int port() {
            return ((InetSocketAddress) listener.localAddress()).getPort();
        }

        @Override
        public void close() {
            listener.close();
            try {
                server.shutdown().toCompletableFuture().get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {

            } finally {
                executor.shutdownNow();
            }
        }
    }
}
