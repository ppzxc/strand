package io.github.ppzxc.strand.integration.app;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Result;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

abstract class Rig implements AutoCloseable {
    static final int TYPE = 0x0100;
    static final long WAIT_SECONDS = 15;

    static final class ServerSetup {
        App.AuthCheck auth = mechanism -> "peer";
        OptionalLong lifetime = OptionalLong.empty();
        App.Factory factory;
        App.Handler handler = request -> {};
        Duration grace = Duration.ofSeconds(1);
        Duration deadline = Duration.ofSeconds(5);

        ServerSetup auth(App.AuthCheck value) {
            this.auth = value;
            return this;
        }

        ServerSetup credentialLifetime(long seconds) {
            this.lifetime = OptionalLong.of(seconds);
            return this;
        }

        ServerSetup factory(App.Factory value) {
            this.factory = value;
            return this;
        }

        ServerSetup handler(App.Handler value) {
            this.handler = value;
            return this;
        }

        ServerSetup shutdown(Duration graceValue, Duration deadlineValue) {
            this.grace = graceValue;
            this.deadline = deadlineValue;
            return this;
        }
    }

    static final class ClientSetup {
        App.Handler handler = request -> {};
        Duration grace = Duration.ofSeconds(1);
        Duration deadline = Duration.ofSeconds(5);

        Duration idlePingInterval;
        Duration ackTimeout;
        long writeQueueBytes;

        ClientSetup liveness(Duration idle, Duration ack) {
            this.idlePingInterval = idle;
            this.ackTimeout = ack;
            return this;
        }

        ClientSetup writeQueueBytes(long value) {
            this.writeQueueBytes = value;
            return this;
        }

        ClientSetup handler(App.Handler value) {
            this.handler = value;
            return this;
        }

        ClientSetup shutdown(Duration graceValue, Duration deadlineValue) {
            this.grace = graceValue;
            this.deadline = deadlineValue;
            return this;
        }
    }

    final boolean tls;
    final Spec spec;
    final ExecutorService pool;
    final ToggleExecutor executor;

    Rig(boolean tls, Spec spec) {
        this.tls = tls;
        this.spec = spec;
        AtomicInteger count = new AtomicInteger();
        ThreadFactory factory = task -> {
            Thread thread = new Thread(task, "app-executor-" + count.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.pool = Executors.newCachedThreadPool(factory);
        this.executor = new ToggleExecutor(pool);
    }

    abstract String name();

    abstract App.Server server(ServerSetup setup);

    abstract App.Client client(ClientSetup setup);

    final App.Server server() {
        return server(new ServerSetup());
    }

    final App.Client client() {
        return client(new ClientSetup());
    }

    static InetSocketAddress anyLoopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    static <T> T get(CompletableFuture<T> future) throws Exception {
        return future.get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    <T> T result(Result<T> result) throws Exception {
        return result.toCompletionStage(pool).toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    static void awaitTrue(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("조건이 시간 안에 참이 되지 않았다");
            }
            Thread.sleep(5);
        }
    }

    static long current(App.Server server, Resource resource) {
        return server.usage(resource).current();
    }

    @Override
    public void close() {
        executor.releaseStalled();
        pool.shutdownNow();
    }
}
