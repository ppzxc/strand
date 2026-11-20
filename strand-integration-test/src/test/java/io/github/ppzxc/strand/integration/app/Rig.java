package io.github.ppzxc.strand.integration.app;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Result;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * 한 전송(NIO 또는 Netty)과 한 보안(TCP 또는 TLS)으로 서버·클라이언트를 만드는 시험 도구. 앱용 실행기와 필수 설정을 모두 채운다. 시간 경합에 기대지 않도록 시험은 {@link
 * #get}과 {@link #awaitTrue}로 반환·완료를 기다린다.
 */
abstract class Rig implements AutoCloseable {
    static final int TYPE = 0x0100;
    static final long WAIT_SECONDS = 15;

    /** 서버를 만드는 설정. 팩터리가 있으면 핸들러보다 우선한다. */
    static final class ServerSetup {
        App.AuthCheck auth = mechanism -> "peer";
        App.Factory factory;
        App.Handler handler = request -> {};
        Duration grace = Duration.ofSeconds(1);
        Duration deadline = Duration.ofSeconds(5);

        ServerSetup auth(App.AuthCheck value) {
            this.auth = value;
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

    /** 클라이언트를 만드는 설정. */
    static final class ClientSetup {
        App.Handler handler = request -> {};
        Duration grace = Duration.ofSeconds(1);
        Duration deadline = Duration.ofSeconds(5);

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

    /** 서버 구성만 바꾸는 편의: 기본 설정으로 서버를 만든다. */
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

    /** 서버 소유 객체 범위의 현재 예약량. */
    static long current(App.Server server, Resource resource) {
        return server.usage(resource).current();
    }

    @Override
    public void close() {
        executor.releaseStalled();
        pool.shutdownNow();
    }
}
