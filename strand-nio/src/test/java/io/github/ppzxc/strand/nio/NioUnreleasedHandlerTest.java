package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NioUnreleasedHandlerTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private static void awaitTrue(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("조건이 시간 안에 참이 되지 않았다");
            }
            Thread.sleep(10);
        }
    }

    private final class Scenario {
        final AppFixture server = track(AppFixture.plain());
        final AppFixture clientFixture = track(AppFixture.plain());
        final CountDownLatch factoryEntered = new CountDownLatch(1);
        final CountDownLatch releaseFactory = new CountDownLatch(1);
        final AtomicInteger disposals = new AtomicInteger();
        final NioStrandServer app;
        final NioStrandConnection clientConnection;

        Scenario() throws Exception {
            app = server.serverBuilder()
                    .authenticate(c -> AuthenticationResult.accepted("p"))
                    .handlerFactory(connection -> {
                        factoryEntered.countDown();
                        releaseFactory.await();
                        return PreparedHandler.of(request -> {}, disposals::incrementAndGet);
                    })
                    .build();
            Listener listener = AppFixture.get(app.listen(AppFixture.anyLoopback()));
            closeables.add(listener::close);
            clientConnection =
                    AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
            assertThat(factoryEntered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
            clientConnection.abort();
            awaitTrue(() -> app.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        }

        void returnFactoryWhileExecutorRejects() {
            server.executor.rejecting = true;
            releaseFactory.countDown();
        }
    }

    private AppFixture track(AppFixture fixture) {
        closeables.add(fixture);
        return fixture;
    }

    @Test
    void aRejectedDisposalIsKeptForTheAppWithItsReservationAndNeverRunsOnTheTransportThread() throws Exception {
        Scenario scenario = new Scenario();

        scenario.returnFactoryWhileExecutorRejects();

        awaitTrue(() -> scenario.app.unreleasedHandlers().size() == 1);

        assertThat(scenario.disposals).hasValue(0);
        assertThat(scenario.app.resourceUsage(Resource.FACTORY_TASKS).current()).isEqualTo(1);

        assertThat(scenario.app.unreleasedHandlers()).hasSize(1);
        assertThat(scenario.app.resourceUsage(Resource.FACTORY_TASKS).current()).isEqualTo(1);
    }

    @Test
    void claimTransfersOwnershipOnceReturnsTheReservationAndTheAppDisposesOnce() throws Exception {
        Scenario scenario = new Scenario();
        scenario.returnFactoryWhileExecutorRejects();
        awaitTrue(() -> scenario.app.unreleasedHandlers().size() == 1);
        UnreleasedHandler unreleased = scenario.app.unreleasedHandlers().get(0);

        Optional<PreparedHandler> claimed = unreleased.claim();

        assertThat(claimed).isPresent();
        assertThat(unreleased.claim()).isEmpty();
        assertThat(scenario.app.unreleasedHandlers()).isEmpty();
        assertThat(scenario.app.resourceUsage(Resource.FACTORY_TASKS).current()).isZero();
        claimed.get().dispose();
        claimed.get().dispose();
        assertThat(scenario.disposals).hasValue(1);
    }

    @Test
    void concurrentClaimsTransferTheOwnershipToExactlyOneCaller() throws Exception {
        Scenario scenario = new Scenario();
        scenario.returnFactoryWhileExecutorRejects();
        awaitTrue(() -> scenario.app.unreleasedHandlers().size() == 1);
        UnreleasedHandler unreleased = scenario.app.unreleasedHandlers().get(0);
        int callers = 8;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                if (unreleased.claim().isPresent()) {
                    winners.incrementAndGet();
                }
            });
            thread.start();
            threads.add(thread);
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
        }

        assertThat(winners).hasValue(1);
        assertThat(scenario.app.resourceUsage(Resource.FACTORY_TASKS).current()).isZero();
    }

    @Test
    void anAcceptedDisposalThatNeverReturnsKeepsTheSlotAndIsNotListedAsUnreleased() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        CountDownLatch disposalEntered = new CountDownLatch(1);
        CountDownLatch releaseDisposal = new CountDownLatch(1);
        NioStrandServer app = server.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handlerFactory(connection -> {
                    factoryEntered.countDown();
                    releaseFactory.await();
                    return PreparedHandler.of(request -> {}, () -> {
                        disposalEntered.countDown();
                        try {
                            releaseDisposal.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                })
                .build();
        Listener listener = AppFixture.get(app.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        assertThat(factoryEntered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                .isTrue();
        connection.abort();
        awaitTrue(() -> app.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);

        releaseFactory.countDown();

        assertThat(disposalEntered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                .isTrue();
        assertThat(app.resourceUsage(Resource.FACTORY_TASKS).current()).isEqualTo(1);
        assertThat(app.unreleasedHandlers()).isEmpty();
        releaseDisposal.countDown();
        awaitTrue(() -> app.resourceUsage(Resource.FACTORY_TASKS).current() == 0);
    }
}
