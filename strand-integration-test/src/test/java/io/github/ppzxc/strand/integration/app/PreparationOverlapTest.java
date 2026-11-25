package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ShutdownKind;
import io.github.ppzxc.strand.api.ShutdownResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class PreparationOverlapTest {
    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    static java.util.stream.Stream<RigKind> kinds() {
        return RigKind.all();
    }

    private Rig open(RigKind kind, Spec spec) throws Exception {
        Rig rig = kind.open(spec);
        closeables.add(rig);
        return rig;
    }

    private static Throwable rootCause(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void preparationSaturationCancelLateFactoryRejectedDisposalClaimAndMissedShutdownDeadlineOverlap(RigKind kind)
            throws Exception {
        Rig serverRig = open(kind, new Spec().connections(4).preparing(1));
        Rig clientRig = open(kind, new Spec());
        CountDownLatch authEntered = new CountDownLatch(1);
        CountDownLatch releaseAuth = new CountDownLatch(1);
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        AtomicInteger authCalls = new AtomicInteger();
        AtomicInteger factoryCalls = new AtomicInteger();
        AtomicInteger disposals = new AtomicInteger();
        App.Server server = serverRig.server(new Rig.ServerSetup()
                .shutdown(Duration.ofMillis(100), Duration.ofMillis(500))
                .auth(mechanism -> {
                    if (authCalls.incrementAndGet() == 1) {
                        authEntered.countDown();
                        releaseAuth.await();
                    }
                    return "peer";
                })
                .factory(connection -> {
                    if (factoryCalls.incrementAndGet() == 1) {
                        factoryEntered.countDown();
                        releaseFactory.await();
                    }
                    return new App.Prepared(request -> {}, disposals::incrementAndGet);
                }));
        App.Lsn listener = Rig.get(server.listen());
        App.Client client = clientRig.client();

        App.Attempt held = client.connectAttempt(listener.address());
        CompletableFuture<App.Conn> heldResult = held.result();
        assertThat(authEntered.await(Rig.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        App.Attempt refused = client.connectAttempt(listener.address());
        assertThatThrownBy(() -> Rig.get(refused.result())).isInstanceOf(ExecutionException.class);
        assertThat(server.usage(Resource.PREPARING_CONNECTIONS).rejections()).isEqualTo(1);
        assertThat(authCalls).hasValue(1);

        assertThat(held.cancel()).isTrue();
        assertThatThrownBy(() -> Rig.get(heldResult))
                .isInstanceOf(ExecutionException.class)
                .satisfies(error -> assertThat(rootCause(error).getClass().getSimpleName())
                        .isEqualTo("ConnectionAttemptCancelledException"));
        Rig.get(held.transportReleased());
        awaitTrue(() -> server.usage(Resource.PREPARING_CONNECTIONS).current() == 0);
        assertThat(server.usage(Resource.AUTHENTICATION_TASKS).current()).isEqualTo(1);

        releaseAuth.countDown();
        awaitTrue(() -> server.usage(Resource.AUTHENTICATION_TASKS).current() == 0);
        awaitTrue(() -> server.usage(Resource.TOTAL_CONNECTIONS).current() == 0);
        assertThat(factoryCalls).hasValue(0);
        assertThat(heldResult.isCompletedExceptionally()).isTrue();

        App.Conn doomed = Rig.get(client.connect(listener.address()));
        assertThat(factoryEntered.await(Rig.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        doomed.abort();
        awaitTrue(() -> server.usage(Resource.TOTAL_CONNECTIONS).current() == 0);
        assertThat(server.usage(Resource.FACTORY_TASKS).current()).isEqualTo(1);

        CompletableFuture<Void> stopped = server.shutdown();
        awaitTrue(() -> server.shutdownResult().isDone());
        ShutdownResult missed = server.shutdownResult().resultNow();
        assertThat(missed.kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThat(missed.remainingResources()).containsKey(Resource.FACTORY_TASKS);
        assertThat(server.resourcesTerminated().isDone()).isFalse();

        serverRig.executor.rejecting(true);
        releaseFactory.countDown();
        awaitTrue(() -> server.unreleased().size() == 1);
        assertThat(disposals).hasValue(0);
        assertThat(server.usage(Resource.FACTORY_TASKS).current()).isEqualTo(1);
        assertThat(server.unreleased()).hasSize(1);
        assertThat(server.resourcesTerminated().isDone()).isFalse();

        App.Unreleased unreleased = server.unreleased().get(0);
        int callers = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Optional<App.Prepared>> claims = new CopyOnWriteArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    claims.add(unreleased.claim());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            thread.start();
            threads.add(thread);
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(Rig.WAIT_SECONDS));
        }
        assertThat(claims).hasSize(callers);
        List<App.Prepared> owned = new ArrayList<>();
        for (Optional<App.Prepared> claim : claims) {
            claim.ifPresent(owned::add);
        }
        assertThat(owned).hasSize(1);
        assertThat(server.unreleased()).isEmpty();
        assertThat(server.usage(Resource.FACTORY_TASKS).current()).isZero();

        assertThat(disposals).hasValue(0);
        owned.get(0).disposal.run();
        assertThat(disposals).hasValue(1);

        Rig.get(server.resourcesTerminated());
        assertThat(server.shutdownResult().resultNow()).isSameAs(missed);
        assertThatThrownBy(() -> Rig.get(stopped)).isInstanceOf(ExecutionException.class);
        Rig.get(listener.closed());
        for (Resource resource : new Resource[] {
            Resource.PREPARING_CONNECTIONS,
            Resource.TOTAL_CONNECTIONS,
            Resource.AUTHENTICATION_TASKS,
            Resource.FACTORY_TASKS,
            Resource.RESOLVE_TASKS
        }) {
            assertThat(server.usage(resource).current()).as(resource.name()).isZero();
        }
        assertThat(disposals).hasValue(1);
    }
}
