package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.Status;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ResourceReleaseTest {
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

    private static void spinUntilDone(Result<?> result) {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(Rig.WAIT_SECONDS);
        while (!result.isDone()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("완료하지 않았다");
            }
            Thread.yield();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void socketsAreAlreadyReleasedWhenConnectionClosedIsObservedOnBothSides(RigKind kind) throws Exception {
        assumeTrue(ResourceProbe.socketsVisible(), "/proc/self/fd가 없는 플랫폼");
        Rig rig = kind.open();
        closeables.add(rig);
        List<App.In> served = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup().handler(request -> {
            served.add(request);
            request.respond(Status.OK, request.payload());
        }));
        App.Client client = rig.client();
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);

        Set<String> baseline = ResourceProbe.sockets();

        for (int i = 0; i < 10; i++) {
            App.Conn connection = Rig.get(client.connect(listener.address()));
            RequestResult echoed = rig.result(connection
                    .request(Request.of(TYPE, "ping".getBytes(StandardCharsets.UTF_8)), RequestOptions.noDeadline())
                    .result());
            assertThat(echoed.status()).isEqualTo(Status.OK);
            App.Conn accepted = served.get(served.size() - 1).connection();

            if (i % 2 == 0) {
                connection.close();
            } else {
                connection.abort();
            }
            spinUntilDone(connection.closed());
            spinUntilDone(accepted.closed());

            assertThat(ResourceProbe.socketsSince(baseline))
                    .as("closed()를 관찰한 직후 남은 소켓(%d번째)", i)
                    .isEmpty();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void nothingTheTestStartedRemainsAfterTheOwnersReportTheirResourcesTerminated(RigKind kind) throws Exception {
        Set<Thread> threadsBefore = ResourceProbe.strandThreads();
        Set<String> socketsBefore = ResourceProbe.socketsVisible() ? ResourceProbe.sockets() : null;
        Rig rig = kind.open();
        closeables.add(rig);
        List<App.In> held = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup()
                .shutdown(java.time.Duration.ofMillis(100), java.time.Duration.ofSeconds(5))
                .handler(held::add));
        App.Client client = rig.client();
        App.Lsn listener = Rig.get(server.listen());
        App.Conn closedEarly = Rig.get(client.connect(listener.address()));
        closedEarly.close();
        awaitTrue(() -> closedEarly.closed().isDone());

        App.Conn busy = Rig.get(client.connect(listener.address()));
        io.github.ppzxc.strand.api.RequestHandle pending =
                busy.request(Request.of(TYPE, "hold".getBytes(StandardCharsets.UTF_8)), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 1);

        listener.close();
        Rig.get(listener.closed());
        Rig.get(client.shutdown());
        Rig.get(server.shutdown());
        Rig.get(client.resourcesTerminated());
        Rig.get(server.resourcesTerminated());

        assertThat(pending.result().isDone()).isTrue();
        assertThat(pending.result().resultNow().certainty())
                .isNotEqualTo(io.github.ppzxc.strand.api.Certainty.RESPONSE);
        assertThat(client.shutdownResult().resultNow().kind())
                .isEqualTo(io.github.ppzxc.strand.api.ShutdownKind.COMPLETED);
        assertThat(server.shutdownResult().resultNow().kind())
                .isEqualTo(io.github.ppzxc.strand.api.ShutdownKind.COMPLETED);
        if (socketsBefore != null) {
            assertThat(ResourceProbe.socketsSince(socketsBefore)).as("남은 소켓").isEmpty();
        }

        awaitTrue(() -> {
            Set<Thread> remaining = ResourceProbe.strandThreads();
            remaining.removeAll(threadsBefore);
            return remaining.isEmpty();
        });
        if (kind.isNetty()) {
            awaitTrue(() -> {
                try {
                    return !ResourceProbe.hasScheduledTasks(((NettyRig) rig).group);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void tlsPlaintextAndCiphertextSpacesAreCountedSeparatelyAndReturnedAfterTheRealCleanup(RigKind kind)
            throws Exception {
        Rig rig = kind.open();
        closeables.add(rig);
        List<App.In> served = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup().handler(served::add));
        App.Client client = rig.client();
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        App.Conn connection = Rig.get(client.connect(listener.address()));
        connection.request(Request.of(TYPE, new byte[1]), RequestOptions.noDeadline());
        awaitTrue(() -> served.size() == 1);

        long plaintext = client.usage(io.github.ppzxc.strand.api.Resource.TLS_PLAINTEXT_BYTES)
                .current();
        long ciphertext = client.usage(io.github.ppzxc.strand.api.Resource.TLS_CIPHERTEXT_BYTES)
                .current();
        long control = client.usage(io.github.ppzxc.strand.api.Resource.TLS_CONTROL_BYTES)
                .current();
        if (kind.isTls()) {

            assertThat(plaintext).isPositive();
            assertThat(ciphertext).isPositive().isNotEqualTo(plaintext);
            assertThat(control).isPositive();
        } else {
            assertThat(plaintext + ciphertext + control).isZero();
        }

        connection.abort();
        awaitTrue(() -> connection.closed().isDone());
        awaitTrue(() -> served.get(0).connection().closed().isDone());

        for (io.github.ppzxc.strand.api.Resource resource : new io.github.ppzxc.strand.api.Resource[] {
            io.github.ppzxc.strand.api.Resource.TLS_PLAINTEXT_BYTES,
            io.github.ppzxc.strand.api.Resource.TLS_CIPHERTEXT_BYTES,
            io.github.ppzxc.strand.api.Resource.TLS_CONTROL_BYTES
        }) {
            awaitTrue(() -> client.usage(resource).current() == 0);
            awaitTrue(() -> server.usage(resource).current() == 0);
        }
    }
}
