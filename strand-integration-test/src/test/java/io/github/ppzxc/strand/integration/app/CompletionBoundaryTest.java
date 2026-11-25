package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ShutdownKind;
import io.github.ppzxc.strand.api.Status;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CompletionBoundaryTest {
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

    private static Request request(String text) {
        return Request.of(TYPE, text.getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void listenerConnectionShutdownResultAndResourceTerminationCompleteAtDifferentBoundaries(RigKind kind)
            throws Exception {
        Rig rig = kind.open();
        closeables.add(rig);
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        closeables.add(releaseHandler::countDown);
        List<App.In> served = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup()
                .shutdown(Duration.ofMillis(100), Duration.ofMillis(500))
                .handler(in -> {
                    served.add(in);
                    if (new String(in.payload(), StandardCharsets.UTF_8).equals("hold")) {
                        handlerEntered.countDown();
                        releaseHandler.await();
                    } else {
                        in.respond(Status.OK, in.payload());
                    }
                }));
        App.Client client = rig.client();
        App.Lsn listener = Rig.get(server.listen());
        App.Conn stuck = Rig.get(client.connect(listener.address()));
        App.Conn alive = Rig.get(client.connect(listener.address()));
        stuck.request(request("hold"), RequestOptions.noDeadline());
        assertThat(handlerEntered.await(Rig.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        listener.close();
        Rig.get(listener.closed());
        RequestResult echoed = rig.result(
                alive.request(request("still"), RequestOptions.noDeadline()).result());
        assertThat(echoed.status()).isEqualTo(Status.OK);
        assertThat(server.usage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(2);

        stuck.abort();
        awaitTrue(() -> stuck.closed().isDone());
        awaitTrue(() -> served.get(0).connection().closed().isDone());
        assertThat(server.usage(Resource.HANDLER_TASKS).current()).isEqualTo(1);

        server.shutdown();
        awaitTrue(() -> server.shutdownResult().isDone());
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThat(server.shutdownResult().resultNow().remainingResources()).containsEntry(Resource.HANDLER_TASKS, 1L);
        assertThat(server.resourcesTerminated().isDone()).isFalse();

        releaseHandler.countDown();
        Rig.get(server.resourcesTerminated());
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        awaitTrue(() -> alive.closed().isDone());
        assertThat(server.usage(Resource.HANDLER_TASKS).current()).isZero();
        assertThat(server.usage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        Rig.get(client.shutdown());
        Rig.get(client.resourcesTerminated());
    }
}
