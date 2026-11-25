package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class HandlerReadinessTest {
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void requestBeforeHandlerIsPreparedIsHeldAndDeliveredOnceItIsActivated(RigKind kind) throws Exception {
        Rig rig = kind.open();
        closeables.add(rig);
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        closeables.add(releaseFactory::countDown);
        AtomicInteger delivered = new AtomicInteger();
        App.Server server = rig.server(new Rig.ServerSetup().factory(connection -> {
            factoryEntered.countDown();
            releaseFactory.await();
            return new App.Prepared(
                    in -> {
                        delivered.incrementAndGet();
                        in.respond(Status.OK, in.payload());
                    },
                    () -> {});
        }));
        App.Client client = rig.client();
        App.Lsn listener = Rig.get(server.listen());
        App.Conn conn = Rig.get(client.connect(listener.address()));
        assertThat(factoryEntered.await(Rig.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        RequestHandle early = conn.request(request("early"), RequestOptions.noDeadline());
        Thread.sleep(200);

        assertThat(early.result().isDone()).isFalse();
        assertThat(delivered).hasValue(0);

        releaseFactory.countDown();
        RequestResult result = rig.result(early.result());
        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(delivered).hasValue(1);

        RequestResult later = rig.result(
                conn.request(request("late"), RequestOptions.noDeadline()).result());
        assertThat(later.status()).isEqualTo(Status.OK);
        assertThat(delivered).hasValue(2);
    }

    private static Request request(String text) {
        return Request.of(TYPE, text.getBytes(StandardCharsets.UTF_8));
    }
}
