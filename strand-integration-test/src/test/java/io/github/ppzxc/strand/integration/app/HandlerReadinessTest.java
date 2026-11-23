package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
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

/** 핸들러가 준비되기 전의 요청은 거절되어 처리 안 됨으로 끝나고, 준비된 뒤의 요청은 전달된다(#370, 규격 4.5절). */
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
    void requestBeforeHandlerIsPreparedIsRefusedAndLaterRequestIsDelivered(RigKind kind) throws Exception {
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

        RequestResult early = rig.result(
                conn.request(request("early"), RequestOptions.noDeadline()).result());
        assertThat(early.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(early.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(delivered).hasValue(0);

        // 팩터리가 끝나 핸들러가 준비되는 시점은 비동기이므로, 준비 전 요청은 계속 거절되다가 준비 뒤 요청은 전달된다.
        releaseFactory.countDown();
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(Rig.WAIT_SECONDS);
        RequestResult late;
        do {
            late = rig.result(
                    conn.request(request("late"), RequestOptions.noDeadline()).result());
            if (late.status() != Status.OK) {
                assertThat(late.status()).isEqualTo(Status.UNAVAILABLE);
                assertThat(late.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
                assertThat(delivered).hasValue(0);
                assertThat(System.nanoTime()).isLessThan(limit);
                Thread.sleep(5);
            }
        } while (late.status() != Status.OK);
        assertThat(delivered).hasValue(1);
    }

    private static Request request(String text) {
        return Request.of(TYPE, text.getBytes(StandardCharsets.UTF_8));
    }
}
