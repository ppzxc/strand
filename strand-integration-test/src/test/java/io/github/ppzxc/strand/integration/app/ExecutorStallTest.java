package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Observation;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.netty.NettyResults;
import io.netty.util.concurrent.Future;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ExecutorStallTest {
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

    static java.util.stream.Stream<RigKind> nettyKinds() {
        return RigKind.netty();
    }

    private static Request request(String text) {
        return Request.of(TYPE, text.getBytes(StandardCharsets.UTF_8));
    }

    private final class Stalled {
        final Rig rig;
        final App.Conn connection;
        final List<App.In> served = new CopyOnWriteArrayList<>();

        Stalled(RigKind kind) throws Exception {
            rig = kind.open();
            closeables.add(rig);
            App.Server server = rig.server(new Rig.ServerSetup().handler(served::add));
            App.Lsn listener = Rig.get(server.listen());
            closeables.add(listener::close);
            connection = Rig.get(rig.client().connect(listener.address()));
            closeables.add(connection::abort);

            RequestHandle probe = connection.request(request("probe"), RequestOptions.noDeadline());
            awaitTrue(() -> served.size() == 1);
            probe.cancel();
            rig.result(probe.result());
            rig.executor.stalled(true);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void resultsStayReadableAndCancelAndAbortProgressWhileTheAppExecutorNeverRunsAnything(RigKind kind)
            throws Exception {
        Stalled stalled = new Stalled(kind);
        App.Conn connection = stalled.connection;
        RequestHandle first = connection.request(request("first"), RequestOptions.noDeadline());
        RequestHandle second = connection.request(request("second"), RequestOptions.noDeadline());

        AtomicBoolean observed = new AtomicBoolean();
        Observation stuck = first.result().observe(result -> observed.set(true));

        AtomicBoolean observedElsewhere = new AtomicBoolean();
        first.result().observe(stalled.rig.pool, result -> observedElsewhere.set(true));
        awaitTrue(() -> connection.usage(Resource.OBSERVATIONS).current() >= 1);

        first.cancel();
        awaitTrue(() -> first.result().isDone());
        RequestResult cancelled = first.result().resultNow();

        assertThat(cancelled.status()).isEqualTo(Status.CANCELLED);
        awaitTrue(observedElsewhere::get);
        assertThat(observed).isFalse();
        assertThat(stuck.isDone()).isFalse();

        assertThat(second.result().isDone()).isFalse();

        connection.abort();
        awaitTrue(() -> connection.closed().isDone());

        assertThat(second.result().isDone()).isTrue();
        RequestResult disconnected = second.result().resultNow();
        assertThat(disconnected.certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThat(first.result().resultNow()).isSameAs(cancelled);
        assertThat(connection.closed().resultNow()).isNotNull();

        stalled.rig.executor.releaseStalled();
        awaitTrue(observed::get);
        awaitTrue(stuck::isDone);
        assertThat(stuck.failureNow()).isEmpty();
        awaitTrue(() -> connection.usage(Resource.OBSERVATIONS).current() == 0);
        assertThat(first.result().resultNow()).isSameAs(cancelled);
        assertThat(second.result().resultNow()).isSameAs(disconnected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nettyKinds")
    void nettyConversionsCompleteWithoutTheAppExecutorAndCancellingOneChangesNeitherTheOthersNorTheRequest(RigKind kind)
            throws Exception {
        Stalled stalled = new Stalled(kind);
        App.Conn connection = stalled.connection;
        RequestHandle first = connection.request(request("first"), RequestOptions.noDeadline());
        RequestHandle second = connection.request(request("second"), RequestOptions.noDeadline());
        Future<RequestResult> cancelledConversion = NettyResults.toFuture(first.result());
        Future<RequestResult> firstConversion = NettyResults.toFuture(first.result());
        Future<RequestResult> secondConversion = NettyResults.toFuture(second.result());

        assertThat(cancelledConversion.cancel(false)).isTrue();
        assertThat(cancelledConversion.isCancelled()).isTrue();
        assertThat(firstConversion.isDone()).isFalse();
        assertThat(first.result().isDone()).isFalse();
        assertThat(secondConversion.isDone()).isFalse();

        first.cancel();
        RequestResult converted = firstConversion.get(Rig.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(converted.status()).isEqualTo(Status.CANCELLED);
        assertThat(converted).isSameAs(first.result().resultNow());
        assertThat(cancelledConversion.isCancelled()).isTrue();

        assertThat(secondConversion.isDone()).isFalse();
        assertThat(second.result().isDone()).isFalse();

        connection.abort();
        RequestResult disconnected = secondConversion.get(Rig.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(disconnected.certainty()).isEqualTo(Certainty.UNKNOWN);
        awaitTrue(() -> connection.closed().isDone());
        assertThat(second.result().resultNow()).isSameAs(disconnected);
        assertThat(cancelledConversion.isCancelled()).isTrue();
    }
}
