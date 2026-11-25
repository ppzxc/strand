package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SaturationCrossFlowTest {
    private static final Resource[] REQUEST_RESOURCES = {
        Resource.OUTGOING_REQUESTS,
        Resource.OUTGOING_BYTES,
        Resource.INCOMING_REQUESTS,
        Resource.INCOMING_BYTES,
        Resource.RESPONSE_BYTES,
        Resource.HANDLER_TASKS
    };

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

    private static Request request(String text) {
        return Request.of(TYPE, text.getBytes(StandardCharsets.UTF_8));
    }

    private static RequestResult await(Rig rig, RequestHandle handle) throws Exception {
        return rig.result(handle.result());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void perConnectionSaturationKeepsCancelResultAndCloseProgressingAndReturnsEveryReservation(RigKind kind)
            throws Exception {
        Rig rig = open(kind, new Spec().perConnection(2));
        List<App.In> held = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup().handler(held::add));
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        App.Client client = rig.client();
        App.Conn connection = Rig.get(client.connect(listener.address()));
        closeables.add(connection::abort);

        RequestHandle first = connection.request(request("one"), RequestOptions.noDeadline());
        RequestHandle second = connection.request(request("two"), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 2);
        RequestHandle overflow = connection.request(request("three"), RequestOptions.noDeadline());

        RequestResult refused = await(rig, overflow);
        assertThat(refused.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(refused.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(refused.resource()).hasValue(Resource.OUTGOING_REQUESTS);
        assertThat(held).hasSize(2);
        assertThat(connection.usage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(2);
        assertThat(connection.usage(Resource.OUTGOING_REQUESTS).rejections()).isEqualTo(1);

        first.cancel();
        RequestResult cancelled = await(rig, first);
        assertThat(cancelled.status()).isEqualTo(Status.CANCELLED);
        awaitTrue(() -> held.get(0).ended().isDone());
        assertThat(held.get(0).ended().resultNow().isPeerReset()).isTrue();
        awaitTrue(() -> connection.usage(Resource.OUTGOING_REQUESTS).current() == 1);
        RequestHandle third = connection.request(request("four"), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 3);
        assertThat(third.result().isDone()).isFalse();
        assertThat(connection.usage(Resource.OUTGOING_REQUESTS).highWater()).isEqualTo(2);

        assertThat(rig.result(held.get(1).respond(Status.OK, "answer".getBytes(StandardCharsets.UTF_8)))
                        .kind())
                .isEqualTo(AcceptanceKind.ACCEPTED);
        RequestResult answered = await(rig, second);
        assertThat(answered.status()).isEqualTo(Status.OK);
        assertThat(answered.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(answered.payload(), StandardCharsets.UTF_8)).isEqualTo("answer");

        AtomicBoolean pendingDoneWhenClosedObserved = new AtomicBoolean();
        connection
                .closed()
                .observe(notice ->
                        pendingDoneWhenClosedObserved.set(third.result().isDone()));
        connection.abort();
        awaitTrue(() -> connection.closed().isDone());
        assertThat(third.result().isDone()).isTrue();
        awaitTrue(() -> held.get(2).connection().closed().isDone());
        awaitTrue(pendingDoneWhenClosedObserved::get);
        RequestResult disconnected = third.result().resultNow();
        assertThat(disconnected.certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThat(first.result().resultNow()).isSameAs(cancelled);
        assertThat(second.result().resultNow()).isSameAs(answered);

        for (Resource resource : REQUEST_RESOURCES) {
            awaitTrue(() -> client.usage(resource).current() == 0);
            awaitTrue(() -> server.usage(resource).current() == 0);
        }
        awaitTrue(() -> client.usage(Resource.TOTAL_CONNECTIONS).current() == 0);
        awaitTrue(() -> server.usage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void ownerWideSaturationAcrossConnectionsKeepsCancelAndCloseProgressingAndKeepsTheOwnerLimit(RigKind kind)
            throws Exception {

        Rig rig = open(kind, new Spec().perConnection(4).connections(2).ownerOutgoingCount(2));
        List<App.In> held = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup().handler(held::add));
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        App.Client client = rig.client();
        App.Conn a = Rig.get(client.connect(listener.address()));
        closeables.add(a::abort);
        App.Conn b = Rig.get(client.connect(listener.address()));
        closeables.add(b::abort);

        RequestHandle a1 = a.request(request("a1"), RequestOptions.noDeadline());
        RequestHandle a2 = a.request(request("a2"), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 2);
        RequestResult refused = await(rig, b.request(request("b-refused"), RequestOptions.noDeadline()));

        assertThat(refused.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(refused.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(refused.resource()).hasValue(Resource.OUTGOING_REQUESTS);
        assertThat(client.usage(Resource.OUTGOING_REQUESTS).limit()).isEqualTo(2);
        assertThat(client.usage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(2);
        assertThat(client.usage(Resource.OUTGOING_REQUESTS).rejections()).isGreaterThanOrEqualTo(1);

        a1.cancel();
        assertThat(await(rig, a1).status()).isEqualTo(Status.CANCELLED);
        awaitTrue(() -> client.usage(Resource.OUTGOING_REQUESTS).current() == 1);
        RequestHandle b1 = b.request(request("b1"), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 3);
        assertThat(b1.result().isDone()).isFalse();
        assertThat(client.usage(Resource.OUTGOING_REQUESTS).highWater()).isEqualTo(2);

        a.abort();
        b.abort();
        awaitTrue(() -> a.closed().isDone() && b.closed().isDone());
        assertThat(a2.result().isDone()).isTrue();
        assertThat(b1.result().isDone()).isTrue();
        assertThat(a2.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
        awaitTrue(() -> client.usage(Resource.OUTGOING_REQUESTS).current() == 0);
        awaitTrue(() -> client.usage(Resource.OUTGOING_BYTES).current() == 0);
        awaitTrue(() -> server.usage(Resource.INCOMING_REQUESTS).current() == 0);
        awaitTrue(() -> server.usage(Resource.INCOMING_BYTES).current() == 0);
        awaitTrue(() -> client.usage(Resource.TOTAL_CONNECTIONS).current() == 0);
        awaitTrue(() -> server.usage(Resource.TOTAL_CONNECTIONS).current() == 0);
        assertThat(client.usage(Resource.OUTGOING_REQUESTS).highWater()).isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void responseAndObservationSaturationKeepCancelResultAndCloseProgressing(RigKind kind) throws Exception {

        Rig rig = open(kind, new Spec().perConnection(4).bodyBytes(16, 1 << 20).observations(1, 64));
        List<App.In> held = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup().handler(held::add));
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        App.Client client = rig.client();
        App.Conn connection = Rig.get(client.connect(listener.address()));
        closeables.add(connection::abort);
        RequestHandle handle = connection.request(request("hold"), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 1);

        io.github.ppzxc.strand.api.Observation first = handle.result().observe(result -> {});
        io.github.ppzxc.strand.api.Observation refusedObservation =
                handle.result().observe(result -> {});
        assertThat(refusedObservation.isDone()).isTrue();
        assertThat(refusedObservation.failureNow()).isPresent();
        assertThat(connection.usage(Resource.OBSERVATIONS).rejections()).isEqualTo(1);
        assertThat(handle.result().isDone()).isFalse();

        io.github.ppzxc.strand.api.ResponseAcceptance shortage =
                rig.result(held.get(0).respond(Status.OK, new byte[64]));
        assertThat(shortage.kind()).isEqualTo(AcceptanceKind.RESOURCE_EXHAUSTED);
        assertThat(shortage.resource()).hasValue(Resource.RESPONSE_BYTES);

        handle.cancel();

        awaitTrue(() -> handle.result().isDone());
        RequestResult cancelled = handle.result().resultNow();
        assertThat(cancelled.status()).isEqualTo(Status.CANCELLED);
        awaitTrue(() -> held.get(0).ended().isDone());
        assertThat(rig.result(held.get(0).respond(Status.OK, new byte[1])).kind())
                .isEqualTo(AcceptanceKind.REQUEST_ENDED);

        connection.abort();
        awaitTrue(() -> connection.closed().isDone());
        awaitTrue(() -> held.get(0).connection().closed().isDone());
        awaitTrue(first::isDone);
        for (Resource resource : new Resource[] {
            Resource.OBSERVATIONS,
            Resource.OUTGOING_BYTES,
            Resource.OUTGOING_REQUESTS,
            Resource.RESPONSE_BYTES,
            Resource.INCOMING_BYTES
        }) {
            awaitTrue(() -> client.usage(resource).current() == 0);
            awaitTrue(() -> server.usage(resource).current() == 0);
        }
        assertThat(handle.result().resultNow()).isSameAs(cancelled);
    }
}
