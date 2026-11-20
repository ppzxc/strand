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

/**
 * 연결별·소유 객체 전체 요청 한도가 찬 채로 취소·결과 보존·연결 종료가 이어지고 예약이 반환되어 전체 한도가 유지되는 교차 흐름을 실제 TCP/TLS에서 본다(#328). 개별 포화
 * 시험(#318 #324)이 한 자원씩 보는 것과 달리 한도·취소·응답·종료를 한 연결 흐름에서 잇는다.
 */
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

        // 한도를 넘은 요청은 보내지 않고 처리 안 됨으로 확정된다. 원인과 거절 횟수가 집계에 남는다.
        RequestResult refused = await(rig, overflow);
        assertThat(refused.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(refused.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(refused.resource()).hasValue(Resource.OUTGOING_REQUESTS);
        assertThat(held).hasSize(2);
        assertThat(connection.usage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(2);
        assertThat(connection.usage(Resource.OUTGOING_REQUESTS).rejections()).isEqualTo(1);

        // 포화 중에도 기존 요청 취소가 진행되고 예약이 돌아와 새 요청이 접수된다.
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

        // 응답은 취소·포화와 상관없이 결과로 보존된다.
        assertThat(rig.result(held.get(1).respond(Status.OK, "answer".getBytes(StandardCharsets.UTF_8)))
                        .kind())
                .isEqualTo(AcceptanceKind.ACCEPTED);
        RequestResult answered = await(rig, second);
        assertThat(answered.status()).isEqualTo(Status.OK);
        assertThat(answered.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(answered.payload(), StandardCharsets.UTF_8)).isEqualTo("answer");

        // 남은 요청 결과는 종료 사건보다 먼저 보존된다. 종료 알림 시점에 결과가 이미 확정되어 있어야 한다.
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

        // 모든 요청·응답·핸들러 예약이 연결과 소유 객체 양쪽에서 돌아온다.
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
        // 연결 하나의 한도는 4지만 소유 객체 전체 송신 한도는 2다: 한 연결이 전체 한도를 다 쓰면 다른 연결이 거절된다.
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

        // 취소가 소유 객체 전체 예약을 돌려주어 다른 연결이 한도 안에서 다시 접수된다.
        a1.cancel();
        assertThat(await(rig, a1).status()).isEqualTo(Status.CANCELLED);
        awaitTrue(() -> client.usage(Resource.OUTGOING_REQUESTS).current() == 1);
        RequestHandle b1 = b.request(request("b1"), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 3);
        assertThat(b1.result().isDone()).isFalse();
        assertThat(client.usage(Resource.OUTGOING_REQUESTS).highWater()).isEqualTo(2);

        // 한도가 찬 상태에서 두 연결을 끊어도 남은 결과는 보존되고 전체 예약은 0으로 돌아온다.
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
        // 응답 바이트는 16바이트, 연결의 관찰 자리는 하나다.
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

        // 관찰 자리가 차면 다음 관찰은 실패한 Observation이고 원래 결과는 그대로다.
        io.github.ppzxc.strand.api.Observation first = handle.result().observe(result -> {});
        io.github.ppzxc.strand.api.Observation refusedObservation =
                handle.result().observe(result -> {});
        assertThat(refusedObservation.isDone()).isTrue();
        assertThat(refusedObservation.failureNow()).isPresent();
        assertThat(connection.usage(Resource.OBSERVATIONS).rejections()).isEqualTo(1);
        assertThat(handle.result().isDone()).isFalse();

        // 응답 바이트가 모자라면 접수하지 않고 응답 권한을 소비하지 않는다.
        io.github.ppzxc.strand.api.ResponseAcceptance shortage =
                rig.result(held.get(0).respond(Status.OK, new byte[64]));
        assertThat(shortage.kind()).isEqualTo(AcceptanceKind.RESOURCE_EXHAUSTED);
        assertThat(shortage.resource()).hasValue(Resource.RESPONSE_BYTES);

        // 관찰·응답이 모두 포화인 채로 취소가 진행되어 결과가 보존되고, 이후 응답 재시도는 종료로 판정된다.
        handle.cancel();
        // 관찰 자리가 찼으므로 결과는 관찰이 아니라 비차단 조회로 읽는다.
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
