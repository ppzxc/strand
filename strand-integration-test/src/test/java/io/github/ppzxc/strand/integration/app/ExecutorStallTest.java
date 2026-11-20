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

/**
 * 앱 실행기가 작업을 받기만 하고 실행하지 않는 동안 실제 TCP/TLS 요청의 취소·종료가 진행되고 원래 결과와 다른 변환 결과 조회가 유지되는지 본다(#328). 개별 시험(#318 #324
 * #327)은 메모리 통로나 한 가지 정체만 본다.
 */
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

    /** 연결과 서버 쪽 핸들러 응답 없음을 만든다. 연결이 준비된 뒤에 앱 실행기를 정체시킨다. */
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
            // 정체 전에 서버 핸들러가 시작할 수 있음을 한 번 확인해 둔다: 응답하지 않는 요청을 낸다.
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
        // 기본 관찰은 정체한 앱 실행기로 간다: 등록은 자리를 차지하고 실행은 멈춘다.
        AtomicBoolean observed = new AtomicBoolean();
        Observation stuck = first.result().observe(result -> observed.set(true));
        // 다른 실행기에 붙인 관찰은 정체와 무관하게 돈다.
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
        // 다른 요청의 결과 조회는 영향을 받지 않는다: 아직 확정 전이며 조회가 예외 없이 완료 여부를 알린다.
        assertThat(second.result().isDone()).isFalse();

        connection.abort();
        awaitTrue(() -> connection.closed().isDone());
        // 요청 결과는 종료 사건보다 먼저 보존되고 조회로 읽힌다.
        assertThat(second.result().isDone()).isTrue();
        RequestResult disconnected = second.result().resultNow();
        assertThat(disconnected.certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThat(first.result().resultNow()).isSameAs(cancelled);
        assertThat(connection.closed().resultNow()).isNotNull();

        // 정체가 풀리면 밀린 관찰이 돌고 자리가 돌아오며 결과는 그대로다.
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

        // 변환 Future 취소는 실제 요청 취소가 아니고 다른 변환도 건드리지 않는다.
        assertThat(cancelledConversion.cancel(false)).isTrue();
        assertThat(cancelledConversion.isCancelled()).isTrue();
        assertThat(firstConversion.isDone()).isFalse();
        assertThat(first.result().isDone()).isFalse();
        assertThat(secondConversion.isDone()).isFalse();

        // 실제 요청 취소는 핸들로 한다. 앱 실행기가 멈춰 있어도 변환은 값(취소 결과)으로 완료한다.
        first.cancel();
        RequestResult converted = firstConversion.get(Rig.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(converted.status()).isEqualTo(Status.CANCELLED);
        assertThat(converted).isSameAs(first.result().resultNow());
        assertThat(cancelledConversion.isCancelled()).isTrue();
        // 다른 변환의 결과 조회는 유지된다.
        assertThat(secondConversion.isDone()).isFalse();
        assertThat(second.result().isDone()).isFalse();

        // 연결 끊김도 변환 실패가 아니라 값이다. 결과는 종료 사건보다 먼저 확정되어 있다.
        connection.abort();
        RequestResult disconnected = secondConversion.get(Rig.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(disconnected.certainty()).isEqualTo(Certainty.UNKNOWN);
        awaitTrue(() -> connection.closed().isDone());
        assertThat(second.result().resultNow()).isSameAs(disconnected);
        assertThat(cancelledConversion.isCancelled()).isTrue();
    }
}
