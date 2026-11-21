package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 읽지 않는 상대에게 쓰기가 막힌 연결이 확인 대기 시간 안에 {@code UNRESPONSIVE}로 끝난다(#375). 막힌 연결은 유휴 PING을 전송 계층에 넘기지 못하므로
 * 넘긴 PING의 확인 대기만으로는 끝나지 않는다. 시간 간격을 추측하지 않고 끝남을 조건으로 기다린다.
 */
class BlockedWriteUnresponsiveTest {
    /** 규격 5.2절의 {@code UNRESPONSIVE} 오류 코드. */
    private static final long UNRESPONSIVE = 6;

    /** 프레임 하나가 상한(1 MiB)에 가까운 요청. 커널이 삼키는 양(수 MiB)보다 많이 쌓이게 한다. */
    private static final int PAYLOAD_BYTES = 1_000_000;

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
    void aConnectionBlockedWritingToAPeerThatStoppedReadingEndsAsUnresponsive(RigKind kind) throws Exception {
        // 쓰기 대기열 한도(기본 절대 한도 64 MiB)보다 적게 쌓는다. 읽기는 멈추지 않고 쓰기만 막힌다.
        Spec spec = frameSized(new Spec().perConnection(40).owner(40).bodyBytes(1L << 27, 1L << 28));
        run(kind, spec, new Rig.ClientSetup(), 40);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void aConnectionThatStoppedReadingAtTheWriteQueueLimitEndsAsUnresponsive(RigKind kind) throws Exception {
        // 한도의 하한은 상대가 알린 최대 프레임 하나(1 MiB)이고, 막힌 쓰기에 그보다 많이 쌓여 읽기를 멈춘다.
        Spec spec = frameSized(new Spec().perConnection(100).owner(100).bodyBytes(1L << 27, 1L << 28));
        run(kind, spec, new Rig.ClientSetup().writeQueueBytes(16384), 100);
    }

    /** 최대 프레임 크기를 상한(1 MiB)으로 올려 {@link #PAYLOAD_BYTES} 요청이 로컬에서 거절되지 않게 한다. */
    private static Spec frameSized(Spec spec) {
        spec.maxFrameSize = 1_048_576;
        return spec;
    }

    private void run(RigKind kind, Spec spec, Rig.ClientSetup clientSetup, int requests) throws Exception {
        Rig rig = kind.open(spec);
        closeables.add(rig);
        App.Server server = rig.server();
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        PausableRelay relay = new PausableRelay(listener.address());
        closeables.add(relay);
        App.Client client = rig.client(clientSetup.liveness(Duration.ofMillis(300), Duration.ofMillis(1000)));
        App.Conn connection = Rig.get(client.connect(relay.address()));
        closeables.add(connection::abort);

        relay.pauseClientToServer();
        List<RequestHandle> handles = new ArrayList<>();
        byte[] payload = new byte[PAYLOAD_BYTES];
        for (int i = 0; i < requests; i++) {
            handles.add(connection.request(Request.of(TYPE, payload), RequestOptions.noDeadline()));
        }

        awaitTrue(() -> connection.closed().isDone());
        CloseNotice notice = rig.result(connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE);
        // 상대가 보낸 CLOSE가 아니라 이 연결이 스스로 무응답을 판단해 끝냈다.
        assertThat(notice.sender()).contains(CloseNotice.Sender.LOCAL);
        for (RequestHandle handle : handles) {
            RequestResult result = rig.result(handle.result());
            assertThat(result.certainty()).isNotEqualTo(Certainty.RESPONSE);
        }
    }
}
