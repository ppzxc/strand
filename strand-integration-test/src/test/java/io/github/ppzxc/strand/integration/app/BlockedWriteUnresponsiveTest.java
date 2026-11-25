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

class BlockedWriteUnresponsiveTest {

    private static final long UNRESPONSIVE = 6;

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

        Spec spec = frameSized(new Spec().perConnection(40).owner(40).bodyBytes(1L << 27, 1L << 28));
        run(kind, spec, new Rig.ClientSetup(), 40);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void aConnectionThatStoppedReadingAtTheWriteQueueLimitEndsAsUnresponsive(RigKind kind) throws Exception {

        Spec spec = frameSized(new Spec().perConnection(100).owner(100).bodyBytes(1L << 27, 1L << 28));
        run(kind, spec, new Rig.ClientSetup().writeQueueBytes(16384), 100);
    }

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

        assertThat(notice.sender()).contains(CloseNotice.Sender.LOCAL);
        for (RequestHandle handle : handles) {
            RequestResult result = rig.result(handle.result());
            assertThat(result.certainty()).isNotEqualTo(Certainty.RESPONSE);
        }
    }
}
