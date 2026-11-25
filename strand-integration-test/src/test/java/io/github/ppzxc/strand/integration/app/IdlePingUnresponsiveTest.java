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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class IdlePingUnresponsiveTest {

    private static final long UNRESPONSIVE = 6;

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
    void anIdleConnectionToAPeerThatStoppedReadingEndsAsUnresponsive(RigKind kind) throws Exception {
        Rig rig = kind.open();
        closeables.add(rig);
        List<App.In> held = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup().handler(held::add));
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        PausableRelay relay = new PausableRelay(listener.address());
        closeables.add(relay);
        App.Client client = rig.client(new Rig.ClientSetup().liveness(Duration.ofMillis(20), Duration.ofMillis(200)));
        App.Conn connection = Rig.get(client.connect(relay.address()));
        closeables.add(connection::abort);
        RequestHandle pending = connection.request(
                Request.of(TYPE, "idle".getBytes(StandardCharsets.UTF_8)), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 1);

        relay.pauseClientToServer();

        awaitTrue(() -> connection.closed().isDone());
        CloseNotice notice = rig.result(connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE);
        RequestResult result = rig.result(pending.result());
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
    }
}
