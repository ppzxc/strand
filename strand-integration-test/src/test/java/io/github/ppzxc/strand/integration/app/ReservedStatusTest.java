package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** 예약 status(17~255)로 응답하려는 호출은 즉시 예외이고, 그 요청은 유효한 status로 다시 응답할 수 있다(규격 2.5.1절). */
class ReservedStatusTest {
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
    void respondRejectsReservedStatusImmediatelyAndKeepsTheRequestAnswerable(RigKind kind) throws Exception {
        Rig rig = kind.open();
        closeables.add(rig);
        List<App.In> served = new CopyOnWriteArrayList<>();
        AtomicReference<App.Conn> accepted = new AtomicReference<>();
        App.Server server = rig.server(new Rig.ServerSetup().factory(connection -> {
            accepted.set(connection);
            return new App.Prepared(served::add, () -> {});
        }));
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        App.Conn client = Rig.get(rig.client().connect(listener.address()));
        closeables.add(client::abort);
        awaitTrue(() -> accepted.get() != null);
        client.request(Request.of(TYPE, new byte[] {1}), RequestOptions.noDeadline());
        awaitTrue(() -> served.size() == 1);
        App.In request = served.get(0);

        for (int status : new int[] {17, 255, 256, -1}) {
            assertThatThrownBy(() -> request.respond(status, new byte[0])).isInstanceOf(IllegalArgumentException.class);
        }

        ResponseAcceptance acceptance = rig.result(request.respond(16, new byte[0]));
        assertThat(acceptance.kind()).isEqualTo(io.github.ppzxc.strand.api.AcceptanceKind.ACCEPTED);
    }
}
