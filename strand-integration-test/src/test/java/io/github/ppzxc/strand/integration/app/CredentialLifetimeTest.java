package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** 서버가 연결 인증 통과에 실은 {@code credential_lifetime}을 클라이언트가 연결에서 읽는다(규격 3.3.1절, #366). */
class CredentialLifetimeTest {
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

    private Rig lastRig;

    private App.Conn connect(Rig rig, Rig.ServerSetup setup) throws Exception {
        lastRig = rig;
        closeables.add(rig);
        App.Server server = rig.server(setup.handler(in -> in.respond(Status.OK, in.payload())));
        App.Client client = rig.client();
        App.Lsn listener = Rig.get(server.listen());
        return Rig.get(client.connect(listener.address()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void clientReadsTheLifetimeTheServerAnnounced(RigKind kind) throws Exception {
        App.Conn connection = connect(kind.open(), new Rig.ServerSetup().credentialLifetime(300));

        assertThat(connection.credentialLifetime()).hasValue(300);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void clientReadsTheMaximumUnsignedLifetime(RigKind kind) throws Exception {
        App.Conn connection = connect(kind.open(), new Rig.ServerSetup().credentialLifetime(0xFFFF_FFFFL));

        assertThat(connection.credentialLifetime()).hasValue(0xFFFF_FFFFL);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void clientSeesNoLifetimeWhenTheServerAnnouncedNone(RigKind kind) throws Exception {
        App.Conn connection = connect(kind.open(), new Rig.ServerSetup());

        assertThat(connection.credentialLifetime()).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void libraryDoesNotCloseTheConnectionWhenTheLifetimeIsOver(RigKind kind) throws Exception {
        // 수명 0은 알린 순간 이미 지난 것이다. 시간을 기다리지 않고도 "지났는데 닫지 않음"을 본다.
        App.Conn connection = connect(kind.open(), new Rig.ServerSetup().credentialLifetime(0));

        assertThat(connection.credentialLifetime()).hasValue(0);
        RequestResult result = lastRig.result(connection
                .request(Request.of(TYPE, new byte[] {1}), RequestOptions.noDeadline())
                .result());
        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(connection.closed().isDone()).isFalse();
    }
}
