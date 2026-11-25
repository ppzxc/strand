package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.TYPE;
import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class CredentialCloseTest {
    private static final long EXCESSIVE_USE = 5;
    private static final long UNAUTHENTICATED = 7;

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

    private final class Pair {
        final Rig rig;
        final App.Conn client;
        final App.Conn server;
        final List<App.In> served = new CopyOnWriteArrayList<>();

        Pair(RigKind kind) throws Exception {
            rig = kind.open();
            closeables.add(rig);
            AtomicReference<App.Conn> accepted = new AtomicReference<>();
            App.Server s = rig.server(new Rig.ServerSetup().factory(connection -> {
                accepted.set(connection);
                return new App.Prepared(served::add, () -> {});
            }));
            App.Lsn listener = Rig.get(s.listen());
            closeables.add(listener::close);
            client = Rig.get(rig.client().connect(listener.address()));
            closeables.add(client::abort);
            awaitTrue(() -> accepted.get() != null);
            server = accepted.get();
        }

        CloseNotice clientNotice() throws Exception {
            return rig.result(client.closed());
        }

        CloseNotice serverNotice() throws Exception {
            return rig.result(server.closed());
        }

        void sendPending() throws Exception {

            while (served.isEmpty()) {
                RequestHandle handle = client.request(
                        Request.of(TYPE, "p".getBytes(StandardCharsets.UTF_8)), RequestOptions.noDeadline());
                awaitTrue(() -> !served.isEmpty() || handle.result().isDone());
            }
            assertThat(served).hasSize(1);
        }
    }

    private void expectErrorClose(RigKind kind, Consumer<App.Conn> action, long errorCode) throws Exception {
        Pair pair = new Pair(kind);
        action.accept(pair.server);
        CloseNotice peer = pair.clientNotice();
        assertThat(peer.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(peer.errorCode()).isEqualTo(errorCode);
        assertThat(peer.sender()).contains(CloseNotice.Sender.PEER);
        CloseNotice local = pair.serverNotice();
        assertThat(local.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(local.errorCode()).isEqualTo(errorCode);
        assertThat(local.sender()).contains(CloseNotice.Sender.LOCAL);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void expiredCredentialClosesWithNoErrorSeenAsPeerClose(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        pair.server.expireCredential();
        CloseNotice peer = pair.clientNotice();
        assertThat(peer.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(peer.sender()).contains(CloseNotice.Sender.PEER);
        CloseNotice local = pair.serverNotice();
        assertThat(local.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(local.sender()).contains(CloseNotice.Sender.LOCAL);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void revokedCredentialClosesWithUnauthenticated(RigKind kind) throws Exception {
        expectErrorClose(kind, App.Conn::revokeCredential, UNAUTHENTICATED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void excessiveUseClosesWithExcessiveUse(RigKind kind) throws Exception {
        expectErrorClose(kind, App.Conn::closeExcessiveUse, EXCESSIVE_USE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void expiryClosesOnlyAfterPendingStreamsEnd(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        pair.sendPending();
        pair.server.expireCredential();

        assertThat(pair.client.closed().isDone()).isFalse();
        assertThat(pair.server.closed().isDone()).isFalse();
        pair.served.get(0).respond(0, new byte[0]);
        assertThat(pair.clientNotice().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void revocationClosesAtOnceEvenWithPendingStream(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        pair.sendPending();
        pair.server.revokeCredential();
        assertThat(pair.clientNotice().errorCode()).isEqualTo(UNAUTHENTICATED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void excessiveUseClosesAtOnceEvenWithPendingStream(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        pair.sendPending();
        pair.server.closeExcessiveUse();
        assertThat(pair.clientNotice().errorCode()).isEqualTo(EXCESSIVE_USE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void actionsAfterTheConnectionEndedDoNothing(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        pair.server.revokeCredential();
        pair.clientNotice();
        pair.serverNotice();
        pair.server.expireCredential();
        pair.server.revokeCredential();
        pair.server.closeExcessiveUse();
        assertThat(pair.serverNotice().errorCode()).isEqualTo(UNAUTHENTICATED);
        assertThat(pair.clientNotice().errorCode()).isEqualTo(UNAUTHENTICATED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void actionsAfterSendingCloseKeepTheFirstReason(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        pair.server.close();
        pair.server.revokeCredential();
        pair.server.closeExcessiveUse();
        assertThat(pair.clientNotice().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(pair.serverNotice().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void actionsAfterReceivingCloseDoNothing(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        pair.client.close();
        assertThat(pair.serverNotice().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        pair.server.revokeCredential();
        pair.server.closeExcessiveUse();
        assertThat(pair.clientNotice().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void clientConnectionRejectsExpiryAndRevocation(RigKind kind) throws Exception {
        Pair pair = new Pair(kind);
        assertThatThrownBy(pair.client::expireCredential).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(pair.client::revokeCredential).isInstanceOf(IllegalStateException.class);

        pair.client.closeExcessiveUse();
        assertThat(pair.serverNotice().errorCode()).isEqualTo(EXCESSIVE_USE);
    }
}
