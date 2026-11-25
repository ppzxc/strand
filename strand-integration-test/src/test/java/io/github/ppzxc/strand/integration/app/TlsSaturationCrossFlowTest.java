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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TlsSaturationCrossFlowTest {
    private static final int PAYLOAD = 8 * 1024;
    private static final int SENT = 3000;

    private static final Resource[] TRACKED = {
        Resource.OUTGOING_REQUESTS,
        Resource.OUTGOING_BYTES,
        Resource.INCOMING_REQUESTS,
        Resource.INCOMING_BYTES,
        Resource.RESPONSE_BYTES,
        Resource.HANDLER_TASKS,
        Resource.OBSERVATIONS,
        Resource.FRAME_ASSEMBLY_BYTES,
        Resource.CONTROL_OUTPUT_BYTES,
        Resource.RESULT_EVENT_BYTES,
        Resource.TLS_PLAINTEXT_BYTES,
        Resource.TLS_CIPHERTEXT_BYTES,
        Resource.TLS_CONTROL_BYTES,
        Resource.TOTAL_CONNECTIONS
    };

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    static java.util.stream.Stream<RigKind> kinds() {
        return RigKind.tls();
    }

    private static Request request(byte[] payload) {
        return Request.of(TYPE, payload);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kinds")
    void ciphertextSaturationKeepsCancelResultAndCloseProgressingAndKeepsLimits(RigKind kind) throws Exception {
        Rig rig = kind.open(new Spec().perConnection(SENT + 8).owner(SENT + 8).bodyBytes(1L << 30, 1L << 31));
        closeables.add(rig);
        List<App.In> held = new CopyOnWriteArrayList<>();
        App.Server server = rig.server(new Rig.ServerSetup().handler(held::add));
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        PausableRelay relay = new PausableRelay(listener.address());
        closeables.add(relay);
        App.Client client = rig.client();
        App.Conn connection = Rig.get(client.connect(relay.address()));
        closeables.add(connection::abort);

        RequestHandle answeredHandle =
                connection.request(request("first".getBytes(StandardCharsets.UTF_8)), RequestOptions.noDeadline());
        awaitTrue(() -> held.size() == 1);
        assertThat(rig.result(held.get(0).respond(Status.OK, "answer".getBytes(StandardCharsets.UTF_8)))
                        .kind())
                .isEqualTo(AcceptanceKind.ACCEPTED);
        RequestResult answered = rig.result(answeredHandle.result());
        assertThat(answered.status()).isEqualTo(Status.OK);

        relay.pauseClientToServer();
        List<RequestHandle> sent = new ArrayList<>();
        byte[] payload = new byte[PAYLOAD];
        for (int i = 0; i < SENT; i++) {
            sent.add(connection.request(request(payload), RequestOptions.noDeadline()));
        }
        awaitStable(held);
        int delivered = held.size();
        assertThat(delivered).as("멈춘 상대에게 모든 요청이 닿지 못해야 한다").isLessThan(SENT + 1);

        RequestHandle stuck = sent.get(SENT - 1);
        assertThat(stuck.result().isDone()).isFalse();
        stuck.cancel();
        awaitTrue(() -> stuck.result().isDone());
        RequestResult cancelled = stuck.result().resultNow();
        assertThat(cancelled.status()).isEqualTo(Status.CANCELLED);

        assertThat(answeredHandle.result().resultNow()).isSameAs(answered);

        connection.abort();
        awaitTrue(() -> connection.closed().isDone());
        for (RequestHandle handle : sent) {
            awaitTrue(() -> handle.result().isDone());
        }
        assertThat(stuck.result().resultNow()).isSameAs(cancelled);
        assertThat(answeredHandle.result().resultNow()).isSameAs(answered);
        assertThat(sent.get(SENT - 2).result().resultNow().certainty())
                .isIn(Certainty.UNKNOWN, Certainty.NOT_PROCESSED);

        for (Resource resource : TRACKED) {
            assertThat(client.usage(resource).highWater())
                    .as("client %s", resource)
                    .isLessThanOrEqualTo(client.usage(resource).limit());
            assertThat(server.usage(resource).highWater())
                    .as("server %s", resource)
                    .isLessThanOrEqualTo(server.usage(resource).limit());
        }
        assertThat(client.usage(Resource.TLS_CIPHERTEXT_BYTES).highWater()).isPositive();

        relay.close();
        for (Resource resource : TRACKED) {
            awaitTrue(() -> client.usage(resource).current() == 0);
            awaitTrue(() -> server.usage(resource).current() == 0);
        }
    }

    private static void awaitStable(List<App.In> held) throws Exception {
        int last = -1;
        int stableRounds = 0;
        long limit = System.nanoTime() + 20_000_000_000L;
        while (stableRounds < 30) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("전달 수가 안정되지 않았다");
            }
            Thread.sleep(20);
            int now = held.size();
            stableRounds = now == last ? stableRounds + 1 : 0;
            last = now;
        }
    }
}
