package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** 종료 세 가지와 알림(AUTH 관문 통과, 연결 종료)을 전송 모듈의 interface로만 관찰한다. */
class ConnectionNotificationTest {

    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private ConnectionDriver start(ConnectionDriver.Builder builder) {
        ConnectionDriver driver = builder.clock(() -> 0L).start();
        started.add(driver);
        return driver;
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    /** 알림을 모아 두고 Future로 기다리게 하는 리스너. */
    private static final class Recorder implements ConnectionListener {
        final CompletableFuture<OptionalLong> authenticated = new CompletableFuture<>();
        final CompletableFuture<ConnectionClosed> closed = new CompletableFuture<>();
        final List<ConnectionClosed> allClosed = new ArrayList<>();

        @Override
        public synchronized void onAuthenticated(OptionalLong credentialLifetime) {
            authenticated.complete(credentialLifetime);
        }

        @Override
        public synchronized void onClosed(ConnectionClosed event) {
            allClosed.add(event);
            closed.complete(event);
        }
    }

    @ParameterizedTest
    @EnumSource(ConnectionDriverTest.Wire.class)
    void clientIsToldItPassedTheAuthGate(ConnectionDriverTest.Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        Recorder recorder = new Recorder();
        start(ConnectionDriver.server(wire.config(), link.second()).executor(handlers));
        start(ConnectionDriver.client(wire.config(), link.first()).executor(handlers).listener(recorder));

        OptionalLong lifetime = await(recorder.authenticated);

        assertThat(lifetime).isEmpty();
    }

    /** 미리 정한 바이트를 읽게 하고 쓴 것은 버리는 통로. 상대 쪽 core가 낼 수 없는 입력(자격 증명 수명 등)을 먹일 때 쓴다. */
    private static final class ScriptedPassage implements Passage {
        private final LinkedBlockingQueue<Inbound> inbox = new LinkedBlockingQueue<>();

        void feed(String hex) {
            String clean = hex.replaceAll("\\s", "");
            byte[] out = new byte[clean.length() / 2];
            for (int i = 0; i < out.length; i++) {
                out[i] = (byte) Integer.parseInt(clean.substring(2 * i, 2 * i + 2), 16);
            }
            inbox.add(Inbound.bytes(out));
        }

        @Override
        public Inbound await(long timeoutMs) throws IOException {
            try {
                return inbox.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        @Override
        public void write(OutboundMessage message) {}

        @Override
        public void wakeup() {
            inbox.add(Inbound.none());
        }

        @Override
        public void close() {}
    }

    @Test
    void clientIsToldTheCredentialLifetimeTheServerAnnounced() throws Exception {
        ScriptedPassage passage = new ScriptedPassage();
        Recorder recorder = new Recorder();
        start(ConnectionDriver.client(EndpointConfig.defaults(), passage).executor(handlers).listener(recorder));
        passage.feed("0000001000010000000000000000000000000000"); // 서버의 HELLO
        passage.feed("0000001000010100000000000000000000000000"); // 내 HELLO에 대한 ACK
        passage.feed("0000001c0005010000000000000000000000000000010000012c00ff00000001"); // 수명 300초인 AUTH(ACK)

        assertThat(await(recorder.authenticated)).hasValue(300);
    }

    private static final int APPLICATION_TYPE = 256;

    private static byte[] bytes(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @ParameterizedTest
    @EnumSource(ConnectionDriverTest.Wire.class)
    void closingTheConnectionLetsPendingStreamsFinishThenBothSidesAreToldItWasAClose(
            ConnectionDriverTest.Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        Recorder clientSide = new Recorder();
        Recorder serverSide = new Recorder();
        CompletableFuture<IncomingRequest> received = new CompletableFuture<>();
        start(ConnectionDriver.server(wire.config(), link.second())
                .handler(handlers, received::complete)
                .listener(serverSide));
        ConnectionDriver client =
                start(ConnectionDriver.client(wire.config(), link.first()).executor(handlers).listener(clientSide));
        CompletableFuture<RequestResult> pending =
                client.request(APPLICATION_TYPE, bytes("slow")).result();
        IncomingRequest request = await(received);

        client.closeConnection();
        assertThat(pending).isNotDone();
        request.respond(0, bytes("late")).join();

        RequestResult result = await(pending);
        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(result.body(), java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("late");
        CloseReason local = await(clientSide.closed).reason();
        assertThat(local.kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(local.sender()).hasValue(CloseReason.Sender.LOCAL);
        CloseReason remote = await(serverSide.closed).reason();
        assertThat(remote.kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(remote.sender()).hasValue(CloseReason.Sender.PEER);
    }
}
