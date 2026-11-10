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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        private final List<ConnectionClosed> allClosed = new ArrayList<>();

        synchronized List<ConnectionClosed> closedNotices() {
            return new ArrayList<>(allClosed);
        }

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
        start(ConnectionDriver.client(wire.config(), link.first())
                .executor(handlers)
                .listener(recorder));

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
        start(ConnectionDriver.client(EndpointConfig.defaults(), passage)
                .executor(handlers)
                .listener(recorder));
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
    void closingTheConnectionLetsPendingStreamsFinishThenBothSidesAreToldItWasAClose(ConnectionDriverTest.Wire wire)
            throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        Recorder clientSide = new Recorder();
        Recorder serverSide = new Recorder();
        CompletableFuture<IncomingRequest> received = new CompletableFuture<>();
        start(ConnectionDriver.server(wire.config(), link.second())
                .handler(handlers, received::complete)
                .listener(serverSide));
        ConnectionDriver client = start(ConnectionDriver.client(wire.config(), link.first())
                .executor(handlers)
                .listener(clientSide));
        CompletableFuture<RequestResult> pending =
                client.request(APPLICATION_TYPE, bytes("slow")).result();
        IncomingRequest request = await(received);

        client.closeConnection();
        assertThat(pending).isNotDone();
        request.respond(0, bytes("late")).join();

        RequestResult result = await(pending);
        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(result.body(), java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo("late");
        CloseReason local = await(clientSide.closed).reason();
        assertThat(local.kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(local.sender()).hasValue(CloseReason.Sender.LOCAL);
        CloseReason remote = await(serverSide.closed).reason();
        assertThat(remote.kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(remote.sender()).hasValue(CloseReason.Sender.PEER);
    }

    private static final int STATUS_UNAVAILABLE = 14;
    private static final int STATUS_CANCELLED = 1;
    private static final int UNAUTHENTICATED = 7;

    /** 서버가 요청을 받고도 응답하지 않는 연결 한 쌍을 맺는다. 클라이언트가 낸 요청 하나가 서버에 닿은 뒤에 돌아온다. */
    private final class Held {
        final InMemoryPassage.Pair link = InMemoryPassage.streams();
        final Recorder clientSide = new Recorder();
        final Recorder serverSide = new Recorder();
        final ProbePassage clientPassage = new ProbePassage(link.first());
        final ProbePassage serverPassage = new ProbePassage(link.second());
        final ConnectionDriver client;
        final RequestHandle pendingHandle;
        final CompletableFuture<RequestResult> pending;

        /** 양쪽 전송 스레드가 끝나고 알림이 모두 돌 때까지 기다린다. */
        void awaitEnded() throws InterruptedException {
            awaitLatch(clientPassage.closed);
            awaitLatch(serverPassage.closed);
            drainNotifications();
        }

        Held() throws Exception {
            CompletableFuture<IncomingRequest> received = new CompletableFuture<>();
            start(ConnectionDriver.server(EndpointConfig.defaults(), serverPassage)
                    .handler(handlers, received::complete)
                    .listener(serverSide));
            client = start(ConnectionDriver.client(EndpointConfig.defaults(), clientPassage)
                    .executor(handlers)
                    .listener(clientSide));
            pendingHandle = client.request(APPLICATION_TYPE, bytes("held"));
            pending = pendingHandle.result();
            await(received);
        }
    }

    @Test
    void abortingTheWaitEndsPendingRequestsWithTheCoreResultAndNotifiesOnce() throws Exception {
        Held held = new Held();
        held.client.closeConnection();

        held.client.abortClosing();

        RequestResult result = await(held.pending);
        assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
        ConnectionClosed closed = await(held.clientSide.closed);
        assertThat(closed.reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(closed.reason().sender()).hasValue(CloseReason.Sender.LOCAL);
        held.awaitEnded();
        assertThat(held.clientSide.closedNotices()).hasSize(1);
        assertThat(held.serverSide.closedNotices()).hasSize(1);
    }

    @Test
    void disconnectingAtOnceEndsPendingRequestsWithTheCoreResultAndNotifiesOnceWithoutAnIoException() throws Exception {
        Held held = new Held();

        held.client.close();

        RequestResult result = await(held.pending);
        assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
        ConnectionClosed closed = await(held.clientSide.closed);
        assertThat(closed.reason().kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
        assertThat(closed.ioException()).isEmpty();
        held.awaitEnded();
        assertThat(held.clientSide.closedNotices()).hasSize(1);
    }

    @Test
    void aRequestIssuedWhileClosingEndsWithTheCoreUnavailableResultNotAnException() throws Exception {
        Held held = new Held();
        held.client.closeConnection();

        RequestResult result =
                await(held.client.request(APPLICATION_TYPE, bytes("too-late")).result());

        assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
        assertThat(result.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(held.pending).isNotDone();
    }

    @Test
    void cancellingWhileClosingEndsWithTheCoreCancelledResult() throws Exception {
        Held held = new Held();
        held.client.closeConnection();
        RequestHandle late = held.client.request(APPLICATION_TYPE, bytes("too-late"));
        await(late.result());

        held.pendingHandle.cancel();

        RequestResult result = await(held.pending);
        assertThat(result.status()).isEqualTo(STATUS_CANCELLED);
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void whenThePeerDropsTheConnectionTheNoticeCarriesTheEndOfStreamException() throws Exception {
        Held held = new Held();

        held.link.second().close();

        ConnectionClosed closed = await(held.clientSide.closed);
        assertThat(closed.reason().kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
        assertThat(closed.ioException()).get().isInstanceOf(java.io.EOFException.class);
        assertThat(await(held.pending).certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void aReadFailureIsCarriedInTheNotice() throws Exception {
        IOException failure = new IOException("읽기 실패");
        Recorder recorder = new Recorder();
        Passage failing = new Passage() {
            @Override
            public Inbound await(long timeoutMs) throws IOException {
                throw failure;
            }

            @Override
            public void write(OutboundMessage message) {}

            @Override
            public void wakeup() {}

            @Override
            public void close() {}
        };
        start(ConnectionDriver.client(EndpointConfig.defaults(), failing)
                .executor(handlers)
                .listener(recorder));

        ConnectionClosed closed = await(recorder.closed);

        assertThat(closed.ioException()).containsSame(failure);
        assertThat(closed.reason().kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
    }

    @Test
    void aWriteFailureIsCarriedInTheNotice() throws Exception {
        IOException failure = new IOException("쓰기 실패");
        Recorder recorder = new Recorder();
        Passage failing = new Passage() {
            private final LinkedBlockingQueue<Boolean> wake = new LinkedBlockingQueue<>();

            @Override
            public Inbound await(long timeoutMs) throws IOException {
                try {
                    wake.take();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
                return Inbound.none();
            }

            @Override
            public void write(OutboundMessage message) throws IOException {
                throw failure;
            }

            @Override
            public void wakeup() {
                wake.add(true);
            }

            @Override
            public void close() {}
        };
        start(ConnectionDriver.client(EndpointConfig.defaults(), failing)
                .executor(handlers)
                .listener(recorder));

        assertThat(await(recorder.closed).ioException()).containsSame(failure);
    }

    @Test
    void anAuthenticatorFailureIsAnErrorCloseReportedOnBothSides() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        Recorder clientSide = new Recorder();
        Recorder serverSide = new Recorder();
        IllegalStateException authenticatorFailure = new IllegalStateException("인증 서버 장애");
        start(ConnectionDriver.server(
                        EndpointConfig.defaults().authenticator((mechanism, credentials) -> {
                            throw authenticatorFailure;
                        }),
                        link.second())
                .executor(handlers)
                .listener(serverSide));
        start(ConnectionDriver.client(EndpointConfig.defaults().auth("X", new byte[0]), link.first())
                .executor(handlers)
                .listener(clientSide));

        ConnectionClosed server = await(serverSide.closed);
        ConnectionClosed client = await(clientSide.closed);

        assertThat(server.reason().kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(server.reason().sender()).hasValue(CloseReason.Sender.LOCAL);
        assertThat(server.authenticatorFailure()).containsSame(authenticatorFailure);
        assertThat(client.reason().kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(client.reason().errorCode())
                .isEqualTo(server.reason().errorCode())
                .isNotZero();
        assertThat(client.reason().sender()).hasValue(CloseReason.Sender.PEER);
    }

    @Test
    void aRejectedAuthIsAnErrorCloseWithUnauthenticated() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        Recorder clientSide = new Recorder();
        start(ConnectionDriver.server(EndpointConfig.defaults(), link.second()).executor(handlers));
        start(ConnectionDriver.client(EndpointConfig.defaults().auth("NOPE", new byte[0]), link.first())
                .executor(handlers)
                .listener(clientSide));

        CloseReason reason = await(clientSide.closed).reason();

        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(UNAUTHENTICATED);
        assertThat(reason.sender()).hasValue(CloseReason.Sender.PEER);
    }

    @Test
    void theNoticeStillGoesOutOnceWhenTheTransportThreadDiesFromAnUnexpectedError() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        Recorder serverSide = new Recorder();
        AtomicInteger tasks = new AtomicInteger();
        // 첫 작업(받은 요청 핸들러)만 거부해 전송 스레드를 오류로 멈추게 하고, 알림은 받는다.
        Executor flaky = runnable -> {
            if (tasks.getAndIncrement() == 0) {
                throw new RejectedExecutionException("가득 참");
            }
            handlers.execute(runnable);
        };
        ConnectionDriver server = start(ConnectionDriver.server(EndpointConfig.defaults(), link.second())
                .handler(flaky, request -> {})
                .listener(serverSide));
        ConnectionDriver client = start(
                ConnectionDriver.client(EndpointConfig.defaults(), link.first()).executor(handlers));
        CompletableFuture<RequestResult> own =
                server.request(APPLICATION_TYPE, bytes("server-own")).result();
        client.request(APPLICATION_TYPE, bytes("trigger"));

        assertThat(await(own).status()).isEqualTo(STATUS_UNAVAILABLE);
        assertThat(await(serverSide.closed).reason().kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
        server.close();
        drainNotifications();
        assertThat(serverSide.closedNotices()).hasSize(1);
    }

    /**
     * 통로를 감싸 드라이버가 부른 것을 관찰하고 고장을 주입한다. {@code close}는 전송 스레드가 끝날 때 한 번 불리므로, {@link #closed}로 전송
     * 스레드가 끝났음을 결정적으로 기다릴 수 있다.
     */
    private static final class ProbePassage implements Passage {
        private final Passage delegate;
        final CountDownLatch shutdownCalled = new CountDownLatch(1);
        final CountDownLatch readResumedAfterShutdown = new CountDownLatch(1);
        final CountDownLatch closed = new CountDownLatch(1);
        volatile boolean failWrites;
        volatile boolean holdShutdown;
        volatile boolean failReadsAfterShutdown;
        private volatile boolean shutdownDone;

        ProbePassage(Passage delegate) {
            this.delegate = delegate;
        }

        @Override
        public Inbound await(long timeoutMs) throws IOException {
            if (shutdownDone) {
                readResumedAfterShutdown.countDown();
                if (failReadsAfterShutdown) {
                    throw new IOException("CLOSED 뒤 읽기 실패");
                }
            }
            return delegate.await(timeoutMs);
        }

        @Override
        public void write(OutboundMessage message) throws IOException {
            if (failWrites) {
                throw new IOException("쓰기 실패");
            }
            delegate.write(message);
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void shutdownOutput() {
            shutdownDone = true;
            shutdownCalled.countDown();
            if (!holdShutdown) {
                delegate.shutdownOutput();
            }
        }

        /** 붙들어 둔 {@link #shutdownOutput}을 이제 통로에 전한다(상대가 이제 끊는다). */
        void releaseShutdown() {
            delegate.shutdownOutput();
        }

        @Override
        public void close() {
            closed.countDown();
            delegate.close();
        }
    }

    /** 이미 내보낸 알림을 실행기가 다 돌릴 때까지 기다린다. 전송 스레드가 끝난 뒤에 불러야 더 들어오는 알림이 없다. */
    private void drainNotifications() throws InterruptedException {
        handlers.shutdown();
        assertThat(handlers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private static void awaitBothEnded(Exchanged link) throws InterruptedException {
        awaitLatch(link.client.closed);
        awaitLatch(link.server.closed);
    }

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    /** CLOSE를 교환한 한 쌍. 클라이언트 쪽 통로만 관찰하고, 서버 쪽은 상대 역할로 {@code serverPassage}를 감싼다. */
    private final class Exchanged {
        final ProbePassage client;
        final ProbePassage server;
        final Recorder clientSide = new Recorder();
        final Recorder serverSide = new Recorder();
        final ConnectionDriver clientDriver;

        Exchanged() {
            InMemoryPassage.Pair link = InMemoryPassage.streams();
            client = new ProbePassage(link.first());
            server = new ProbePassage(link.second());
            start(ConnectionDriver.server(EndpointConfig.defaults(), server)
                    .executor(handlers)
                    .listener(serverSide));
            clientDriver = start(ConnectionDriver.client(EndpointConfig.defaults(), client)
                    .executor(handlers)
                    .listener(clientSide));
        }
    }

    @Test
    void afterTheCloseExchangeBothSidesCloseOnlyTheirOutputAndEndOnceThePeerHangsUp() throws Exception {
        Exchanged link = new Exchanged();
        await(link.clientSide.authenticated);

        link.clientDriver.closeConnection();

        awaitLatch(link.client.shutdownCalled);
        awaitLatch(link.server.shutdownCalled);
        awaitLatch(link.client.closed);
        awaitLatch(link.server.closed);
        awaitBothEnded(link);
        drainNotifications();
        assertThat(link.clientSide.closedNotices()).hasSize(1);
        assertThat(link.serverSide.closedNotices()).hasSize(1);
        assertThat(link.clientSide.closedNotices().get(0).ioException()).isEmpty();
        assertThat(link.serverSide.closedNotices().get(0).ioException()).isEmpty();
        assertThat(link.clientSide.closedNotices().get(0).reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
    }

    @Test
    void afterClosedTheDriverKeepsReadingUntilThePeerHangsUp() throws Exception {
        Exchanged link = new Exchanged();
        await(link.clientSide.authenticated);
        link.server.holdShutdown = true; // 서버는 보내는 쪽을 닫지 않는다. 클라이언트는 끊김을 받지 못한다.

        link.clientDriver.closeConnection();

        awaitLatch(link.client.shutdownCalled);
        awaitLatch(link.client.readResumedAfterShutdown);
        assertThat(link.client.closed.getCount()).isEqualTo(1);

        link.server.releaseShutdown();

        awaitLatch(link.client.closed);
        awaitBothEnded(link);
        drainNotifications();
        assertThat(link.clientSide.closedNotices()).hasSize(1);
        assertThat(link.clientSide.closedNotices().get(0).ioException()).isEmpty();
    }

    @Test
    void aReadFailureAfterClosedIsNotANewNotice() throws Exception {
        Exchanged link = new Exchanged();
        await(link.clientSide.authenticated);
        link.server.holdShutdown = true;
        link.client.failReadsAfterShutdown = true;

        link.clientDriver.closeConnection();

        awaitLatch(link.client.closed);
        awaitBothEnded(link);
        drainNotifications();
        assertThat(link.clientSide.closedNotices()).hasSize(1);
        assertThat(link.clientSide.closedNotices().get(0).ioException()).isEmpty();
        assertThat(link.clientSide.closedNotices().get(0).reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
    }

    @Test
    void aWriteFailureRightAfterClosedIsNeitherInTheNoticeNorANewOne() throws Exception {
        Exchanged link = new Exchanged();
        await(link.clientSide.authenticated);
        // 서버는 CLOSE를 받아 답하며 CLOSED를 내는 같은 차례에 쓰기가 실패한다. CLOSED 알림이 먼저 나간다.
        link.server.failWrites = true;

        link.clientDriver.closeConnection();

        awaitLatch(link.server.closed);
        awaitBothEnded(link);
        drainNotifications();
        assertThat(link.serverSide.closedNotices()).hasSize(1);
        assertThat(link.serverSide.closedNotices().get(0).ioException()).isEmpty();
        assertThat(link.serverSide.closedNotices().get(0).reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
    }

    @Test
    void theNoticeIsSentOnceWhenThePeerHangsUpWithoutClose() throws Exception {
        Exchanged link = new Exchanged();
        await(link.clientSide.authenticated);

        link.server.close();

        awaitLatch(link.client.closed);
        awaitBothEnded(link);
        drainNotifications();
        assertThat(link.clientSide.closedNotices()).hasSize(1);
        assertThat(link.clientSide.closedNotices().get(0).ioException()).get().isInstanceOf(java.io.EOFException.class);
    }

    @Test
    void theNoticeIsSentOnceWhenAReadFailureRacesTheClose() throws Exception {
        Exchanged link = new Exchanged();
        await(link.clientSide.authenticated);
        link.client.failReadsAfterShutdown = true;
        link.server.holdShutdown = true;

        // 서버가 끊김(EOF)과 CLOSE를 거의 동시에 보내도(상대 close와 내 closeConnection이 겹침) 알림은 한 번이다.
        link.clientDriver.closeConnection();
        link.server.releaseShutdown();

        awaitLatch(link.client.closed);
        awaitBothEnded(link);
        drainNotifications();
        assertThat(link.clientSide.closedNotices()).hasSize(1);
    }
}
