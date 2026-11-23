package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.InMemoryPassage;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import io.github.ppzxc.strand.nio.internal.TestMessages;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 연결 엔진 하나를 수동 시계와 메모리 통로에 꽂고 상대를 코어 {@link Endpoint}로 시험대 스레드에서 직접 굴린다. 통로의 쓰기 한도로 쓰기 0·부분·전체를
 * 결정적으로 만들고, 문(gate)으로 전송 스레드를 멈춰 앱 호출이 core 등록 전 대기에 머무는 틈을 만든다.
 */
final class EngineRig implements AutoCloseable {
    static final int TYPE = AppFixture.TYPE;
    private static final long HOUR_MS = TimeUnit.HOURS.toMillis(1);

    /** 전송 스레드를 {@code await} 입구에서 멈출 수 있는 통로. */
    static final class GatedPassage implements Passage {
        private final Passage delegate;
        private volatile CountDownLatch gate;
        private volatile CountDownLatch entered = new CountDownLatch(0);

        GatedPassage(Passage delegate) {
            this.delegate = delegate;
        }

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
            CountDownLatch current = gate;
            if (current != null) {
                entered.countDown();
                try {
                    current.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            return delegate.await(timeoutMs, waitForRead, waitForWrite);
        }

        @Override
        public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
            return delegate.writeBytes(bytes, offset, length);
        }

        @Override
        public boolean pendingOutput() {
            return delegate.pendingOutput();
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void shutdownOutput() {
            delegate.shutdownOutput();
        }

        @Override
        public void close() {
            delegate.close();
        }

        /** 전송 스레드가 다음 {@code await} 입구에서 멈출 때까지 기다린다. */
        void closeGate() throws InterruptedException {
            CountDownLatch reached = new CountDownLatch(1);
            entered = reached;
            gate = new CountDownLatch(1);
            delegate.wakeup();
            if (!reached.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("전송 스레드가 문 앞에 서지 않았다");
            }
        }

        void openGate() {
            CountDownLatch current = gate;
            gate = null;
            if (current != null) {
                current.countDown();
            }
        }
    }

    /** 시험이 정하는 조정. 기본은 확인 대기 1시간, 유휴 PING 끔에 가까운 느슨한 값이다. */
    static final class Tuning {
        java.util.function.UnaryOperator<EndpointConfig> config = c -> c;
        java.util.function.UnaryOperator<Passage> passage = p -> p;
        java.util.function.UnaryOperator<EndpointConfig> peerConfig = c -> c;
        long closeFinishNanos = TimeUnit.SECONDS.toNanos(5);
        long writeQueueBytes = 64L << 20;
        long perConnection = 8;

        /** 연결별 한도 규모. 키우면 제어 출력 공간이 커져 쓰기 대기열 상한이 읽기를 먼저 멈춘다. */
        Tuning perConnection(long value) {
            this.perConnection = value;
            return this;
        }

        /** 두 끝의 엔드포인트 설정을 고친다. */
        Tuning config(java.util.function.UnaryOperator<EndpointConfig> value) {
            this.config = value;
            return this;
        }

        /** 엔진이 보는 통로를 감싼다(문 통로 안쪽). */
        Tuning passage(java.util.function.UnaryOperator<Passage> value) {
            this.passage = value;
            return this;
        }

        /** 상대(코어 엔드포인트)의 설정만 더 고친다. */
        Tuning peerConfig(java.util.function.UnaryOperator<EndpointConfig> value) {
            this.peerConfig = value;
            return this;
        }

        Tuning closeFinish(java.time.Duration value) {
            this.closeFinishNanos = value.toNanos();
            return this;
        }

        Tuning writeQueueBytes(long value) {
            this.writeQueueBytes = value;
            return this;
        }
    }

    final AppFixture fixture;
    final AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1_000));
    final InMemoryPassage client;
    private final InMemoryPassage peerEnd;
    final GatedPassage gate;
    final Endpoint peer;
    final List<Event> peerEvents = new ArrayList<>();
    final OwnerBudgets owner;
    final ConnectionBudgets budgets;
    final CompletableFuture<NioStrandConnection> ready = new CompletableFuture<>();
    final NioStrandConnection connection;
    boolean peerSawEnd;

    private EngineRig(
            ConnectionParams.Role role,
            RequestHandler handler,
            InMemoryPassage.Pair pair,
            java.util.concurrent.Executor appExecutor,
            Tuning tuning) {
        this.fixture = new AppFixture.Spec().perConnection(tuning.perConnection).plain();
        InMemoryPassage clientEnd = pair.first();
        this.peerEnd = pair.second();
        clientEnd.manualTime();
        this.client = clientEnd;
        this.gate = new GatedPassage(tuning.passage.apply(clientEnd));
        EndpointConfig config = tuning.config.apply(EndpointConfig.defaults()
                .knownTypes(Collections.singleton(TYPE))
                .maxConcurrentStreams(8)
                .ackTimeoutMs(HOUR_MS)
                .idleIntervalMs(HOUR_MS * 24));
        this.peer = role == ConnectionParams.Role.CLIENT
                ? Endpoint.server(tuning.peerConfig.apply(config))
                : Endpoint.client(tuning.peerConfig.apply(config));
        OwnerBudgets ownerBudgets = new OwnerBudgets(fixture.limits);
        this.owner = ownerBudgets;
        this.budgets = ownerBudgets.newConnection(fixture.limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        if (ConnectionEngine.admit(budgets, space) != null) {
            throw new AssertionError("예약하지 못했다");
        }
        java.util.concurrent.Executor executor = appExecutor == null ? fixture.executor : appExecutor;
        ConnectionParams params = new ConnectionParams(
                role,
                config,
                remaining -> gate,
                new InetSocketAddress("127.0.0.1", 1),
                new SerialExecutor(executor),
                executor,
                budgets,
                space,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                clock.get() + TimeUnit.HOURS.toNanos(1),
                tuning.closeFinishNanos,
                tuning.writeQueueBytes,
                clock::get,
                new ConnectionParams.PreparationObserver() {
                    @Override
                    public void ready(NioStrandConnection value) {
                        ready.complete(value);
                    }

                    @Override
                    public void failed(PreparationFailure failure) {
                        ready.completeExceptionally(failure.cause());
                    }
                },
                handler,
                null,
                role == ConnectionParams.Role.SERVER ? credentials -> AuthenticationResult.accepted("peer") : null,
                false);
        this.connection = new NioStrandConnection(params);
    }

    /** 엔진이 클라이언트이고 상대(코어 서버)가 요청에 응답하는 연결. 준비가 끝난 상태로 돌려준다. */
    static EngineRig client() throws Exception {
        return client(new Tuning());
    }

    static EngineRig client(Tuning tuning) throws Exception {
        EngineRig rig = new EngineRig(ConnectionParams.Role.CLIENT, null, InMemoryPassage.streams(), null, tuning);
        rig.start();
        return rig;
    }

    /** 엔진이 서버이고 상대(코어 클라이언트)가 요청을 보내는 연결. 준비가 끝난 상태로 돌려준다. */
    static EngineRig server(RequestHandler handler) throws Exception {
        return server(handler, (java.util.concurrent.Executor) null);
    }

    static EngineRig server(RequestHandler handler, Tuning tuning) throws Exception {
        EngineRig rig =
                new EngineRig(ConnectionParams.Role.SERVER, handler, InMemoryPassage.streams(), null, tuning);
        rig.start();
        return rig;
    }

    /** 앱용 실행기를 직접 정한다. 실행하지 않는 실행기처럼 앱 쪽 작업의 진행을 시험이 쥐어야 할 때 쓴다. */
    static EngineRig server(RequestHandler handler, java.util.concurrent.Executor appExecutor) throws Exception {
        EngineRig rig = new EngineRig(
                ConnectionParams.Role.SERVER, handler, InMemoryPassage.streams(), appExecutor, new Tuning());
        rig.start();
        return rig;
    }

    private void start() throws Exception {
        connection.start();
        settle();
        ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /** 수동 시계를 올리고 전송 스레드를 깨운다. */
    void advance(java.time.Duration duration) {
        clock.addAndGet(duration.toNanos());
        client.wakeup();
    }

    /** 전송 스레드가 가라앉을 때까지 상대와 주고받는다. 상대가 낸 사건은 {@link #peerEvents}에 쌓인다. */
    void settle() {
        for (int round = 0; round < 200; round++) {
            InMemoryPassage.awaitQuiescence(client);
            if (!pumpPeer()) {
                return;
            }
        }
        throw new AssertionError("주고받음이 끝나지 않았다");
    }

    /** 엔진이 쓴 것을 상대에게 먹이고 상대가 쓰려는 것을 엔진 쪽 편지함에 넣는다. 한 일이 있었으면 true. */
    boolean pumpPeer() {
        boolean any = false;
        try {
            for (Inbound inbound = peerEnd.await(0); ; inbound = peerEnd.await(0)) {
                if (inbound.kind() == Inbound.Kind.BYTES) {
                    TestMessages.deliver(peer, inbound.data());
                    any = true;
                } else if (inbound.kind() == Inbound.Kind.END) {
                    peerSawEnd = true;
                    any = true;
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            peerSawEnd = true;
        }
        List<OutboundMessage> messages = TestMessages.takeOutputs(peer);
        for (OutboundMessage message : messages) {
            try {
                TestMessages.write(peerEnd, message, 0);
            } catch (IOException e) {
                peerSawEnd = true;
            }
            any = true;
        }
        List<Event> events = TestMessages.drainEvents(peer);
        peerEvents.addAll(events);
        return any || !events.isEmpty();
    }

    /** 상대가 연결을 끊는다(통로를 닫아 엔진이 끝남을 읽게 한다). */
    void peerHangsUp() {
        peerEnd.close();
    }

    /** 상대가 받은 요청의 스트림 번호(받은 순서). */
    List<Long> peerRequestStreams() {
        List<Long> streams = new ArrayList<>();
        peerEvents.stream()
                .filter(event -> event.kind() == Event.Kind.REQUEST_RECEIVED)
                .forEach(event -> streams.add(event.streamId()));
        return streams;
    }

    /** 상대가 받은 RESET 사건의 스트림 번호와 오류 코드(받은 순서). */
    List<long[]> peerResets() {
        List<long[]> resets = new ArrayList<>();
        peerEvents.stream()
                .filter(event -> event.kind() == Event.Kind.REQUEST_RESET)
                .forEach(event -> resets.add(
                        new long[] {event.streamId(), event.resetErrorCode().getAsLong()}));
        return resets;
    }

    /** 상대가 요청에 응답한다. */
    void peerRespond(long streamId, int status, byte[] body) {
        peer.respond(streamId, status, body);
        settle();
    }

    /** 요청 사건이 받은 순서로 함께 쌓였는지 보는 보조: 사건 종류만 순서대로. */
    List<Event.Kind> peerEventKinds() {
        List<Event.Kind> kinds = new ArrayList<>();
        peerEvents.forEach(event -> kinds.add(event.kind()));
        return kinds;
    }

    long used(Resource resource) {
        return connection.resourceUsage(resource).current();
    }

    @Override
    public void close() {
        gate.openGate();
        connection.abort();
        fixture.close();
    }
}
