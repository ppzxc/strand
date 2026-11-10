package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/**
 * 연결 하나를 돌리는 전송 모듈의 연결 드라이버. {@link Endpoint} 하나를 소유하고, 전용 전송 스레드 하나만 이를 부른다. 다른 스레드의 호출({@link
 * #request}, {@link #respond}, {@link #close})은 명령으로 큐에 넣고 전송 스레드를 깨운다. {@code Endpoint}를 잠금으로 감싸지 않는다.
 *
 * <p>전송 스레드가 한 번 도는 순서는 읽기, {@code receive}/{@code receiveMessage}, 명령 처리, 이벤트 배출, 메시지 꺼내기, 쓰기다. 시계 진행은
 * 읽기와 명령 처리 사이에 들어간다(아직 없다). 클라이언트의 서문과 HELLO는 첫 쓰기로 나간다.
 *
 * <p>{@code Authenticator}는 core가 {@code receive} 안에서 동기로 부르므로 전송 스레드에서 돈다. 블로킹하지 않는 {@code Authenticator}만
 * 지원한다. 요청 Future는 전송 스레드에서 완료되므로, 그 위에 건 동기 콜백도 블로킹하면 안 된다.
 */
public final class ConnectionDriver implements AutoCloseable {

    private enum Role {
        CLIENT,
        SERVER
    }

    /** 연결 드라이버를 만들어 시작한다. */
    public static final class Builder {
        private final Role role;
        private final EndpointConfig config;
        private final Passage passage;
        private @Nullable Executor handlerExecutor;
        private @Nullable RequestHandler handler;
        private LongSupplier clock = () -> System.nanoTime() / 1_000_000L;

        private Builder(Role role, EndpointConfig config, Passage passage) {
            this.role = role;
            this.config = config;
            this.passage = passage;
        }

        /**
         * 상대가 낸 요청을 받는 핸들러와, 핸들러를 돌릴 실행기를 정한다. 정하지 않으면 받은 요청은 응답 없이 둔다. 실행기가 요청을 받지 않고
         * 예외를 던지면(예: {@code RejectedExecutionException}) 연결을 끊고 전송 스레드를 끝낸다. 남은 요청의 Future는 core가 끊김으로 내는
         * 결과로 끝나고, 예외는 전송 스레드의 처리기로 전해진다.
         *
         * @param executor 핸들러를 돌리는 실행기. 전송 스레드를 막지 않게 블로킹 작업을 맡길 수 있는 것이어야 한다
         * @param requestHandler 핸들러
         * @return 이 빌더
         */
        public Builder handler(Executor executor, RequestHandler requestHandler) {
            this.handlerExecutor = executor;
            this.handler = requestHandler;
            return this;
        }

        /**
         * 단조 시계(밀리초)를 주입한다. 시계 진행은 다음 티켓이 끼운다. 정하지 않으면 {@link System#nanoTime()}이다.
         *
         * @param monotonicMillis 단조 시계
         * @return 이 빌더
         */
        public Builder clock(LongSupplier monotonicMillis) {
            this.clock = monotonicMillis;
            return this;
        }

        /**
         * 전송 스레드를 시작한다. 클라이언트는 서문과 HELLO를 먼저 내보낸다.
         *
         * @return 돌아가는 연결 드라이버
         */
        public ConnectionDriver start() {
            ConnectionDriver driver = new ConnectionDriver(role, config, passage, handlerExecutor, handler, clock);
            driver.thread.start();
            return driver;
        }
    }

    /** 전송 스레드가 실행하는 명령. 전송 스레드가 끝난 뒤에 들어온 명령은 {@link #abandon}으로 끝낸다. */
    private interface Command {
        void execute(Loop loop);

        void abandon(Throwable reason);
    }

    private final Role role;
    private final EndpointConfig config;
    private final Passage passage;
    private final @Nullable Executor handlerExecutor;
    private final @Nullable RequestHandler handler;
    // 시계 진행은 다음 티켓(#236)이 전송 스레드에서 쓴다. 지금은 주입 지점만 둔다.
    @SuppressWarnings("UnusedVariable")
    private final LongSupplier clock;

    private final Queue<Command> commands = new ConcurrentLinkedQueue<>();
    private final Thread thread;
    private volatile boolean closeRequested;
    private volatile boolean stopped;

    private ConnectionDriver(
            Role role,
            EndpointConfig config,
            Passage passage,
            @Nullable Executor handlerExecutor,
            @Nullable RequestHandler handler,
            LongSupplier clock) {
        this.role = role;
        this.config = config;
        this.passage = passage;
        this.handlerExecutor = handlerExecutor;
        this.handler = handler;
        this.clock = clock;
        this.thread = new Thread(this::run, "strand-nio-transport");
        this.thread.setDaemon(true);
    }

    /**
     * 클라이언트 연결 드라이버를 만든다.
     *
     * @param config 엔드포인트 설정. 메시지 통로면 {@code webSocket()}을 켠 설정이어야 한다
     * @param passage 서버에 이어진 통로
     * @return 빌더
     */
    public static Builder client(EndpointConfig config, Passage passage) {
        return new Builder(Role.CLIENT, config, passage);
    }

    /**
     * 서버 연결 드라이버를 만든다.
     *
     * @param config 엔드포인트 설정. 메시지 통로면 {@code webSocket()}을 켠 설정이어야 한다
     * @param passage 맺어진 클라이언트에 이어진 통로
     * @return 빌더
     */
    public static Builder server(EndpointConfig config, Passage passage) {
        return new Builder(Role.SERVER, config, passage);
    }

    /**
     * 요청을 낸다. AUTH 관문 전이면 core가 미뤘다가 통과 뒤에 보낸다. 어느 스레드에서든 부를 수 있다.
     *
     * @param type 애플리케이션 {@code type}(256 이상)
     * @param body 요청 본문
     * @return 요청 핸들. {@link RequestHandle#result()}가 결과(status, 결과 확정성, 응답 본문)가 오면 끝나고, 보낼 수 없는 요청이면 예외로
     *     끝난다
     */
    public RequestHandle request(int type, byte[] body) {
        CompletableFuture<RequestResult> future = new CompletableFuture<>();
        byte[] copy = body.clone();
        submit(new Command() {
            @Override
            public void execute(Loop loop) {
                try {
                    loop.pending.put(loop.endpoint.request(type, copy), future);
                } catch (RuntimeException e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void abandon(Throwable reason) {
                future.completeExceptionally(reason);
            }
        });
        return new RequestHandle(future);
    }

    /**
     * 상대가 낸 요청에 스트림 번호로 응답한다. 어느 스레드에서든 부를 수 있다.
     *
     * @param streamId 상대가 연 스트림 번호
     * @param status 규격 2.5.1절의 status 값
     * @param body 응답 본문
     * @return 응답 명령이 처리되면 끝나는 Future. 보낼 수 없는 응답이면 예외로 끝난다
     */
    public CompletableFuture<Void> respond(long streamId, int status, byte[] body) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        byte[] copy = body.clone();
        submit(new Command() {
            @Override
            public void execute(Loop loop) {
                try {
                    loop.endpoint.respond(streamId, status, copy);
                    future.complete(null);
                } catch (RuntimeException e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void abandon(Throwable reason) {
                future.completeExceptionally(reason);
            }
        });
        return future;
    }

    /** 연결을 즉시 끊고 전송 스레드가 끝나길 기다린다. 남은 Future는 core가 내는 결과로 끝난다. 여러 번 불러도 된다. */
    @Override
    public void close() {
        closeRequested = true;
        passage.wakeup();
        if (Thread.currentThread() == thread) {
            return;
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void submit(Command command) {
        commands.add(command);
        passage.wakeup();
        if (stopped) {
            abandonRemainingCommands();
        }
    }

    private void abandonRemainingCommands() {
        Command command = commands.poll();
        while (command != null) {
            command.abandon(new IllegalStateException("연결이 끝났다"));
            command = commands.poll();
        }
    }

    /** 전송 스레드의 본체. */
    private void run() {
        try {
            drive(new Loop(role == Role.CLIENT ? Endpoint.client(config) : Endpoint.server(config)));
        } finally {
            stopped = true;
            passage.close();
            abandonRemainingCommands();
        }
    }

    private void drive(Loop loop) {
        try {
            loop.settle();
            while (!loop.finished) {
                loop.step();
            }
        } catch (RuntimeException | Error e) {
            // 예기치 못한 오류(핸들러 실행기 거부, 엔드포인트가 거부한 입력 등). 연결을 끊어 core가 내는 결과로 Future를 끝내고,
            // 오류는 스레드의 처리기가 보게 다시 던진다.
            loop.abort();
            throw e;
        } finally {
            loop.failPending();
        }
    }

    /** 전송 스레드 안에서만 쓰는 상태. {@link Endpoint}와 번호표 → Future 표는 여기에만 있다. */
    private final class Loop {
        final Endpoint endpoint;
        final Map<Long, CompletableFuture<RequestResult>> pending = new HashMap<>();
        boolean finished;

        Loop(Endpoint endpoint) {
            this.endpoint = endpoint;
        }

        /** 읽기, 받기, 명령 처리, 이벤트 배출, 쓰기를 한 번 돈다. */
        void step() {
            try {
                receive(passage.await(-1));
            } catch (IOException e) {
                endpoint.disconnect();
            }
            runCommands();
            if (closeRequested) {
                endpoint.disconnect();
            }
            settle();
        }

        private void receive(Inbound inbound) {
            switch (inbound.kind()) {
                case BYTES:
                    endpoint.receive(inbound.data());
                    break;
                case MESSAGE:
                    endpoint.receiveMessage(inbound.messageKind(), inbound.data());
                    break;
                case END:
                    endpoint.disconnect();
                    break;
                default:
                    break;
            }
        }

        private void runCommands() {
            Command command = commands.poll();
            while (command != null) {
                command.execute(this);
                command = commands.poll();
            }
        }

        /** 이벤트를 내보내고 쌓인 메시지를 쓴다. 쓰다 실패하면 끊김으로 보고 그 결과 이벤트까지 내보낸다. */
        void settle() {
            dispatchEvents();
            if (!write()) {
                endpoint.disconnect();
                dispatchEvents();
                write();
                finished = true;
            }
        }

        /** 오류로 멈출 때 연결을 끊고 core가 내는 결과를 내보낸다. 이것마저 실패하면 {@link #failPending}이 남은 Future를 끝낸다. */
        void abort() {
            endpoint.disconnect();
            dispatchEvents();
            write();
        }

        private boolean write() {
            try {
                for (OutboundMessage message : endpoint.drainMessages()) {
                    passage.write(message);
                }
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        private void dispatchEvents() {
            RuntimeException deliveryFailure = null;
            for (Event event : endpoint.drainEvents()) {
                switch (event.kind()) {
                    case RESULT:
                        complete(event);
                        break;
                    case REQUEST_RECEIVED:
                        // 같은 묶음의 다른 이벤트(결과, 종료)를 잃지 않도록 실패를 모았다가 다 내보낸 뒤 던진다.
                        try {
                            deliver(event);
                        } catch (RuntimeException e) {
                            deliveryFailure = e;
                        }
                        break;
                    case CLOSED:
                        finished = true;
                        break;
                    default:
                        break;
                }
            }
            if (deliveryFailure != null) {
                throw deliveryFailure;
            }
        }

        private void complete(Event event) {
            CompletableFuture<RequestResult> future =
                    pending.remove(event.ticket().getAsLong());
            if (future != null) {
                future.complete(
                        new RequestResult(event.status(), event.certainty().get(), event.body()));
            }
        }

        private void deliver(Event event) {
            Executor executor = handlerExecutor;
            RequestHandler target = handler;
            if (executor == null || target == null) {
                return;
            }
            IncomingRequest request =
                    new IncomingRequest(ConnectionDriver.this, event.streamId(), event.type(), event.body());
            executor.execute(() -> target.handle(request));
        }

        // 최후 수단. core가 낸 결과로 끝나지 않고 남은 Future만 예외로 끝낸다. 정상 종료와 abort 뒤에는 비어 있다.
        void failPending() {
            for (CompletableFuture<RequestResult> future : pending.values()) {
                future.completeExceptionally(new IllegalStateException("연결이 끝났다"));
            }
            pending.clear();
        }
    }
}
