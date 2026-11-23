package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.github.ppzxc.strand.core.internal.ErrorCode;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import io.github.ppzxc.strand.core.internal.WriteQueueLimit;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/**
 * 연결 하나를 돌리는 전송 모듈의 연결 드라이버. {@link Endpoint} 하나를 소유하고, 전용 전송 스레드 하나만 이를 부른다. 다른 스레드의 호출({@link
 * #request}, {@link RequestHandle#cancel() cancel}, {@link #respond}, {@link #ping}, {@link #closeConnection}, {@link #abortClosing},
 * {@link #close})은 명령으로 큐에 넣고 전송 스레드를 깨운다. {@code Endpoint}를 잠금으로 감싸지 않는다.
 *
 * <p>전송 스레드가 한 번 도는 순서는 읽기와 기다림, 입력({@code receiveBounded}, {@code receiveUnit}), 시계 진행({@code advanceTo}), 명령 처리,
 * 사건 수거, 쓰기다. 같은 읽기에 들어온 ACK가 확인 대기 초과로 오판되지 않고, 기한 있는 요청이 맞춘 시계 위에서 기한을 잡는다. 기다림은 core의 {@code
 * nextDeadlineNanos}, (CLOSED 뒤라면) 마무리 기한, 읽을 것, (쓸 출력이 남았으면) 통로의 쓰기 가능, 깨움 가운데 먼저 오는 것까지이고, 기한이 없으면
 * 입출력이나 명령이 올 때까지 기다린다. 클라이언트의 서문과 HELLO는 첫 쓰기로 나간다. core는 진행 보고 방식 하나로 구동한다. 바이트 스트림 통로의 입력은
 * 어디서 잘라도 되는 바이트로, 메시지 통로의 입력은 한 단위(또는 바이트로 넘길 수 없는 입력)로 넣는다.
 *
 * <p><b>쓰기.</b> 드라이버는 core가 내는 출력을 하나씩 통로가 받아 주는 만큼만 쓰고(비블로킹·부분 쓰기) 받아 준 바이트 수를 core에 보고한다. 쓰기가
 * 막혀도(상대가 읽지 않아 소켓 버퍼가 찬 경우) 전송 스레드는 쓰기에서 기다리지 않고 읽기, 시계, 명령 처리를 계속 돈다. 그래서 읽지 않는 상대는 core의
 * 기존 규칙(확인 대기, 요청 기한, 유휴 PING)으로 끊기고, 막힘이 풀린 뒤 흐른 시간이 한꺼번에 시계에 보내지지 않는다. {@link #writtenBytes()}와
 * {@link #writtenFrames()}는 출력을 끝까지 다 썼을 때 는다.
 *
 * <p><b>결과 확정성.</b> 결과 확정성과 확인 대기가 시작되는 때는 core가 정한다(쓰기를 시작함과 넘김). 쓰기를 시작했지만 다 쓰지 못한 요청은 연결이
 * 끊기면 처리 안 됨이 아니라 결과 모름이 된다.
 *
 * <p><b>인증.</b> 서버가 받은 {@code AUTH}의 판단은 {@link ConnectionAuthenticator}에 맡긴다. 인증기는 전송 스레드에서 불리므로 블로킹하면 안
 * 되고, 판단이 늦으면 {@code CompletionStage}를 늦게 완료한다. 요청 Future는 전송 스레드에서 완료되므로, 그 위에 건 동기 콜백도 블로킹하면 안 된다.
 *
 * <p>끝내는 방법은 셋이다. {@link #closeConnection}은 연결 종료(CLOSE)를 보내고 대기 중인 스트림이 끝나길 기다리고, {@link #abortClosing}은
 * 종료 중인 연결에서 기다림을 그만두고, {@link #close}는 즉시 끊는다. 연결이 끝나면 core가 낸 결과로 남은 Future가 모두 끝나고, 종료 알림이 핸들러
 * 실행기로 한 번 나간다. 연결이 CLOSE로 끝난 뒤에는 보내는 쪽만 닫고 상대가 끊을 때까지 읽는다. 이 마무리(남은 쓰기 비우기, {@code shutdownOutput},
 * 상대가 끊을 때까지 읽기)에는 시간 상한이 있다({@link Builder#closedFinishLimitMs}, 기본 5초). 상한은 core가 낸 CLOSED 이벤트를 내보낸 시점(주입 시계)부터
 * 세며, 지나면 통로를 닫고 전송 스레드를 끝낸다. 오류 CLOSE가 막힌 쓰기 뒤에 있어 상대가 받지 못해도 같은 상한으로 끝난다. 그래서 상대가 끊지 않아도
 * {@link #close}를 따로 부를 필요가 없다. 이미 종료 알림이 나갔으므로 상한에 걸려도 새 알림을 내지 않고 {@link ConnectionClosed}도 바뀌지 않는다.
 * 전송 스레드의 기다림은 core의 {@code nextDeadline}과 이 마무리 기한 가운데 이른 쪽까지다.
 *
 * <p>연결이 끝나 전송 스레드가 마치는 때(즉시 끊기, 상대가 끊음, CLOSE 없는 끊김, CLOSED 뒤 마무리 상한) 쓰기 대기열에 남아 다 쓰지 못한 메시지는 버리고, 버린 바이트는 {@link #discardedBytes()}에 센다. 즉시 끊기는 통로를
 * 먼저 닫으므로 더 쓰지 않고, 그 밖의 경우는 마치기 전에 통로가 받아 주는 만큼 한 번 쓴다.
 *
 * <p><b>쓰기 대기열 상한과 읽기 멈춤.</b> 상대가 읽지 않으면서 PING이나 요청을 계속 보내면(ACK PING, {@code RESET(REFUSED)}, 응답한 뒤 다시 열리는
 * 상대 요청의 응답) 쓰기 대기열이 끝없이 커질 수 있다. 대기열의 남은 바이트가 상한을 넘으면 드라이버는 읽기를 멈추고, 시계 진행, 명령 처리, 쓰기는
 * 계속한다. 읽기를 멈춘 동안에는 쓰기 가능, 깨움, 기한으로만 깨어난다. 대기열이 상한 이하로 내려가면 다시 읽는다. 받는 것이 없으니 core의 유휴 PING이
 * 나가고(꺼내는 순간 확인 대기가 시작된다), 상대가 계속 읽지 않으면 확인 대기 초과로 끊긴다. 새 타이머나 새 종료 원인은 없다.
 *
 * <p><b>상한.</b> 협상 값에서 셈한다. (상대가 알린 {@code max_concurrent_streams} + 내가 알린 {@code max_concurrent_streams}) × 상대가 알린
 * {@code max_frame_size}에 제어 프레임을 위한 프레임 하나를 더한 값이다. 절대 한도에 걸리지 않는 설정에서는 규칙을 지키며 흐름 제어 한도 안에서 주고받는
 * 상대의 정상 범위보다 작지 않다.
 *
 * <p><b>절대 한도.</b> 위 값은 상대가 알린 값에 달려 있어 상대가 크게 알리면 상한이 사실상 없어진다. 그래서 절대 한도로 막는다. 기본은 64MiB이고
 * {@link Builder#maxWriteQueueBytes}로 바꾼다. 절대 한도가 정상 범위에 프레임 하나를 더한 값보다 작으면 그 범위를 꽉 채워 주고받는 두 끝도 읽기 멈춤에 걸릴 수 있으므로, 그런
 * 설정에서는 한도를 올린다. 절대 한도가 작아도 상한은 상대가 알린 최대 프레임 하나보다 작아지지 않는다.
 *
 * <p><b>범위 밖 출력과 넘침.</b> 정상 범위 밖의 출력(이미 취소한 요청의 프레임과 RESET, 애플리케이션이 따로 낸 PING 등)이 겹쳐 상한을 넘으면, 읽는
 * 상대라도 대기열이 내려갈 때까지 읽기가 멈춘다. 두 끝이 동시에 그러면 서로 기다리므로, 유휴 PING 확인 대기로 끊기거나 기한이 없으면 풀리지 않는다. 읽기
 * 멈춤 판단은 스텝이 시작될 때 한 번 하므로, 상한은 한 스텝에서 받은 입력이 만든 출력만큼 넘을 수 있다. 그 양은 한 스텝에서 받는 입력의 양, 곧 통로의 읽기 한
 * 번의 크기에 비례해 막힌다(입력 프레임 하나가 만드는 출력이 입력보다 조금 클 수 있다). 바이트 스트림 통로에서는 읽기 한 번이 여러 프레임이다.
 *
 * <p><b>읽기 멈춤의 한계.</b> 읽기를 멈추면 그동안 상대의 EOF, CLOSE, ACK도 읽지 않는다. 시계는 계속 가므로 멈춘 사이 확인 대기가 지날 수 있고, 읽기는 하지만 대기열을 비우는 데 확인 대기보다 오래 걸리는 느린 상대도 끊길 수 있다. 기한이
 * 없는 연결에서는 읽지 않는 상대가 연결을 끊게 하지 못한다. 유휴 PING 간격이 0인 설정이 그렇다. 이때는 요청 기한과 애플리케이션의 {@link #close}에
 * 맡긴다. 종료 중인 연결도 유휴 PING 간격이 0이면 같고, CLOSED 뒤에는 마무리 시간 상한이 끝낸다. 읽기를 멈춘 동안에도 통로가 닫혔거나 쓰기가 실패하면
 * (쓰기 실패는 쓰는 단계에서 드러난다) 연결은 끊긴 것으로 처리된다.
 */
public final class ConnectionDriver implements AutoCloseable {

    /** 프레임 헤더 크기(규격 2.1절). 이보다 짧은 바이너리 메시지는 8바이트 연결 서문이라 프레임으로 세지 않는다. */
    private static final int FRAME_HEADER_BYTES = Endpoint.FRAME_HEADER_SIZE;
    /** 한 스텝에서 받는 입력(통로에서 읽어 온 것)의 개수 상한. 이를 넘게 쌓였어도 시계 진행과 명령 처리를 먼저 하고 나머지는 다음 스텝에서 받는다. */
    private static final int MAX_RECEIVES_PER_STEP = 64;
    /** 한 번에 core에 넣는 입력의 단위 수 상한. 이를 넘게 쌓였어도 사이에 사건을 수거하고 출력을 쓴 뒤 나머지를 넣는다. */
    private static final int MAX_UNITS_PER_STEP = 64;
    /** 한 번에 수거하는 사건 수. */
    private static final int EVENT_BATCH = 1024;
    /** 사건·출력 수거 공간. 한 단위가 만드는 양보다 충분히 크고, 넘치면 사건 수거와 쓰기를 진행한 뒤 입력을 이어 넣는다. */
    private static final CollectionSpace COLLECTION_SPACE = CollectionSpace.of(1 << 16, 1 << 16);

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final byte[] NO_BYTES = new byte[0];

    /** CLOSED 뒤 마무리 시간 상한의 기본값(밀리초). */
    private static final long DEFAULT_CLOSED_FINISH_LIMIT_MS = 5000;

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
        private @Nullable ConnectionListener listener;
        private ConnectionAuthenticator authenticator = ConnectionAuthenticator.anonymousOnly();
        private LongSupplier clock = () -> System.nanoTime() / 1_000_000L;
        private long maxWriteQueueBytes = WriteQueueLimit.DEFAULT_ABSOLUTE_LIMIT;
        private long closedFinishLimitMs = DEFAULT_CLOSED_FINISH_LIMIT_MS;

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
         * 핸들러와 알림을 돌릴 실행기를 정한다. 핸들러 없이 알림만 받을 때 쓴다. {@link #handler}도 같은 실행기를 정한다.
         *
         * @param executor 실행기. 전송 스레드를 막지 않게 블로킹 작업을 맡길 수 있는 것이어야 한다
         * @return 이 빌더
         */
        public Builder executor(Executor executor) {
            this.handlerExecutor = executor;
            return this;
        }

        /**
         * 연결 알림(AUTH 관문 통과, 연결 종료)을 받는 리스너를 정한다. 알림은 핸들러 실행기에서 불린다. 실행기를 정하지 않으면 {@link #start}가
         * 예외를 던진다.
         *
         * @param connectionListener 리스너
         * @return 이 빌더
         */
        public Builder listener(ConnectionListener connectionListener) {
            this.listener = connectionListener;
            return this;
        }

        /**
         * 서버가 받은 {@code AUTH}를 판단하는 인증기를 정한다. 정하지 않으면 익명만 통과시킨다. 클라이언트에서는 쓰이지 않는다.
         *
         * @param connectionAuthenticator 인증기. 전송 스레드에서 불리므로 블로킹하면 안 된다
         * @return 이 빌더
         */
        public Builder authenticator(ConnectionAuthenticator connectionAuthenticator) {
            this.authenticator = connectionAuthenticator;
            return this;
        }

        /**
         * 단조 시계(밀리초)를 주입한다. 전송 스레드가 매번 흐른 시간을 재서 엔드포인트의 {@code advance}를 부른다. 정하지 않으면 {@link System#nanoTime()}이다.
         *
         * @param monotonicMillis 단조 시계
         * @return 이 빌더
         */
        public Builder clock(LongSupplier monotonicMillis) {
            this.clock = monotonicMillis;
            return this;
        }

        /**
         * 쓰기 대기열 상한의 절대 한도(바이트)를 정한다. 상한은 협상 값에서 셈하지만 상대가 알린 큰 값이 상한을 없애지 못하게 이 한도로 막는다. 정상 흐름
         * 제어 범위(상대가 알린 동시 스트림 + 내가 알린 동시 스트림) × 상대가 알린 최대 프레임에 프레임 하나를 더한 값이 기본값 {@value WriteQueueLimit#DEFAULT_ABSOLUTE_LIMIT}을
         * 넘는 설정에서는, 그 범위를 꽉 채워 주고받는 두 끝이 읽기 멈춤에 걸리지 않도록 이 값을 그 값 이상으로 올린다. 정하지 않으면 기본값이다.
         *
         * @param bytes 절대 한도. 규격이 허용하는 가장 작은 {@code max_frame_size}({@value EndpointConfig#MAX_FRAME_SIZE_LOWER_LIMIT}) 이상이어야 한다. 프레임 하나는 대기열에 담겨야 하므로 이 하한을 그대로 쓴다. 상한은 이 값이 작아도 상대가 알린 최대 프레임 하나보다 작아지지 않는다
         * @return 이 빌더
         * @throws IllegalArgumentException {@code bytes}가 {@link EndpointConfig#MAX_FRAME_SIZE_LOWER_LIMIT}보다 작을 때
         */
        public Builder maxWriteQueueBytes(long bytes) {
            if (bytes < EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT) {
                throw new IllegalArgumentException(
                        "maxWriteQueueBytes는 " + EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT + " 이상이어야 한다: " + bytes);
            }
            this.maxWriteQueueBytes = bytes;
            return this;
        }

        /**
         * 연결이 CLOSE로 끝난 뒤(core가 CLOSED를 낸 뒤)의 마무리(남은 쓰기 비우기, 상대가 끊을 때까지 읽기)에 거는 시간 상한을 정한다. 상한은 core가
         * CLOSED를 낸 스텝에서 그 이벤트를 내보낸 시점(주입 시계)부터 세고, 지나면 통로를 닫고 전송 스레드를 끝낸다. 이미 종료 알림이 나갔으므로 새 알림을 내지 않는다. 정하지
         * 않으면 5초다. core의 {@code EndpointConfig}가 아니라 전송 모듈의 설정이다.
         *
         * @param limitMs 상한(밀리초, 1 이상)
         * @return 이 빌더
         * @throws IllegalArgumentException {@code limitMs}가 1보다 작을 때
         */
        public Builder closedFinishLimitMs(long limitMs) {
            if (limitMs < 1) {
                throw new IllegalArgumentException("closedFinishLimitMs는 1 이상이어야 한다: " + limitMs);
            }
            this.closedFinishLimitMs = limitMs;
            return this;
        }

        /**
         * 전송 스레드를 시작한다. 클라이언트는 서문과 HELLO를 먼저 내보낸다.
         *
         * @return 돌아가는 연결 드라이버
         */
        public ConnectionDriver start() {
            if (listener != null && handlerExecutor == null) {
                throw new IllegalStateException("리스너를 받으려면 실행기가 있어야 한다");
            }
            ConnectionDriver driver = new ConnectionDriver(
                    role,
                    config,
                    passage,
                    handlerExecutor,
                    handler,
                    listener,
                    authenticator,
                    clock,
                    maxWriteQueueBytes,
                    closedFinishLimitMs);
            driver.thread.start();
            return driver;
        }
    }

    /**
     * 전송 스레드가 실행하는 명령. 전송 스레드가 끝나기 전에 큐에 들어간 명령은 끊긴 엔드포인트에서 {@link #execute}로 처리하고, 전송 스레드가
     * 끝난 뒤에 들어온 명령만 {@link #abandon}으로 끝낸다.
     */
    private interface Command {
        void execute(Loop loop);

        void abandon(Throwable reason);
    }

    /** 엔드포인트의 동작 하나를 그대로 부르는 명령. 결과를 돌려주지 않는다. */
    private static final class EndpointCall implements Command {
        private final Consumer<Endpoint> call;

        EndpointCall(Consumer<Endpoint> call) {
            this.call = call;
        }

        @Override
        public void execute(Loop loop) {
            call.accept(loop.endpoint);
        }

        @Override
        public void abandon(Throwable reason) {}
    }

    private final Role role;
    private final EndpointConfig config;
    private final Passage passage;
    private final @Nullable Executor handlerExecutor;
    private final @Nullable RequestHandler handler;
    private final @Nullable ConnectionListener listener;
    private final ConnectionAuthenticator authenticator;
    private final LongSupplier clock;
    private final long maxWriteQueueBytes;
    private final long closedFinishLimitMs;

    private final Queue<Command> commands = new ConcurrentLinkedQueue<>();
    private final Thread thread;
    private volatile boolean closeRequested;
    private volatile boolean stopped;
    // 관찰 지점. 전송 스레드만 늘리고 다른 스레드는 읽기만 한다.
    private final AtomicLong receivedBytes = new AtomicLong();
    private final AtomicLong writtenBytes = new AtomicLong();
    private final AtomicLong writtenFrames = new AtomicLong();
    private final AtomicLong discardedBytes = new AtomicLong();
    private final Object writeBlockedLock = new Object();
    // 쓰기 대기열이 다 나가지 못하고 쓰기 가능을 기다리는 동안 true.
    // 쓰는 스레드는 전송 스레드 하나뿐이라 바뀔 때만 writeBlockedLock을 잡고 깨우며, 기다리는 쪽은 그 잠금 안에서 읽는다.
    private volatile boolean writeBlocked;

    private ConnectionDriver(
            Role role,
            EndpointConfig config,
            Passage passage,
            @Nullable Executor handlerExecutor,
            @Nullable RequestHandler handler,
            @Nullable ConnectionListener listener,
            ConnectionAuthenticator authenticator,
            LongSupplier clock,
            long maxWriteQueueBytes,
            long closedFinishLimitMs) {
        this.role = role;
        this.config = config;
        this.passage = passage;
        this.handlerExecutor = handlerExecutor;
        this.handler = handler;
        this.listener = listener;
        this.authenticator = authenticator;
        this.clock = clock;
        this.maxWriteQueueBytes = maxWriteQueueBytes;
        this.closedFinishLimitMs = closedFinishLimitMs;
        this.thread = new Thread(this::run, "strand-nio-transport");
        this.thread.setDaemon(true);
    }

    /**
     * 클라이언트 연결 드라이버를 만든다.
     *
     * @param config 엔드포인트 설정. 통로 종류와 무관하다
     * @param passage 서버에 이어진 통로
     * @return 빌더
     */
    public static Builder client(EndpointConfig config, Passage passage) {
        return new Builder(Role.CLIENT, config, passage);
    }

    /**
     * 서버 연결 드라이버를 만든다.
     *
     * @param config 엔드포인트 설정. 통로 종류와 무관하다
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
     * @return 요청 핸들. {@link RequestHandle#result()}가 결과(status, 결과 확정성, 응답 본문)가 오면 끝난다. 실패하는 방식은 둘이다.
     *     연결이 종료 중이거나 연결이 끝나기 전에 큐에 들어간 요청은 core의 로컬 {@code UNAVAILABLE}(처리 안 됨) 결과로 끝나고, 전송
     *     스레드가 이미 끝난 뒤에 낸 요청은 {@link IllegalStateException}으로 끝난다
     */
    public RequestHandle request(int type, byte[] body) {
        return submitRequest(type, body, 0);
    }

    /**
     * 기한을 두고 요청을 낸다. 기한은 요청을 낸 시점(전송 스레드가 시계를 맞춘 뒤)부터 센다. 기한이 지나면 Future가
     * {@code DEADLINE_EXCEEDED}로 끝난다. 쓰기 전이면 처리 안 됨, 쓰기를 시작했으면 결과 모름이다.
     *
     * @param type 애플리케이션 {@code type}(256 이상)
     * @param body 요청 본문
     * @param deadlineMs 기한(밀리초, 1 이상)
     * @return 요청 핸들
     * @throws IllegalArgumentException {@code deadlineMs}가 1보다 작을 때
     */
    public RequestHandle request(int type, byte[] body, long deadlineMs) {
        if (deadlineMs < 1) {
            throw new IllegalArgumentException("deadlineMs는 1 이상이어야 한다: " + deadlineMs);
        }
        return submitRequest(type, body, deadlineMs);
    }

    private RequestHandle submitRequest(int type, byte[] body, long deadlineMs) {
        CompletableFuture<RequestResult> future = new CompletableFuture<>();
        byte[] copy = body.clone();
        TicketBox box = new TicketBox();
        submit(new Command() {
            @Override
            public void execute(Loop loop) {
                try {
                    long ticket = deadlineMs > 0
                            ? loop.endpoint.request(type, copy, deadlineMs)
                            : loop.endpoint.request(type, copy);
                    box.ticket = ticket;
                    loop.pending.put(ticket, future);
                } catch (RuntimeException e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void abandon(Throwable reason) {
                future.completeExceptionally(reason);
            }
        });
        return new RequestHandle(future, () -> cancelRequest(box));
    }

    /** 번호표는 전송 스레드에서 정해지므로 요청 명령이 채운다. 명령 큐가 FIFO라 취소 명령은 늘 요청 명령 뒤에 처리된다. */
    private static final class TicketBox {
        // 전송 스레드에서만 읽고 쓴다. 요청이 던져 번호표를 못 받았으면 NONE이다.
        long ticket = NONE;
        static final long NONE = -1L;
    }

    private void cancelRequest(TicketBox box) {
        submit(new Command() {
            @Override
            public void execute(Loop loop) {
                if (box.ticket != TicketBox.NONE) {
                    loop.endpoint.cancelRequest(box.ticket);
                }
            }

            @Override
            public void abandon(Throwable reason) {}
        });
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

    /**
     * 연결 종료(CLOSE)를 보내고 대기 중인 스트림이 끝나길 기다린다. 새 요청은 받지 않는다. 모든 스트림이 끝나면 연결이 끝나고
     * {@link ConnectionListener#onClosed}가 불린다. 어느 스레드에서든 부를 수 있다. 상대가 응답하지 않아 기다림이 늘어지면 {@link
     * #abortClosing}으로 그만둔다.
     */
    public void closeConnection() {
        submit(new EndpointCall(endpoint -> endpoint.close(ErrorCode.NO_ERROR)));
    }

    /**
     * 종료 중인 연결에서 더 기다리지 않고 닫는다. 아직 응답을 받지 못한 요청의 Future는 core가 내는 결과(결과 확정성)로 끝난다. 종료 중인
     * 연결이 아니면 아무 일도 없다. 어느 스레드에서든 부를 수 있다.
     */
    public void abortClosing() {
        submit(new EndpointCall(Endpoint::abortClosing));
    }

    /**
     * PING을 보낸다. AUTH 관문 전이면 core가 미뤘다가 통과 뒤에 보낸다. 어느 스레드에서든 부를 수 있다.
     *
     * @param payload 정확히 8바이트
     * @return PING 명령이 처리되면 끝나는 Future. payload가 8바이트가 아니거나 전송 스레드가 이미 끝났으면 예외로 끝난다. 연결이
     *     닫히는 중이라 core가 보내지 않는 PING은 조용히 버려지고 Future는 정상으로 끝난다
     */
    public CompletableFuture<Void> ping(byte[] payload) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        byte[] copy = payload.clone();
        submit(new Command() {
            @Override
            public void execute(Loop loop) {
                try {
                    loop.endpoint.ping(copy);
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

    /** 이 연결의 전송 스레드. 테스트가 이 연결의 스레드만 들여다보려는 용도다. */
    Thread transportThread() {
        return thread;
    }

    /**
     * 지금까지 {@code Endpoint}가 처리를 끝낸 받은 바이트 수(메시지 통로면 받은 메시지 내용의 합). 처리를 끝낸 뒤에 늘므로, 이 값이 어떤 입력을
     * 포함하면 그 입력의 결과 이벤트가 나오기 전이라도 이미 처리된 것이다. 어느 스레드에서든 읽을 수 있다.
     *
     * @return 받은 바이트 수
     */
    public long receivedBytes() {
        return receivedBytes.get();
    }

    /**
     * 쓰기 대기열이 통로가 받아 주는 만큼만 나가고 남아 쓰기 가능을 기다리는 상태(또는 그 상태가 풀린 것)가 될 때까지 기다린다. 시험이 시간 간격을 추측하지 않고 쓰기 막힘을
     * 관측하게 하는 패키지 범위 관측점이다. 사건이 아니라 현재 상태를 기다리므로 막힌 동안에 불러야 한다. 그 상태는 대기열이 다 나가거나 버려지거나 입출력 오류가 나면 풀린다.
     *
     * @param blocked 기다릴 상태. true면 막힘, false면 풀림
     * @param timeout 기다릴 최대 시간
     * @param unit timeout의 단위
     * @return 제한 시간 안에 그 상태이면 true, 아니면 false
     * @throws InterruptedException 기다리다 중단되면
     */
    boolean awaitWriteBlocked(boolean blocked, long timeout, TimeUnit unit) throws InterruptedException {
        long start = System.nanoTime();
        long limit = unit.toNanos(timeout);
        synchronized (writeBlockedLock) {
            while (writeBlocked != blocked) {
                long remaining = limit - (System.nanoTime() - start);
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(writeBlockedLock, remaining);
            }
            return true;
        }
    }

    /**
     * 지금까지 통로에 다 쓴 바이트 수(연결 서문 포함). 어느 스레드에서든 읽을 수 있다.
     *
     * @return 보낸 바이트 수
     */
    public long writtenBytes() {
        return writtenBytes.get();
    }

    /**
     * 지금까지 통로에 다 쓴 프레임 수. 8바이트 연결 서문은 프레임이 아니라서 세지 않는다. 어느 스레드에서든 읽을 수 있다.
     *
     * @return 보낸 프레임 수
     */
    public long writtenFrames() {
        return writtenFrames.get();
    }

    /**
     * 지금까지 통로에 쓰지 못하고 버린 바이트 수. 전송 스레드가 끝날 때(쓰기 실패, 즉시 끊기, 상대가 끊음, CLOSE 없는 끊김, CLOSED 뒤 마무리 상한 등 어떤 끝이든) 쓰기 대기열과 core 쓰기 대기열에 남아 있던
     * 메시지의 남은 바이트다. 운영자가 끝난 연결이 보내지 못한 양을 알아보는 관측용이고 알림이나 종료 원인은 바꾸지 않는다. 어느 스레드에서든 읽을 수 있다.
     *
     * @return 버린 바이트 수
     */
    public long discardedBytes() {
        return discardedBytes.get();
    }

    /**
     * 연결을 즉시 끊고 전송 스레드가 끝나길 기다린다. 통로를 닫으므로 상대가 읽지 않아 쓰기가 막혀 있어도 끝난다. 쓰기 대기열에 남은 메시지는 다 쓰지 않고 버린다. 남은 Future는 core가 내는 결과로 끝난다.
     * 즉시 끊기라서 이미 큐에 넣은 출력은 보장하지 않는다. 아직 처리되지 않은 {@link #request}, {@link #respond}, {@link #ping}은 전송 스레드가 통로
     * 닫힘을 보기 전에 처리되지 못할 수 있고, 그 요청은 core가 내는 결과나 예외로 끝난다. 출력을 다 내보내고 끝내려면 {@link #closeConnection()}을 쓴다.
     * 여러 번 불러도 된다.
     */
    @Override
    public void close() {
        closeRequested = true;
        passage.wakeup();
        if (Thread.currentThread() == thread) {
            return;
        }
        // 깨움으로는 끝나지 않는 대기(선택기, 통로 안쪽의 보내기 대기 등)를 통로를 닫아 끝낸다.
        passage.close();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 큐에 넣은 뒤 {@code stopped}를 본다. 전송 스레드는 큐를 소진한 뒤 {@code stopped}를 올리고 다시 한 번 비우므로, 명령은 소진되거나
     * (소진 뒤에 들어왔다면) 이 두 번째 비움이나 여기서의 비움으로 끝난다. 어느 쪽에도 안 걸리는 틈은 없다.
     */
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

    private void setWriteBlocked(boolean blocked) {
        // 바뀔 때만 잠그고 깨워, 상태가 그대로인 대부분의 쓰기에는 비용이 없다.
        if (writeBlocked == blocked) {
            return;
        }
        synchronized (writeBlockedLock) {
            writeBlocked = blocked;
            writeBlockedLock.notifyAll();
        }
    }

    /** 전송 스레드의 본체. */
    private void run() {
        try {
            drive(new Loop(role == Role.CLIENT ? Endpoint.client(config) : Endpoint.server(config)));
        } finally {
            stopped = true;
            // 어떤 경로로 끝났든 스레드가 끝나면 쓰기 막힘 신호는 풀려 있다.
            setWriteBlocked(false);
            passage.close();
            abandonRemainingCommands();
        }
    }

    private void drive(Loop loop) {
        Throwable failure = null;
        try {
            loop.settle();
            while (!loop.finished) {
                loop.step();
            }
        } catch (RuntimeException | Error e) {
            // 예기치 못한 오류(핸들러 실행기 거부, 엔드포인트가 거부한 입력 등). 연결을 끊어 core가 내는 결과로 Future를 끝내고,
            // 오류는 스레드의 처리기가 보게 다시 던진다.
            failure = e;
            loop.abort(e);
            throw e;
        } finally {
            finish(loop, failure);
        }
    }

    /**
     * 전송 스레드가 끝나기 전의 마무리. {@code stopped}를 올리기 전에, 연결이 끝나기 전에 큐에 들어간 명령을 이미 끊긴 엔드포인트로 흘려 core가
     * 내는 결과로 끝낸다. 소진 중 난 오류가 멈추게 한 원래 오류를 가리지 않으며, 어떤 경우에도 남은 Future는 끝낸다.
     */
    private void finish(Loop loop, @Nullable Throwable failure) {
        try {
            loop.drainCommands();
        } catch (RuntimeException | Error e) {
            if (failure == null) {
                throw e;
            }
            failure.addSuppressed(e);
        } finally {
            try {
                // 연결이 끝나 더 쓰지 않는다. 남은 것은 버리고 센다. 이것이 던져도 남은 Future는 끝낸다.
                loop.discardQueued();
            } finally {
                loop.failPending();
            }
        }
    }

    /** 전송 스레드 안에서만 쓰는 상태. {@link Endpoint}와 번호표 → Future 표는 여기에만 있다. */
    private final class Loop {
        final Endpoint endpoint;
        final Map<Long, CompletableFuture<RequestResult>> pending = new HashMap<>();
        boolean finished;
        // core가 CLOSED를 냈다. 종료 알림은 이때 한 번만 나간다.
        private boolean closed;
        private @Nullable CloseReason closeReason;
        // CLOSED 뒤 마무리 기한(주입 시계 값). CLOSED를 낸 시점에 정해진다.
        private long closedFinishDeadline;
        // 읽기나 쓰기가 실패했거나 상대가 끊었다. 더 읽을 것이 없다.
        private boolean transportGone;
        private boolean outputShutdown;
        // 받아 두고 아직 core에 다 넣지 못한 입력. 바이트 스트림 입력은 input의 inputPos부터, 한 단위나 잘못된 입력은 pendingUnit이다.
        private byte[] input = NO_BYTES;
        private int inputPos;
        private @Nullable Inbound pendingUnit;
        // 지금 쓰는 출력과 그 바이트, 통로가 받아 준 만큼의 위치. core가 출력을 철회하거나 우선순위를 바꾸면 식별자가 달라진다.
        private @Nullable EndpointOutput writing;
        private byte[] writingBytes = NO_BYTES;
        private int writingOffset;
        // 인증 판단 도중 난 애플리케이션 인증기의 실패. 종료 알림에 싣는다.
        private @Nullable Throwable authenticatorFailure;
        // 연결을 끊게 한 첫 입출력 예외. 종료 알림에 싣는다.
        private @Nullable IOException ioFailure;
        // 엔드포인트의 가상 시계는 0에서 시작한다. startedAt은 그 0에 해당하는 주입 시계 값이고, advancedTo까지 advance했다.
        private final long startedAt;
        private long lastElapsedNanos;

        Loop(Endpoint endpoint) {
            this.endpoint = endpoint;
            this.startedAt = clock.getAsLong();
        }

        /** 읽기와 받기, 시계 진행, 명령 처리, 사건 수거, 쓰기를 한 번 돈다. */
        void step() {
            try {
                receiveAvailable();
            } catch (IOException e) {
                transportFailed(e);
            }
            advanceClock();
            runCommands();
            if (closeRequested) {
                disconnect();
                finished = true;
            }
            settle();
        }

        /**
         * 기다렸다가, 지금 읽을 수 있는 것을 최대 {@value #MAX_RECEIVES_PER_STEP}개까지 받아 core에 넣는다. 같은 읽기에 도착해 있던 ACK가 시계를
         * 보내기 전에 받아지므로 확인 대기 초과로 오판되지 않는데, 이는 그 입력이 한 스텝 상한 이내일 때만 보장한다. 상한에 걸리면 남은 입력은 다음
         * 스텝에서 읽히므로 그 사이 시계가 기한을 넘으면 오판할 수 있다. 상대가 계속 보내도 시계와 명령 처리가 굶지 않게 하려는 설계상 절충이다. 입력은 한
         * 번에 최대 {@value #MAX_UNITS_PER_STEP}단위씩 넣고, 그 사이에 사건을 수거하고 출력을 쓴다. 다 넣지 못한 입력이 남았으면 새로 읽지 않는다.
         */
        private void receiveAvailable() throws IOException {
            boolean pendingInput = hasPendingInput();
            // 쓰기 대기열이 상한을 넘으면 읽기를 멈춘다. 이때는 쓰기 가능, 깨움, 기한으로만 깨어나므로 읽을 것이 남아 있어도 빈 깨어남이 없다.
            Inbound inbound =
                    passage.await(pendingInput ? 0 : waitLimit(), !pendingInput && !readPaused(), wantsWrite());
            accept(inbound);
            processInput();
            int received = inbound.kind() == Inbound.Kind.NONE ? 0 : 1;
            while (received > 0
                    && received < MAX_RECEIVES_PER_STEP
                    && inbound.kind() != Inbound.Kind.END
                    && !hasPendingInput()) {
                inbound = passage.await(0);
                if (inbound.kind() == Inbound.Kind.NONE) {
                    return;
                }
                accept(inbound);
                processInput();
                received++;
            }
        }

        private boolean hasPendingInput() {
            return inputPos < input.length || pendingUnit != null;
        }

        /** 읽어 온 것을 받아 둔다. 끝남이면 통로가 끊긴 것으로 core에 알린다. */
        private void accept(Inbound inbound) {
            switch (inbound.kind()) {
                case BYTES:
                    input = inbound.data();
                    inputPos = 0;
                    break;
                case UNIT:
                case UNSUPPORTED:
                    pendingUnit = inbound;
                    break;
                case END:
                    transportFailed(new EOFException("상대가 연결을 끊었다"));
                    break;
                default:
                    break;
            }
        }

        private boolean wantsWrite() {
            return endpoint.nextOutput() != null || passage.pendingOutput();
        }

        /** 쓰기 대기열이 상한을 넘어 읽기를 멈춰야 하는가. 상한 아래로 내려가면 다시 읽는다. */
        private boolean readPaused() {
            // 상한이 늘 프레임 하나 이상이라 빈 대기열은 어차피 상한을 넘지 않는다.
            return endpoint.pendingOutputBytes()
                    > WriteQueueLimit.of(
                            endpoint.peerMaxFrameSize(),
                            endpoint.peerMaxConcurrentStreams(),
                            endpoint.ownMaxConcurrentStreams(),
                            maxWriteQueueBytes);
        }

        /**
         * 받아 둔 입력을 유한 단위로 core에 넣는다. 수거 공간이 모자라 멈추면 사건을 수거하고 출력을 쓴 뒤 소비 위치부터 다시 넣는다. 출력이 막혀 공간이
         * 비지 않으면 남은 입력을 두고 돌아온다(다음 스텝이 쓰기 가능을 기다렸다가 이어 넣는다).
         */
        private void processInput() {
            for (; ; ) {
                ReceiveProgress progress;
                Inbound unit = pendingUnit;
                if (unit != null) {
                    progress = unit.kind() == Inbound.Kind.UNIT
                            ? endpoint.receiveUnit(unit.data(), 0, unit.data().length, COLLECTION_SPACE)
                            : endpoint.receiveUnsupportedInput(COLLECTION_SPACE);
                    if (!progress.collectionRequired()) {
                        pendingUnit = null;
                        if (unit.kind() == Inbound.Kind.UNIT) {
                            receivedBytes.addAndGet(unit.data().length);
                        }
                    }
                } else if (inputPos < input.length) {
                    progress = endpoint.receiveBounded(
                            input, inputPos, input.length - inputPos, MAX_UNITS_PER_STEP, COLLECTION_SPACE);
                    inputPos += progress.consumed();
                    receivedBytes.addAndGet(progress.consumed());
                    if (!progress.collectionRequired() && progress.consumed() == 0) {
                        // 연결이 이미 끝나 core가 입력을 받지 않는다. 남은 입력은 버린다.
                        inputPos = input.length;
                    }
                } else {
                    return;
                }
                if (progress.collectionRequired()) {
                    // 수거 공간이 찼다. 사건을 내보내고 출력을 써서 공간을 비운다. 그 밖에는 사건을 스텝 끝(settle)에서 내보내므로, 같은 읽기에 온
                    // 입력이 모두 core에 들어간 뒤에 내보낸다.
                    dispatchEvents();
                    boolean progressed;
                    try {
                        progressed = flushOutputs();
                    } catch (IOException e) {
                        recordTransportFailure(e);
                        return;
                    }
                    dispatchEvents();
                    if (!progressed) {
                        return;
                    }
                }
            }
        }

        /**
         * 기다림의 상한. core가 알려 준 다음 기한과 CLOSED 뒤 마무리 기한 가운데 이른 쪽까지이고, 둘 다 없으면 상한이 없다.
         */
        private long waitLimit() {
            long limit = -1;
            OptionalLong next = endpoint.nextDeadlineNanos();
            if (next.isPresent()) {
                limit = clampWait(next.getAsLong() / NANOS_PER_MILLI - (clock.getAsLong() - startedAt));
            }
            long untilFinish = untilFinish();
            if (untilFinish >= 0) {
                limit = limit < 0 ? untilFinish : Math.min(limit, untilFinish);
            }
            return limit;
        }

        /** CLOSED 뒤 마무리 기한까지 남은 시간(밀리초, 0이면 지났다). 마무리 중이 아니면 -1이다. */
        private long untilFinish() {
            if (!closed || finished) {
                return -1;
            }
            return clampWait(closedFinishDeadline - clock.getAsLong());
        }

        /**
         * 남은 시간을 0 이상 int 범위로 자른다. 통로의 시간 제한이 int 범위로 잘리는 구현에서 바쁜 대기가 되지 않게 한 번의 기다림을 int 범위로 자른다.
         * 기한이 더 멀면 깨어난 뒤 다시 계산한다.
         */
        private long clampWait(long remainingMs) {
            return Math.min(Math.max(0, remainingMs), Integer.MAX_VALUE);
        }

        /** 주입 시계로 흐른 시간만큼 엔드포인트의 시계를 옮긴다. */
        private void advanceClock() {
            long elapsedMs = clock.getAsLong() - startedAt;
            long elapsedNanos =
                    elapsedMs > Long.MAX_VALUE / NANOS_PER_MILLI ? Long.MAX_VALUE : elapsedMs * NANOS_PER_MILLI;
            lastElapsedNanos = Math.max(lastElapsedNanos, elapsedNanos);
            endpoint.advanceTo(lastElapsedNanos);
        }

        /** 보내지 못하고 남은 출력을 세고 끊는다. 끊긴 core는 남은 출력을 버린다. */
        private void disconnect() {
            discardedBytes.addAndGet(endpoint.pendingOutputBytes());
            endpoint.disconnect();
        }

        /** 통로가 끊겼다. 연결이 아직 끝나지 않았다면 CLOSE 없이 끊긴 것으로 core에 알린다. */
        private void transportFailed(IOException cause) {
            try {
                // 같은 읽기에 CLOSE 교환과 EOF가 함께 와 있을 수 있다. 앞선 입력이 낸 CLOSED를 먼저 내보내야 이 오류가 종료 뒤의 것으로 구분된다.
                dispatchEvents();
            } finally {
                // 내보내다 실행기 거부로 던져도 입출력 예외 기록과 끊김 통지는 빠뜨리지 않는다. abort 경로의 종료 알림에도 예외가 실려야 한다.
                recordTransportFailure(cause);
                // 스스로 끊는 중이면 이미 낸 명령을 처리한 뒤 한 번에 끊는다. 통로를 먼저 닫았다고 명령보다 앞서 끊으면 안 된다.
                if (!closed && !closeRequested) {
                    disconnect();
                }
            }
        }

        private void recordTransportFailure(IOException cause) {
            transportGone = true;
            if (closed) {
                // CLOSED 뒤에 상대가 끊기를 기다리던 읽기가 끝났다. 연결 종료(CLOSE)로 끝난 연결의 마무리다.
                finished = true;
            } else if (ioFailure == null && !closeRequested) {
                // 스스로 끊은 연결이 닫힌 통로에서 낸 입출력 예외는 연결을 끊게 한 원인이 아니다.
                ioFailure = cause;
            }
        }

        /**
         * 연결이 끝난 뒤, 전송 스레드가 끝나기 전에 큐에 남은 명령을 이미 끊긴 엔드포인트에 흘린다. 요청은 core의 로컬 {@code UNAVAILABLE}
         * (처리 안 됨)로 끝나고, 엔드포인트가 던지는 예외는 그 명령의 결과가 된다. 이 뒤에 들어온 명령은 {@link #stopped}가 오른 뒤
         * {@link #abandonRemainingCommands}가 예외로 끝낸다. 이미 오류로 멈추는 중이면 원래 오류를 가리지 않도록 내보내다 난 예외는 삼킨다
         * (남은 Future는 {@link #failPending}이 끝낸다).
         */
        void drainCommands() {
            try {
                while (!commands.isEmpty()) {
                    Command command = commands.poll();
                    while (command != null) {
                        try {
                            command.execute(this);
                        } catch (RuntimeException e) {
                            command.abandon(e);
                        }
                        command = commands.poll();
                    }
                    dispatchEvents();
                }
            } catch (RuntimeException e) {
                // 실행기 거부 등. 남은 Future는 failPending이 끝낸다.
            }
        }

        private void runCommands() {
            Command command = commands.poll();
            while (command != null) {
                command.execute(this);
                command = commands.poll();
            }
        }

        /**
         * 이벤트를 내보내고 쌓인 메시지를 쓴다. 쓰다 실패하면 끊김으로 보고 그 결과 이벤트까지 내보낸다. 연결이 끝났으면 보내는 쪽만 닫고 상대가 끊을
         * 때까지 읽는다.
         */
        void settle() {
            dispatchEvents();
            if (untilFinish() == 0) {
                // CLOSED 뒤 마무리 기한이 지났다. 이미 종료 알림이 나갔으므로 새 알림 없이 끝낸다. 남은 출력은 통로가 받아 주는 만큼 한 번만 쓴다.
                finished = true;
            }
            if (!write()) {
                // 스스로 끊는 중이면 통로를 먼저 닫아서 난 실패다. 낸 명령을 처리한 뒤 step이 끊는다.
                if (!closeRequested) {
                    disconnect();
                    dispatchEvents();
                    write();
                    finished = true;
                }
            } else if (closed && !finished && !outputShutdown && outputsDone()) {
                outputShutdown = true;
                // core의 전송 종료 지시가 닫으라고 하고(NONE이 아님) 남은 출력을 다 넘겼다. 종료 원인은 통로가 닫는 방식을 고를 때 쓴다.
                if (endpoint.transportClose() != Endpoint.TransportClose.NONE && closeReason != null) {
                    passage.shutdownOutput(closeReason);
                } else {
                    passage.shutdownOutput();
                }
            }
        }

        /** 쓸 출력이 없고 통로 안에 남은 출력도 없다. */
        private boolean outputsDone() {
            return endpoint.nextOutput() == null && !passage.pendingOutput();
        }

        /**
         * 오류로 멈출 때 연결을 끊고 core가 내는 결과를 내보낸다. 이것마저 실패하면 {@link #failPending}이 남은 Future를 끝낸다. 내보내다 난
         * 예외는 멈추게 한 오류에 덧붙인다.
         */
        void abort(Throwable original) {
            try {
                disconnect();
                dispatchEvents();
                write();
            } catch (RuntimeException e) {
                original.addSuppressed(e);
            }
        }

        /**
         * 출력을 통로가 받아 주는 만큼 쓰고 받아 준 바이트 수를 core에 보고한다. 기다리지 않는다. 출력 하나를 끝까지 다 쓴 것만 쓴 양에 더한다. 바이트
         * 스트림 통로는 실제로 쓴 만큼, 메시지 통로는 메시지 전체를 받아 주면 그 전체 길이를 보고한다.
         *
         * @return 한 바이트라도 받아 줬거나 쓸 출력이 없으면 true, 통로가 받아 주지 않아 막혔으면 false
         * @throws IOException 입출력 오류
         */
        private boolean flushOutputs() throws IOException {
            boolean progressed = false;
            for (; ; ) {
                EndpointOutput output = endpoint.nextOutput();
                if (output == null) {
                    writing = null;
                    setWriteBlocked(false);
                    return true;
                }
                if (writing == null || writing.id() != output.id()) {
                    writing = output;
                    writingBytes = output.readOnlyBytes();
                    writingOffset = 0;
                }
                int remaining = writingBytes.length - writingOffset;
                int accepted = passage.writeBytes(writingBytes, writingOffset, remaining);
                if (accepted > 0) {
                    endpoint.outputProgress(output.id(), accepted);
                    writingOffset += accepted;
                    progressed = true;
                    if (writingOffset == writingBytes.length) {
                        completeOutput(writingBytes.length);
                        writing = null;
                    }
                }
                if (accepted < remaining) {
                    setWriteBlocked(true);
                    return progressed;
                }
            }
        }

        /** 출력 하나를 끝까지 다 썼을 때 쓴 양에 더한다. */
        private void completeOutput(int length) {
            writtenBytes.addAndGet(length);
            if (length >= FRAME_HEADER_BYTES) {
                writtenFrames.incrementAndGet();
            }
        }

        /** 통로가 끝났으니 남은 출력을 버린다. 끊긴 core가 버리므로 버릴 양을 먼저 센다. */
        void discardQueued() {
            try {
                disconnect();
                writing = null;
                writingOffset = 0;
            } finally {
                // 버리기를 마친 뒤에 막힘 신호를 푼다. 풀린 것을 본 쪽이 집계를 온전히 보게 하며, 중간에 예외가 나도 푼다.
                setWriteBlocked(false);
            }
        }

        /**
         * 출력을 쓴다. 쓰다 입출력 오류가 나면 끊김으로 기록한다.
         *
         * @return 입출력 오류 없이 돌아왔으면 true(다 못 쓰고 남았어도 true)
         */
        private boolean write() {
            try {
                flushOutputs();
                return true;
            } catch (IOException e) {
                try {
                    recordTransportFailure(e);
                } finally {
                    setWriteBlocked(false);
                }
                return false;
            }
        }

        private void dispatchEvents() {
            RuntimeException deliveryFailure = null;
            for (; ; ) {
                List<Event> batch = endpoint.drainEvents(EVENT_BATCH);
                if (batch.isEmpty()) {
                    break;
                }
                for (Event event : batch) {
                    // 같은 묶음의 다른 이벤트(결과, 종료)를 잃지 않도록 넘기다 난 실패를 모았다가 다 내보낸 뒤 던진다.
                    try {
                        dispatch(event);
                    } catch (RuntimeException e) {
                        deliveryFailure = e;
                    }
                }
            }
            if (deliveryFailure != null) {
                throw deliveryFailure;
            }
        }

        private void dispatch(Event event) {
            switch (event.kind()) {
                case RESULT:
                    complete(event);
                    break;
                case REQUEST_RECEIVED:
                    deliver(event);
                    break;
                case AUTHENTICATED:
                    notifyAuthenticated(event);
                    break;
                case AUTHENTICATION_REQUIRED:
                    authenticate(event);
                    break;
                case CLOSED:
                    // 알림이 실패해도 한 번만 나가도록 먼저 표시한다.
                    if (!closed) {
                        long closedAtMs = clock.getAsLong();
                        // 아주 큰 상한이 넘쳐 음수 기한이 되지 않게 더하기를 포화시킨다. 기한은 처음 CLOSED에서만 정한다.
                        closedFinishDeadline = closedAtMs > Long.MAX_VALUE - closedFinishLimitMs
                                ? Long.MAX_VALUE
                                : closedAtMs + closedFinishLimitMs;
                    }
                    closed = true;
                    closeReason = event.closeReason().orElse(null);
                    if (transportGone || isDisconnection(event)) {
                        finished = true;
                    }
                    notifyClosed(event);
                    break;
                case REQUEST_RESET:
                    // 드라이버의 받은 요청에는 취소 개념이 없어 앱에 알리지 않는다. 이후 응답은 core가 거절한다.
                    break;
                default:
                    throw new IllegalStateException("처리하지 않는 사건 종류: " + event.kind());
            }
        }

        private boolean isDisconnection(Event event) {
            return event.closeReason()
                    .map(r -> r.kind() == CloseReason.Kind.DISCONNECTED)
                    .orElse(true);
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

        private void notifyAuthenticated(Event event) {
            Executor executor = handlerExecutor;
            ConnectionListener target = listener;
            if (executor == null || target == null) {
                return;
            }
            OptionalLong lifetime = event.credentialLifetime();
            executor.execute(() -> target.onAuthenticated(lifetime));
        }

        private void notifyClosed(Event event) {
            Executor executor = handlerExecutor;
            ConnectionListener target = listener;
            if (executor == null || target == null) {
                return;
            }
            ConnectionClosed notice = new ConnectionClosed(event.closeReason().get(), ioFailure, authenticatorFailure);
            executor.execute(() -> target.onClosed(notice));
        }

        /**
         * 받은 AUTH의 판단을 애플리케이션 인증기에 맡긴다. 인증기가 판단을 늦게 내도 전송 스레드는 기다리지 않고, 판단이 나오면 명령으로 돌아와 core에
         * 입력한다. 인증기가 던지거나 실패로 끝나면 일시 실패로 본다.
         */
        private void authenticate(Event event) {
            long id = event.authenticationId();
            CompletionStage<AuthDecision> decision;
            try {
                decision = authenticator.authenticate(
                        event.authenticationMechanism().get(), event.body());
            } catch (RuntimeException e) {
                failAuthentication(id, e);
                return;
            }
            decision.whenComplete((value, failure) -> submit(new EndpointCall(endpoint -> {
                if (failure != null || value == null) {
                    failAuthentication(id, failure != null ? failure : new NullPointerException("인증 판단이 비어 있다"));
                } else {
                    endpoint.completeAuthentication(id, value);
                }
            })));
        }

        /** 인증기 실패를 기록하고 일시 실패로 core에 알린다. */
        private void failAuthentication(long id, Throwable cause) {
            authenticatorFailure = cause;
            endpoint.completeAuthentication(id, AuthDecision.TRANSIENT_FAILURE);
        }

        // 최후 수단. core가 낸 결과로 끝나지 않고 남은 Future만 예외로 끝낸다. 연결 종료(CLOSE)와 abort 뒤에는 비어 있다.
        void failPending() {
            for (CompletableFuture<RequestResult> future : pending.values()) {
                future.completeExceptionally(new IllegalStateException("연결이 끝났다"));
            }
            pending.clear();
        }
    }
}
