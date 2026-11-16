package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/**
 * 연결 하나를 돌리는 전송 모듈의 연결 드라이버. {@link Endpoint} 하나를 소유하고, 전용 전송 스레드 하나만 이를 부른다. 다른 스레드의 호출({@link
 * #request}, {@link RequestHandle#cancel() cancel}, {@link #respond}, {@link #ping}, {@link #closeConnection}, {@link #abortClosing},
 * {@link #close})은 명령으로 큐에 넣고 전송 스레드를 깨운다. {@code Endpoint}를 잠금으로 감싸지 않는다.
 *
 * <p>전송 스레드가 한 번 도는 순서는 읽기와 기다림, {@code receive}/{@code receiveMessage}, 시계 진행({@code advance}), 명령 처리, 이벤트
 * 배출, core 쓰기 대기열에서 메시지 꺼내기, 쓰기다. 같은 읽기에 들어온 ACK가 확인 대기 초과로 오판되지 않고, 기한 있는 요청이 맞춘 시계 위에서 기한을
 * 잡는다. 기다림은 core의 {@code nextDeadline}, (CLOSED 뒤라면) 마무리 기한, 읽을 것, (쓰기 대기열이 남았으면) 통로의 쓰기 가능, 깨움 가운데 먼저
 * 오는 것까지이고, 기한이 없으면 입출력이나 명령이 올 때까지 기다린다. 클라이언트의 서문과 HELLO는 첫 쓰기로 나간다.
 *
 * <p><b>쓰기 대기열.</b> 드라이버는 쓰기 대기열을 든다. core 쓰기 대기열에서는 매 스텝 메시지를 모두 꺼내 이 대기열에 붙이고, 통로가 받아 주는 만큼만
 * 쓴다(비블로킹·부분 쓰기). 쓰기가 막혀도(상대가 읽지 않아 소켓 버퍼가 찬 경우) 전송 스레드는 쓰기에서 기다리지 않고 읽기, 시계, 명령 처리를 계속
 * 돈다. 그래서 읽지 않는 상대는 core의 기존 규칙(확인 대기, 요청 기한, 유휴 PING)으로 끊기고, 막힘이 풀린 뒤 흐른 시간이 한꺼번에 {@code advance}되지
 * 않는다. {@link #writtenBytes()}와 {@link #writtenFrames()}는 메시지를 끝까지 다 썼을 때 는다.
 *
 * <p><b>꺼낸 요청의 결과 확정성.</b> core는 쓰기 대기열에서 꺼낸 시점을 "전송 계층에 넘김"으로 본다. 확인 대기는 그때 시작되고, 꺼냈지만 아직 다 쓰지
 * 못한 요청은 연결이 끊기면 처리 안 됨이 아니라 결과 모름이 된다(안전한 쪽을 받아들인 대가다). 확인 대기 시간에는 드라이버 쓰기 대기열이 비는 시간이
 * 포함된다.
 *
 * <p><b>{@code Authenticator}는 전송 스레드에서 불리며 블로킹하면 안 된다.</b> core가 {@code receive} 안에서 동기로 부르므로 {@code
 * EndpointConfig.authenticator}에 넣은 구현은 전송 스레드에서 돈다. 블로킹하는 인증(원격 조회 등)을 넣으면 그 연결 전체가 멈추므로, 블로킹하지
 * 않는 {@code Authenticator}만 지원한다. "전송 스레드는 블로킹하는 사용자 코드를 부르지 않는다"의 유일한 예외다. 요청 Future는 전송 스레드에서
 * 완료되므로, 그 위에 건 동기 콜백도 블로킹하면 안 된다.
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
 * 번의 크기 × {@value #MAX_RECEIVES_PER_STEP}번에 비례해 막힌다(입력 프레임 하나가 만드는 출력이 입력보다 조금 클 수 있다). 바이트 스트림 통로에서는 읽기 한 번이 여러 프레임이다.
 *
 * <p><b>읽기 멈춤의 한계.</b> 읽기를 멈추면 그동안 상대의 EOF, CLOSE, ACK도 읽지 않는다. 시계는 계속 가므로 멈춘 사이 확인 대기가 지날 수 있고, 읽기는 하지만 대기열을 비우는 데 확인 대기보다 오래 걸리는 느린 상대도 끊길 수 있다. 기한이
 * 없는 연결에서는 읽지 않는 상대가 연결을 끊게 하지 못한다. 유휴 PING 간격이 0인 설정이 그렇다. 이때는 요청 기한과 애플리케이션의 {@link #close}에
 * 맡긴다. 종료 중인 연결도 유휴 PING 간격이 0이면 같고, CLOSED 뒤에는 마무리 시간 상한이 끝낸다. 읽기를 멈춘 동안에도 통로가 닫혔거나 쓰기가 실패하면
 * (쓰기 실패는 쓰는 단계에서 드러난다) 연결은 끊긴 것으로 처리된다.
 */
public final class ConnectionDriver implements AutoCloseable {

    private static final int NO_ERROR = 0;
    /** 프레임 헤더 크기(규격 2.1절). 이보다 짧은 바이너리 메시지는 8바이트 연결 서문이라 프레임으로 세지 않는다. */
    private static final int FRAME_HEADER_BYTES = Endpoint.FRAME_HEADER_SIZE;
    /** 한 스텝에서 받는 입력의 상한. 이를 넘게 쌓였어도 시계 진행과 명령 처리를 먼저 하고 나머지는 다음 스텝에서 받는다. */
    private static final int MAX_RECEIVES_PER_STEP = 64;
    /** 규격이 허용하는 가장 작은 {@code max_frame_size}(규격 3.2.1절). */
    private static final long MIN_FRAME_SIZE = 16_384L;
    /** 쓰기 대기열 상한의 기본 절대 한도. 상대가 HELLO로 알린 큰 값이 상한을 사실상 없애지 못하게 한다. */
    static final long DEFAULT_MAX_WRITE_QUEUE_BYTES = 64L * 1024 * 1024;

    /**
     * 쓰기 대기열 상한(바이트). 규칙을 지키는 상대와의 정상 흐름 제어 범위다. 내 요청(상대가 알린 동시 스트림 × 상대가 알린 최대 프레임)과 상대 요청에
     * 대한 응답(내가 알린 동시 스트림 × 상대가 알린 최대 프레임), 거기에 제어 프레임 몇 개를 위한 프레임 하나를 더한다. 상대가 알린 값은 상대가 정하므로
     * 절대 한도(기본 {@link #DEFAULT_MAX_WRITE_QUEUE_BYTES}, {@link Builder#maxWriteQueueBytes}로 바꾼다)로 막는다.
     */
    static long writeQueueLimit(
            long peerMaxFrameSize, long peerMaxConcurrentStreams, long ownMaxConcurrentStreams, long absoluteLimit) {
        long range = (peerMaxConcurrentStreams + ownMaxConcurrentStreams) * peerMaxFrameSize + peerMaxFrameSize;
        return Math.max(peerMaxFrameSize, Math.min(range, absoluteLimit));
    }

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
        private LongSupplier clock = () -> System.nanoTime() / 1_000_000L;
        private long maxWriteQueueBytes = DEFAULT_MAX_WRITE_QUEUE_BYTES;
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
         * 제어 범위(상대가 알린 동시 스트림 + 내가 알린 동시 스트림) × 상대가 알린 최대 프레임에 프레임 하나를 더한 값이 기본값 {@value #DEFAULT_MAX_WRITE_QUEUE_BYTES}을
         * 넘는 설정에서는, 그 범위를 꽉 채워 주고받는 두 끝이 읽기 멈춤에 걸리지 않도록 이 값을 그 값 이상으로 올린다. 정하지 않으면 기본값이다.
         *
         * @param bytes 절대 한도. 규격이 허용하는 가장 작은 {@code max_frame_size}(16384) 이상이어야 한다. 상한은 이 값이 작아도 상대가 알린 최대 프레임 하나보다 작아지지 않는다
         * @return 이 빌더
         * @throws IllegalArgumentException {@code bytes}가 16384보다 작을 때
         */
        public Builder maxWriteQueueBytes(long bytes) {
            if (bytes < MIN_FRAME_SIZE) {
                throw new IllegalArgumentException("maxWriteQueueBytes는 " + MIN_FRAME_SIZE + " 이상이어야 한다: " + bytes);
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

    private ConnectionDriver(
            Role role,
            EndpointConfig config,
            Passage passage,
            @Nullable Executor handlerExecutor,
            @Nullable RequestHandler handler,
            @Nullable ConnectionListener listener,
            LongSupplier clock,
            long maxWriteQueueBytes,
            long closedFinishLimitMs) {
        this.role = role;
        this.config = config;
        this.passage = passage;
        this.handlerExecutor = handlerExecutor;
        this.handler = handler;
        this.listener = listener;
        this.clock = clock;
        this.maxWriteQueueBytes = maxWriteQueueBytes;
        this.closedFinishLimitMs = closedFinishLimitMs;
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
        submit(new EndpointCall(endpoint -> endpoint.close(NO_ERROR)));
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
        // CLOSED 뒤 마무리 기한(주입 시계 값). CLOSED를 낸 시점에 정해진다.
        private long closedFinishDeadline;
        // 읽기나 쓰기가 실패했거나 상대가 끊었다. 더 읽을 것이 없다.
        private boolean transportGone;
        private boolean outputShutdown;
        // core 쓰기 대기열에서 꺼냈지만 아직 다 쓰지 못한 메시지. 맨 앞 메시지는 writeOffset바이트까지 통로가 받았다.
        private final ArrayDeque<OutboundMessage> writeQueue = new ArrayDeque<>();
        private int writeOffset;
        // writeQueue에 남은 바이트 수(맨 앞 메시지는 이미 쓴 만큼 뺀다). 읽기 멈춤 판단에 쓴다.
        private long queuedBytes;
        // 연결을 끊게 한 첫 입출력 예외. 종료 알림에 싣는다.
        private @Nullable IOException ioFailure;
        // 엔드포인트의 가상 시계는 0에서 시작한다. startedAt은 그 0에 해당하는 주입 시계 값이고, advancedTo까지 advance했다.
        private final long startedAt;
        private long advancedTo;

        Loop(Endpoint endpoint) {
            this.endpoint = endpoint;
            this.startedAt = clock.getAsLong();
            this.advancedTo = startedAt;
        }

        /** 읽기, 받기, 시계 진행({@code advance}), 명령 처리, 이벤트 배출, 쓰기를 한 번 돈다. */
        void step() {
            try {
                receiveAvailable();
            } catch (IOException e) {
                transportFailed(e);
            }
            advanceClock();
            runCommands();
            if (closeRequested) {
                endpoint.disconnect();
                finished = true;
            }
            settle();
        }

        /**
         * 기다렸다가, 지금 읽을 수 있는 것을 최대 {@link #MAX_RECEIVES_PER_STEP}개까지 받는다. 같은 읽기에 도착해 있던 ACK가 시계를 보내기 전에
         * 받아지므로 확인 대기 초과로 오판되지 않는데, 이는 그 입력이 한 스텝 상한 이내일 때만 보장한다. 상한에 걸리면 남은 입력은 다음 스텝에서
         * 읽히므로 그 사이 시계가 기한을 넘으면 오판할 수 있다. 상대가 계속 보내도 시계와 명령 처리가 굶지 않게 하려는 설계상 절충이다.
         */
        private void receiveAvailable() throws IOException {
            // 쓰기 대기열이 상한을 넘으면 읽기를 멈춘다. 이때는 쓰기 가능, 깨움, 기한으로만 깨어나므로 읽을 것이 남아 있어도 빈 깨어남이 없다.
            Inbound inbound = passage.await(waitLimit(), !readPaused(), !writeQueue.isEmpty());
            int received = 0;
            while (inbound.kind() != Inbound.Kind.NONE) {
                receive(inbound);
                received++;
                if (inbound.kind() == Inbound.Kind.END || received >= MAX_RECEIVES_PER_STEP) {
                    return;
                }
                inbound = passage.await(0);
            }
        }

        /** 쓰기 대기열이 상한을 넘어 읽기를 멈춰야 하는가. 상한 아래로 내려가면 다시 읽는다. */
        private boolean readPaused() {
            // 상한이 늘 프레임 하나 이상이라 빈 대기열은 어차피 상한을 넘지 않는다. 그 불변식이 깨져도 멈춘 채 쓰기 가능을 기다리다 아무것도 깨우지 못하는 일이 없게 하는 방어다.
            return !writeQueue.isEmpty()
                    && queuedBytes
                            > writeQueueLimit(
                                    endpoint.peerMaxFrameSize(),
                                    endpoint.peerMaxConcurrentStreams(),
                                    endpoint.ownMaxConcurrentStreams(),
                                    maxWriteQueueBytes);
        }

        /**
         * 기다림의 상한. core가 알려 준 다음 기한과 CLOSED 뒤 마무리 기한 가운데 이른 쪽까지이고, 둘 다 없으면 상한이 없다.
         */
        private long waitLimit() {
            long limit = -1;
            OptionalLong next = endpoint.nextDeadline();
            if (next.isPresent()) {
                limit = clampWait(next.getAsLong() - (clock.getAsLong() - startedAt));
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

        /** 주입 시계로 흐른 시간만큼 엔드포인트의 시계를 보낸다. */
        private void advanceClock() {
            long now = clock.getAsLong();
            if (now > advancedTo) {
                endpoint.advance(now - advancedTo);
                advancedTo = now;
            }
        }

        private void receive(Inbound inbound) {
            switch (inbound.kind()) {
                case BYTES:
                    endpoint.receive(inbound.data());
                    receivedBytes.addAndGet(inbound.data().length);
                    break;
                case MESSAGE:
                    endpoint.receiveMessage(inbound.messageKind(), inbound.data());
                    receivedBytes.addAndGet(inbound.data().length);
                    break;
                case END:
                    transportFailed(new EOFException("상대가 연결을 끊었다"));
                    break;
                default:
                    break;
            }
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
                    endpoint.disconnect();
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
                    endpoint.disconnect();
                    dispatchEvents();
                    write();
                    finished = true;
                }
            } else if (closed && !finished && !outputShutdown && writeQueue.isEmpty()) {
                outputShutdown = true;
                passage.shutdownOutput();
            }
        }

        /**
         * 오류로 멈출 때 연결을 끊고 core가 내는 결과를 내보낸다. 이것마저 실패하면 {@link #failPending}이 남은 Future를 끝낸다. 내보내다 난
         * 예외는 멈추게 한 오류에 덧붙인다.
         */
        void abort(Throwable original) {
            try {
                endpoint.disconnect();
                dispatchEvents();
                write();
            } catch (RuntimeException e) {
                original.addSuppressed(e);
            }
        }

        /** core 쓰기 대기열에서 메시지를 모두 꺼내 드라이버의 쓰기 대기열 뒤에 붙인다. 꺼낸 시점이 core가 보는 "전송 계층에 넘김"이다. */
        private void takeOutput() {
            for (OutboundMessage message : endpoint.drainMessages()) {
                writeQueue.add(message);
                queuedBytes += message.length();
            }
        }

        /**
         * 쓰기 대기열을 통로가 받아 주는 만큼만 비운다. 기다리지 않는다. 메시지를 끝까지 다 쓴 것만 쓴 양에 더한다.
         *
         * @return 입출력 오류 없이 돌아왔으면 true(다 못 쓰고 남았어도 true)
         */
        private boolean flush() {
            try {
                OutboundMessage head = writeQueue.peek();
                while (head != null) {
                    int length = head.length();
                    // 내용이 없는 메시지(WebSocket 닫기)는 호출 한 번으로 끝난다.
                    int accepted = passage.write(head, writeOffset);
                    writeOffset += accepted;
                    queuedBytes -= accepted;
                    if (writeOffset < length) {
                        return true;
                    }
                    writeQueue.poll();
                    writeOffset = 0;
                    if (head.kind() == OutboundMessage.Kind.BINARY) {
                        writtenBytes.addAndGet(length);
                        if (length >= FRAME_HEADER_BYTES) {
                            writtenFrames.incrementAndGet();
                        }
                    }
                    head = writeQueue.peek();
                }
                return true;
            } catch (IOException e) {
                recordTransportFailure(e);
                discardQueued();
                return false;
            }
        }

        /** 쓰기 대기열에 남은 메시지를 버리고 남은 바이트를 센다. */
        void discardQueued() {
            // 끊긴 엔드포인트가 마지막으로 낸 메시지(명령 소진 중 난 것)도 보내지 못한 것이다.
            takeOutput();
            discardedBytes.addAndGet(queuedBytes);
            writeQueue.clear();
            writeOffset = 0;
            queuedBytes = 0;
        }

        private boolean write() {
            takeOutput();
            return flush();
        }

        private void dispatchEvents() {
            RuntimeException deliveryFailure = null;
            for (Event event : endpoint.drainEvents()) {
                // 같은 묶음의 다른 이벤트(결과, 종료)를 잃지 않도록 넘기다 난 실패를 모았다가 다 내보낸 뒤 던진다.
                try {
                    dispatch(event);
                } catch (RuntimeException e) {
                    deliveryFailure = e;
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
                    if (transportGone || isDisconnection(event)) {
                        finished = true;
                    }
                    notifyClosed(event);
                    break;
                default:
                    break;
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
            ConnectionClosed notice = new ConnectionClosed(
                    event.closeReason().get(), ioFailure, event.cause().orElse(null));
            executor.execute(() -> target.onClosed(notice));
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
