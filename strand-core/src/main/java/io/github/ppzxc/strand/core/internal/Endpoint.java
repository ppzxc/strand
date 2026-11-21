package io.github.ppzxc.strand.core.internal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * 엔드포인트(서버 또는 클라이언트 역할). 입출력을 하지 않는 순수 상태 기계다. 받은 바이트는 {@link #receive}, 애플리케이션의 호출은
 * {@link #request}·{@link #ping}·{@link #close}, 시간은 {@link #advance}로 넣는다. 그러면 보낼 바이트와 애플리케이션 이벤트가
 * 쌓이고, {@link #drainMessages}와 {@link #drainEvents}로 한 번씩 꺼낸다. 스레드·소켓·실제 시계에 접근하지 않는다.
 */
public final class Endpoint {

    /** 프레임 헤더 크기(규격 2.1절). 전송 모듈이 바이트 수를 셀 때 쓰도록 {@code Frames}의 값을 그대로 노출한다. */
    public static final int FRAME_HEADER_SIZE = Frames.HEADER_SIZE;

    private static final int PREFACE_PREFIX_LENGTH = 6;
    private static final byte[] NO_BYTES = new byte[0];
    /** 경계에 걸친 단위를 모으는 버퍼의 처음 크기. */
    private static final int PENDING_MIN_CAPACITY = 64;
    /** 다 비웠을 때 이 크기를 넘는 버퍼는 놓는다. 큰 프레임 하나 때문에 연결이 그 크기를 계속 붙잡지 않게 한다. */
    private static final int PENDING_RELEASE_ABOVE = 65_536;

    private static final int MAX_WEBSOCKET_STATUS_CODE = 0xFFFF;
    /** 서버는 짝수, 클라이언트는 홀수 번호를 쓰고 첫 번호는 각각 2와 1이다(규격 2.2.5절). */
    private static final int FIRST_SERVER_STREAM_ID = 2;

    /** 인증 판단 식별자. 한 연결에 인증 판단은 하나뿐이라 값이 하나다. */
    static final long AUTHENTICATION_ID = 1;

    private static final int FIRST_CLIENT_STREAM_ID = 1;

    private static final int PING_PAYLOAD_SIZE = 8;
    private static final int AUTH_ACK_PARAM_SIZE = 6;

    private static final int RESET_PAYLOAD_SIZE = 4;
    private static final int TLV_HEADER_SIZE = 4;
    private static final int TIMEOUT_TAG = 0x0001;
    private static final int TIMEOUT_LENGTH = 4;
    private static final int CLOSE_ERROR_CODE_SIZE = Integer.BYTES;
    private static final int CLOSE_MIN_PAYLOAD_SIZE = Frames.CLOSE_FIXED_PAYLOAD_SIZE;
    private static final int IDEMPOTENCY_KEY_TAG = 0x0003;
    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 64;
    /** extensions_length의 한도(규격 2.4절). */
    private static final int MAX_EXTENSIONS_LENGTH = 8192;

    private final EndpointConfig config;
    private final Outbox outbox = new Outbox();
    /** 애플리케이션에 낼 이벤트. 쌓기만 하고 고치지 않는다. 돌려보낸 요청의 거름은 꺼낼 때({@link #drainEvents()}) 한다. */
    private final List<Event> events = new ArrayList<>();
    /**
     * 이벤트를 마지막으로 꺼낸 때의 {@link #highestPeerStreamId}. 상대 요청 번호는 늘어나기만 하므로, 이보다 큰 번호의 대기 중인 요청이 아직
     * 애플리케이션이 이벤트로 받지 않은 요청이다.
     */
    private long drainedPeerStreamId;
    /** 돌려보내서 {@code REQUEST_RECEIVED}를 애플리케이션에 내지 않을 요청의 번호. 상대가 먼저 RESET한 요청은 대기 중이 아니므로 여기에 오르지 않는다. */
    private final Set<Long> refusedRequests = new HashSet<>();
    /**
     * 내 요청(번호표를 받은 뒤 결과가 나올 때까지)의 장부. 번호를 받기 전(AUTH 관문 전에 미룬 요청)과 번호를 받은 뒤(쓰기 전, 쓰기 시작)를
     * 모두 가진다. 프레임 하나가 메시지 하나이므로 {@link #drainMessages()}로 메시지를 꺼내면 마지막 바이트가 넘어간 것으로 보고 장부에 쓰기
     * 시작을 알린다(규격 3.5절 "연결이 끝날 때").
     *
     * <p>장부의 닫는 중({@code onClosing})과 연결 단계의 CLOSE 교환은 같은 사건이다. 연결이 계속 열려 있을 수 있는 두 곳({@link #close},
     * {@link #handleClose})은 {@link #endQueuedRequests}로 장부에 알린다. 나머지(형태가 잘못된 CLOSE, 연결 오류, 너무 큰 AUTH)는 곧바로
     * 전송 계층을 닫아 {@link ConnectionPhase#isActive()}가 새 요청을 먼저 막으므로 장부의 닫는 중이 필요 없다.
     */
    private final OwnRequestLedger ledger;
    /** 대기 중인 스트림 가운데 상대가 연 것(아직 응답하지 않은 요청)의 번호와 요청 type. 응답에 같은 type을 쓴다(규격 1.3절, 4.1절). */
    private final Map<Long, Integer> peerPendingStreams = new LinkedHashMap<>();
    /**
     * AUTH 관문을 통과하기 전이라 미뤄 둔 PING(규격 3.3절). 미룬 요청은 {@link #ledger}가 번호 없는 요청으로 가진다. PING은 미룬 때의 다음
     * 번호표를 같이 적어 두어, 관문이 열릴 때 그 번호표 앞의 요청이 먼저 나가고 뒤의 요청이 나중에 나가게 한다.
     */
    private final Deque<DeferredPing> deferredPings = new ArrayDeque<>();

    /**
     * 받은 바이트 가운데 아직 한 단위(서문 또는 프레임)가 되지 못한 앞부분. 청크 경계에 걸친 단위 하나만 모으므로 비어 있는 때가 대부분이다.
     * 한 청크 안에 통째로 든 단위는 들어온 배열에서 바로 처리해 복사하지 않는다.
     */
    private byte[] pending = NO_BYTES;
    /** {@link #pending}에서 쓰인 바이트 수. 0이면 모은 것이 없다. */
    private int pendingLength;
    /** {@link #consumeUnit}이 0을 돌려줄 때, 진행하려면 단위의 앞에서부터 있어야 하는 총 바이트 수. */
    private int wantedLength;
    /** 입력 처리 중이다. 애플리케이션 콜백이 같은 엔드포인트에 다시 입력하는 것을 막는다. */
    private boolean receiving;

    /**
     * 연결의 단계: 수립과 관문(규격 3.3절), CLOSE 교환과 종료(규격 3.5절). 받아도 되는 프레임 타입, 송신을 미룰지, 새 요청을 열 수 있는지,
     * 지금 전송 계층을 닫아야 하는지, 연결이 끝났는지, 닫은 로컬 원인은 여기에 묻는다.
     */
    private final ConnectionPhase phase;
    /** 상대가 HELLO로 알린 {@code max_frame_size}. 알리지 않았으면 기본값이다(규격 3.2.1절). */
    private long peerMaxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;

    private long now;
    /** 지금까지 받은 가장 큰 경과 나노초. 역행을 막는 기준이다. */
    private long elapsedNanos;
    /** ACK를 기다리는 HELLO, AUTH, PING의 확인 대기, 서버의 AUTH 대기 기한, 유휴 PING 일정(규격 3.3절, 3.4.1절). */
    private final LivenessTimers timers;

    private final boolean server;
    /** 한 단위가 만들 수 있는 제어 프레임(HELLO 응답, AUTH, CLOSE 등)의 상한. */
    private static final int OUTPUT_CONTROL_RESERVE = 4;

    private boolean deferredAuthentication;
    private boolean authenticationPending;
    private boolean handlerReady;
    private Outbox.@Nullable Node selectedOutput;
    private @Nullable EndpointOutput currentOutput;
    private long nextOutputId;
    private int outputWritten;
    private long outputWaitId = LivenessTimers.NO_WAIT;
    private boolean incrementalOutput;
    private boolean outputDisconnected;
    /** 지금까지 쓰인 상대 공간 번호 중 가장 큰 것(규격 2.2.5절). */
    private long highestPeerStreamId;

    /** 조각으로 조립 중인 WebSocket 메시지가 열려 있는가. */
    private boolean messageOpen;

    private @Nullable WebSocketMessageKind messageKind;
    private byte[] messageBuffer = NO_BYTES;
    private int messageLength;
    /** 지금까지 본 메시지의 바이트 수. 모아 두지 않은 것도 센다. */
    private long messageTotal;
    /** 판정이 끝나 나머지 조각을 버리는 중인가. */
    private boolean messageDiscard;

    private boolean messageHeaderChecked;
    /** 헤더가 알린 프레임의 전체 크기({@code 4 + length}). 헤더를 확인한 뒤에만 의미가 있다. */
    private long messageExpected;

    private Endpoint(boolean server, EndpointConfig config) {
        this.config = config;
        this.timers = new LivenessTimers(config.ackTimeoutMs(), config.authTimeoutMs(), config.idleIntervalMs());
        long firstStreamId = config.firstStreamId(server ? FIRST_SERVER_STREAM_ID : FIRST_CLIENT_STREAM_ID);
        int ownParity = server ? 0 : 1;
        if ((firstStreamId & 1) != ownParity) {
            throw new IllegalArgumentException(
                    "nextStreamId " + firstStreamId + "는 " + (server ? "서버(짝수)" : "클라이언트(홀수)") + "의 번호 공간이 아니다");
        }
        this.ledger = new OwnRequestLedger(outbox, firstStreamId, ownParity);
        this.server = server;
        this.phase = server ? ConnectionPhase.server() : ConnectionPhase.client();
    }

    /**
     * 서버 역할 엔드포인트를 만든다. 연결 서문을 받으면 HELLO를 보낸다.
     *
     * @param config 엔드포인트의 설정
     * @return 연결 서문을 기다리는 엔드포인트
     * @throws IllegalArgumentException 설정의 nextStreamId가 역할의 번호 공간에 속하지 않을 때
     */
    public static Endpoint server(EndpointConfig config) {
        return new Endpoint(true, config);
    }

    /** 사용자 인증 판단을 사건으로 위임하는 내부 서버 엔드포인트. */
    public static Endpoint serverWithDeferredAuthentication(EndpointConfig config) {
        Endpoint endpoint = new Endpoint(true, config);
        endpoint.deferredAuthentication = true;
        return endpoint;
    }

    /**
     * 클라이언트 역할 엔드포인트를 만든다. 만들자마자 연결 서문과 HELLO를 보낼 바이트로 쌓는다.
     *
     * @param config 엔드포인트의 설정
     * @return 서버 HELLO를 기다리는 엔드포인트
     * @throws IllegalArgumentException 설정의 nextStreamId가 역할의 번호 공간에 속하지 않을 때
     */
    public static Endpoint client(EndpointConfig config) {
        Endpoint endpoint = new Endpoint(false, config);
        endpoint.send(Frames.preface());
        endpoint.sendHello();
        return endpoint;
    }

    /**
     * 상대에게서 받은 바이트를 넣는다. 프레임 경계와 무관하게 어느 위치에서 잘라도 된다. 이 호출 안에서 읽고 끝내며 {@code bytes}를 붙잡지
     * 않으므로, 돌아온 뒤에는 호출자가 배열을 다시 써도 된다.
     *
     * <p>입력을 처리하는 동안 애플리케이션 콜백({@code Authenticator})에서 같은 엔드포인트에 다시 입력할 수 없다. 오동작하지 않도록 상태를 건드리지
     * 않고 던진다. 콜백이 던진 예외는 엔드포인트가 받아 연결을 닫으므로 이 호출 밖으로 나오지 않는다.
     *
     * @param bytes 받은 바이트
     * @throws IllegalStateException WebSocket 엔드포인트일 때, 또는 입력을 처리하는 중에 다시 불렀을 때
     */
    public void receive(byte[] bytes) {
        if (config.isWebSocket()) {
            throw new IllegalStateException("WebSocket 엔드포인트는 receiveMessage로 입력한다");
        }
        if (!enterReceive()) {
            return;
        }
        try {
            receiveBytes(bytes);
        } finally {
            receiving = false;
        }
    }

    /** 최대 단위 수만큼 입력을 처리하고 소비한 바이트 수를 반환한다. 수거 공간에는 한도를 두지 않는다. */
    public int receiveBounded(byte[] bytes, int offset, int length, int maximumUnits) {
        return receiveBounded(bytes, offset, length, maximumUnits, CollectionSpace.UNLIMITED)
                .consumed();
    }

    /**
     * 최대 단위 수만큼, 그리고 한 단위가 만들 수 있는 사건·출력을 넣을 공간이 확보된 만큼만 입력을 처리한다. 단위를 처리하기 전에 최악의 경우 필요한
     * 공간을 확인하므로 상태가 절반만 바뀌지 않는다. 모자라면 그 단위 앞에서 멈추고 수거가 필요하다고 알린다. 호출자는 사건과 출력을 수거하고 돌려준
     * 소비 위치부터 다시 입력한다.
     *
     * @throws IllegalStateException 수거할 것이 하나도 없는데도 공간이 한 단위를 처리하기에 모자랄 때
     */
    public ReceiveProgress receiveBounded(
            byte[] bytes, int offset, int length, int maximumUnits, CollectionSpace space) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(space, "space");
        if (offset < 0 || length < 0 || offset > bytes.length - length || maximumUnits < 0) {
            throw new IllegalArgumentException("유효하지 않은 입력 범위 또는 처리 한도");
        }
        if (config.isWebSocket()) {
            throw new IllegalStateException("WebSocket 엔드포인트는 메시지 단위로 입력한다");
        }
        if (maximumUnits == 0 || !enterReceive()) {
            return new ReceiveProgress(0, false);
        }
        try {
            int at = offset;
            int end = offset + length;
            int units = 0;
            boolean collectionRequired = false;
            while (!phase.isEnded() && units < maximumUnits) {
                if (pendingLength == 0 && at == end) {
                    break;
                }
                if (!hasSpace(space)) {
                    collectionRequired = true;
                    break;
                }
                if (pendingLength > 0) {
                    int consumed = consumeUnit(pending, 0, pendingLength);
                    if (consumed > 0) {
                        releasePending();
                        units++;
                    } else if (phase.isEnded() || at == end) {
                        break;
                    } else {
                        int take = Math.min(wantedLength - pendingLength, end - at);
                        if (take <= 0) {
                            throw new IllegalStateException("더 필요한 바이트 수가 정해지지 않았다");
                        }
                        appendPending(bytes, at, take);
                        at += take;
                    }
                } else {
                    int consumed = consumeUnit(bytes, at, end - at);
                    if (consumed > 0) {
                        at += consumed;
                        units++;
                    } else {
                        if (!phase.isEnded()) {
                            appendPending(bytes, at, end - at);
                            at = end;
                        }
                        break;
                    }
                }
            }
            return new ReceiveProgress(at - offset, collectionRequired);
        } finally {
            receiving = false;
        }
    }

    /**
     * 한 단위가 최악의 경우 만들 수 있는 사건 수. 끝나는 내 요청마다 결과 하나에 종료 하나, 인증·받은 요청 사건 하나다. 전송 모듈이 수거 공간의 최소
     * 크기를 셈할 때도 이 값을 쓴다.
     *
     * @param ownRequests 열린 내 요청과 미뤄 둔 내 요청의 수
     * @return 최악의 사건 수
     */
    public static long eventsNeeded(long ownRequests) {
        return ownRequests + 2;
    }

    /**
     * 한 단위가 최악의 경우 만들 수 있는 출력 수. 미뤄 둔 것을 내보내거나 돌려보내는 프레임마다 하나에 제어 프레임 몇 개({@value
     * #OUTPUT_CONTROL_RESERVE})다. 전송 모듈이 수거 공간의 최소 크기를 셈할 때도 이 값을 쓴다.
     *
     * @param ownRequests 열린 내 요청과 미뤄 둔 내 요청의 수
     * @param deferredPings 미뤄 둔 PING 수
     * @param peerPendingRequests 응답을 기다리는 상대 요청 수
     * @return 최악의 출력 수
     */
    public static long outputsNeeded(long ownRequests, long deferredPings, long peerPendingRequests) {
        return ownRequests + deferredPings + peerPendingRequests + OUTPUT_CONTROL_RESERVE;
    }

    /**
     * 한 단위가 최악의 경우 만들 수 있는 사건·출력을 지금 쌓인 것에 더해도 한도 안인가. 사건은 끝나는 내 요청마다 결과 하나, 종료 하나,
     * 인증·받은 요청 사건 하나이고, 출력은 미뤄 둔 것을 내보내거나 돌려보내는 프레임마다 하나에 제어 프레임 몇 개다. 넉넉하게 잡은 상한이라 일찍 멈출 수는 있어도 넘치지는 않는다.
     */
    private boolean hasSpace(CollectionSpace space) {
        long ownRequests = (long) ledger.openCount() + ledger.queuedCount();
        long eventsNeeded = eventsNeeded(ownRequests);
        long outputsNeeded = outputsNeeded(ownRequests, deferredPings.size(), peerPendingStreams.size());
        if (events.size() + eventsNeeded <= space.events() && outbox.size() + outputsNeeded <= space.outputs()) {
            return true;
        }
        if (events.isEmpty() && outbox.isEmpty()) {
            throw new IllegalStateException("수거 공간이 한 단위를 처리하기에 모자라다");
        }
        return false;
    }

    /**
     * 입력 처리에 들어간다. 이미 처리 중이면(애플리케이션 콜백이 다시 입력함) 상태를 건드리지 않고 던진다. 연결이 끝났으면 입력을 받지 않는다.
     *
     * @return 입력을 처리해도 되는가
     */
    private boolean enterReceive() {
        if (receiving) {
            throw new IllegalStateException("입력을 처리하는 중에 같은 엔드포인트에 다시 입력할 수 없다");
        }
        if (phase.isEnded()) {
            return false;
        }
        receiving = true;
        return true;
    }

    /**
     * 받은 바이트를 처리한다. 먼저 경계에 걸친 단위를 모아서 끝내고, 그 뒤로는 들어온 배열에서 바로 처리한다. 남은 꼬리만 복사해 둔다.
     * 어디서 잘라 넣어도 {@link #consumeUnit}이 보는 바이트는 같으므로 결과가 같다.
     */
    private void receiveBytes(byte[] bytes) {
        int at = 0;
        int end = bytes.length;
        while (pendingLength > 0) {
            int consumed = consumeUnit(pending, 0, pendingLength);
            if (phase.isEnded()) {
                return;
            }
            if (consumed > 0) {
                // 모은 바이트는 정확히 한 단위다. wantedLength는 처리 중인 단위의 크기를 넘지 않고, 그만큼까지만 모으기 때문이다.
                releasePending();
                continue;
            }
            if (at == end) {
                return;
            }
            // 판정에 필요한 만큼만 더 모은다. 헤더가 알린 길이만큼 미리 잡지 않는다(상대가 정하는 값이다).
            if (wantedLength <= pendingLength) {
                throw new IllegalStateException("더 필요한 바이트 수가 정해지지 않았다");
            }
            int take = Math.min(wantedLength - pendingLength, end - at);
            appendPending(bytes, at, take);
            at += take;
        }
        while (!phase.isEnded() && at < end) {
            int consumed = consumeUnit(bytes, at, end - at);
            if (consumed == 0) {
                break;
            }
            at += consumed;
        }
        if (!phase.isEnded() && at < end) {
            appendPending(bytes, at, end - at);
        }
    }

    private void appendPending(byte[] bytes, int at, int length) {
        int needed = pendingLength + length;
        if (needed > pending.length) {
            // 받은 만큼에 맞춰 두 배씩 키운다. 상한은 처리 중인 단위의 크기(wantedLength, 최대 max_frame_size)다.
            int doubled = Math.max(PENDING_MIN_CAPACITY, pending.length * 2);
            pending = Arrays.copyOf(pending, Math.max(needed, Math.min(doubled, wantedLength)));
        }
        System.arraycopy(bytes, at, pending, pendingLength, length);
        pendingLength = needed;
    }

    /** 모은 바이트를 모두 처리했다. 큰 버퍼는 놓는다. */
    private void releasePending() {
        pendingLength = 0;
        if (pending.length > PENDING_RELEASE_ABOVE) {
            pending = NO_BYTES;
        }
    }

    /**
     * WebSocket 메시지 하나를 넣는다. 서문은 첫 바이너리 메시지, 그 뒤로는 메시지 하나가 프레임 하나다(규격 1.2절). 바이너리가 아닌
     * 메시지, 크기가 {@code 4 + length}가 아닌 메시지는 연결 오류다. type이 CLOSE이면 크기가 어긋나도 받은 CLOSE로 본다.
     *
     * <p>입력을 처리하는 동안 애플리케이션 콜백에서 같은 엔드포인트에 다시 입력할 수 없다({@link #receive(byte[])}와 같다).
     *
     * @param kind 메시지 종류
     * @param message 메시지 내용
     * @throws IllegalStateException 바이트 스트림 엔드포인트일 때, 또는 입력을 처리하는 중에 다시 불렀을 때
     */
    public void receiveMessage(WebSocketMessageKind kind, byte[] message) {
        if (!config.isWebSocket()) {
            throw new IllegalStateException("바이트 스트림 엔드포인트는 receive로 입력한다");
        }
        if (!enterReceive()) {
            return;
        }
        try {
            receiveWebSocketMessage(kind, message);
        } finally {
            receiving = false;
        }
    }

    private void receiveWebSocketMessage(WebSocketMessageKind kind, byte[] message) {
        if (phase.awaitingPreface()) {
            if (kind == WebSocketMessageKind.BINARY
                    && message.length == Frames.preface().length
                    && matchesPrefacePrefix(message, 0, message.length)) {
                phase.prefaceVerified();
                sendHello();
            } else {
                closeWithoutClose();
            }
            return;
        }
        if (phase.awaitingFirstFrame() && !checkFirstMessage(kind, message, message.length)) {
            closeWithoutClose();
            return;
        }
        if (kind != WebSocketMessageKind.BINARY || message.length < Frames.TYPE_END) {
            connectionError();
            return;
        }
        long length = Frames.readUnsignedInt(message, 0);
        if (length < Frames.MIN_LENGTH
                || length > config.maxFrameSize() - Frames.LENGTH_FIELD_SIZE
                || message.length != Frames.LENGTH_FIELD_SIZE + length) {
            rejectBadLength(message, 0);
            return;
        }
        handle(message, 0, message.length);
    }

    /**
     * WebSocket 메시지 하나의 조각을 넣는다. 메시지의 종류와 시작·끝을 보존하며, 메시지 전체를 전송 모듈이 미리 모을 필요가 없다. 메시지 끝에서
     * 정확한 길이를 검증한 뒤에만 프레임의 효과(요청 전달, 결과 확정 등)를 적용한다(규격 1.2절). 조립 공간은 프레임 크기 상한
     * ({@code max_frame_size})을 넘지 않는다. 프레임 헤더가 상한을 넘는 길이를 알리거나 메시지가 알린 길이를 넘으면 끝을 기다리지 않고 오류로
     * 처리하며, 그 메시지의 나머지 조각은 모으지 않고 버린다. 텍스트 메시지는 시작에서 오류로 처리한다.
     *
     * <p>이 호출 안에서 읽고 끝내며 {@code bytes}를 붙잡지 않는다. 사건·출력 공간이 모자라면 아무것도 소비하지 않고 수거가 필요하다고 알린다.
     * 호출자는 수거한 뒤 같은 조각(같은 {@code start}·{@code end})을 다시 넣는다. 소비는 조각 단위이며 조각 안에서 일부만 소비하지 않는다.
     * 연결이 이미 끝났으면 아무것도 소비하지 않는다.
     *
     * @param kind 메시지 종류. 한 메시지의 모든 조각에서 같아야 한다
     * @param start 메시지의 첫 조각이면 참
     * @param end 메시지의 마지막 조각이면 참
     * @param bytes 조각을 담은 배열
     * @param offset 조각의 시작 위치
     * @param length 조각의 길이
     * @param space 수거 전에 쌓아 둘 수 있는 사건·출력의 한도
     * @return 소비한 바이트 수와 수거 필요 여부
     * @throws IllegalArgumentException 범위가 유효하지 않거나 메시지 안에서 종류가 바뀔 때
     * @throws IllegalStateException 바이트 스트림 엔드포인트일 때, 시작 없이 이어지거나 열린 메시지 안에서 다시 시작할 때, 입력을 처리하는 중에
     *     다시 불렀을 때, 수거할 것이 하나도 없는데도 공간이 모자랄 때
     */
    public ReceiveProgress receiveMessageFragment(
            WebSocketMessageKind kind,
            boolean start,
            boolean end,
            byte[] bytes,
            int offset,
            int length,
            CollectionSpace space) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(space, "space");
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IllegalArgumentException("유효하지 않은 입력 범위");
        }
        if (!config.isWebSocket()) {
            throw new IllegalStateException("바이트 스트림 엔드포인트는 receive로 입력한다");
        }
        if (!enterReceive()) {
            dropMessage();
            return new ReceiveProgress(0, false);
        }
        try {
            if (start == messageOpen) {
                throw new IllegalStateException(start ? "열린 메시지 안에서 다시 시작할 수 없다" : "시작 없이 조각이 이어질 수 없다");
            }
            if (!start && kind != messageKind) {
                throw new IllegalArgumentException("한 메시지 안에서 종류가 바뀔 수 없다");
            }
            if (!hasSpace(space)) {
                return new ReceiveProgress(0, true);
            }
            if (start) {
                messageOpen = true;
                messageKind = kind;
                messageLength = 0;
                messageTotal = 0;
                messageDiscard = false;
                messageHeaderChecked = false;
            }
            if (!messageDiscard) {
                if (kind == WebSocketMessageKind.TEXT) {
                    receiveWebSocketMessage(kind, NO_BYTES);
                    discardMessage();
                } else {
                    takeBinaryFragment(bytes, offset, length);
                }
            }
            if (end) {
                if (!messageDiscard) {
                    finishMessage();
                }
                dropMessage();
            }
            return new ReceiveProgress(length, false);
        } finally {
            receiving = false;
        }
    }

    /** 바이너리 조각을 조립한다. 판정할 수 있게 되면 끝을 기다리지 않고 오류로 처리한다. 모으는 바이트는 판정에 필요한 만큼으로 한정한다. */
    private void takeBinaryFragment(byte[] bytes, int offset, int length) {
        int at = offset;
        int end = offset + length;
        messageTotal += length;
        if (phase.awaitingPreface()) {
            if (messageTotal > Frames.preface().length) {
                closeWithoutClose();
                discardMessage();
                return;
            }
            appendMessage(bytes, at, length);
            return;
        }
        if (!messageHeaderChecked) {
            int take = Math.min(end - at, Frames.HEADER_SIZE - messageLength);
            appendMessage(bytes, at, take);
            at += take;
            if (messageLength < Frames.HEADER_SIZE) {
                return;
            }
            if (!inspectMessageHeader()) {
                discardMessage();
                return;
            }
        }
        if (messageTotal > messageExpected) {
            rejectBadLength(messageBuffer, 0);
            discardMessage();
            return;
        }
        appendMessage(bytes, at, end - at);
    }

    /**
     * 헤더가 모였다. 첫 프레임 검사(규격 3.2.4절)와 {@code length} 범위를 확인한다.
     *
     * @return 계속 모아도 되면 참
     */
    private boolean inspectMessageHeader() {
        if (phase.awaitingFirstFrame()
                && !checkFirstMessage(WebSocketMessageKind.BINARY, messageBuffer, messageLength)) {
            closeWithoutClose();
            return false;
        }
        long length = Frames.readUnsignedInt(messageBuffer, 0);
        if (length < Frames.MIN_LENGTH || length > config.maxFrameSize() - Frames.LENGTH_FIELD_SIZE) {
            rejectBadLength(messageBuffer, 0);
            return false;
        }
        messageExpected = Frames.LENGTH_FIELD_SIZE + length;
        messageHeaderChecked = true;
        return true;
    }

    /** 마지막 조각까지 모였다. 정확한 길이를 확인한 뒤에야 프레임의 효과를 적용한다. */
    private void finishMessage() {
        if (!messageHeaderChecked) {
            receiveWebSocketMessage(WebSocketMessageKind.BINARY, Arrays.copyOf(messageBuffer, messageLength));
        } else if (messageTotal == messageExpected) {
            handle(messageBuffer, 0, messageLength);
        } else {
            rejectBadLength(messageBuffer, 0);
        }
    }

    private void appendMessage(byte[] bytes, int at, int length) {
        if (length == 0) {
            return;
        }
        int needed = messageLength + length;
        if (needed > messageBuffer.length) {
            int grown = Math.max(PENDING_MIN_CAPACITY, messageBuffer.length * 2);
            // 조립 공간은 프레임 크기 상한을 넘기지 않는다.
            int cap = (int) Math.min(config.maxFrameSize(), Integer.MAX_VALUE);
            messageBuffer = Arrays.copyOf(messageBuffer, Math.max(needed, Math.min(grown, cap)));
        }
        System.arraycopy(bytes, at, messageBuffer, messageLength, length);
        messageLength = needed;
    }

    /** 이 메시지의 나머지 조각은 모으지 않고 버린다. */
    private void discardMessage() {
        messageDiscard = true;
        messageLength = 0;
        messageBuffer = NO_BYTES;
    }

    /** 조립 중인 메시지를 놓는다. */
    private void dropMessage() {
        messageOpen = false;
        messageDiscard = false;
        messageHeaderChecked = false;
        messageLength = 0;
        messageTotal = 0;
        if (messageBuffer.length > PENDING_RELEASE_ABOVE) {
            messageBuffer = NO_BYTES;
        }
    }

    /** 조립 중인 메시지에 모아 둔 바이트 수. */
    int messageAssemblyLength() {
        return messageLength;
    }

    /**
     * WebSocket 출력 하나가 WebSocket 계층에 메시지 전체로 접수됐음을 보고한다. 이 호출이 쓰기 시작과 전체 전달을 함께 뜻한다. 접수 전에는 철회할
     * 수 있고 접수 뒤에는 메시지를 유지한 채 필요한 RESET을 뒤에 보낸다. 전송 모듈 내부 대기열에 넣은 것, 소켓에 쓴 것, TLS가 평문을 소비한 것은
     * 접수가 아니다. 상대의 수신·처리를 보장하지 않는다. 전송 모듈은 {@link #nextOutput()}으로 확인한 뒤 이 호출까지 같은 엔드포인트의 다른 명령을
     * 처리하지 않아야 한다.
     *
     * @param id {@link #nextOutput()}이 돌려준 출력의 식별자
     * @throws IllegalStateException WebSocket 엔드포인트가 아닐 때
     * @throws IllegalArgumentException 식별자가 현재 출력의 것이 아닐 때(철회·교체·이미 접수한 출력)
     */
    public void outputAccepted(long id) {
        if (!config.isWebSocket()) {
            throw new IllegalStateException("바이트 스트림 엔드포인트는 outputProgress로 보고한다");
        }
        EndpointOutput output = nextOutput();
        if (output == null || output.id() != id) {
            throw new IllegalArgumentException("유효하지 않은 출력 접수");
        }
        startOutput();
        timers.outputProgressed(now);
        completeOutput();
    }

    /**
     * WebSocket Close를 받았거나 WebSocket이 닫혔음을 알린다. 전송 종료 입력이다. 상태 코드가 1000이어도 Strand {@code CLOSE}나 {@code
     * last_stream_id}를 만들지 않으며, 응답·RESET을 받지 못한 요청은 메시지를 접수했는지에 따라 결과 모름 또는 처리 안 됨으로 확정한다(규격 3.5절).
     * 요청 결과는 종료 사건보다 먼저 남는다. WebSocket 닫기 응답과 실제 통로 정리는 전송 모듈이 맡는다. 조립 중이던 메시지는 효과 없이 버린다. 이미
     * 끝났다면 아무것도 하지 않는다.
     *
     * @param statusCode 받은 WebSocket 상태 코드(0~65535). 결과에 영향을 주지 않는다
     * @throws IllegalStateException WebSocket 엔드포인트가 아닐 때
     * @throws IllegalArgumentException 상태 코드가 범위 밖일 때
     */
    public void webSocketClosed(int statusCode) {
        if (!config.isWebSocket()) {
            throw new IllegalStateException("바이트 스트림 엔드포인트는 disconnect로 알린다");
        }
        if (statusCode < 0 || statusCode > MAX_WEBSOCKET_STATUS_CODE) {
            throw new IllegalArgumentException("WebSocket 상태 코드는 0~65535여야 한다: " + statusCode);
        }
        disconnect();
        dropMessage();
    }

    /** 전송 모듈이 연결을 어떻게 닫아야 하는지에 대한 core의 지시. Strand CLOSE·WebSocket Close 메시지와는 별개다. */
    public enum TransportClose {
        /** 연결이 끝나지 않았다. 닫지 않는다. */
        NONE,
        /** 연결이 끝났다. 남은 출력(Strand CLOSE, WebSocket Close 포함)을 처리한 뒤 닫는다. */
        AFTER_PENDING_OUTPUT,
        /** 연결이 끝났고 보낼 것이 없거나 보낼 수 없다. 기다리지 않고 닫는다(EOF·쓰기 실패·abort 포함). */
        IMMEDIATE
    }

    /**
     * 전송 종료 지시. 연결이 끝나지 않았으면 {@link TransportClose#NONE}이다. 끝났으면 아직 처리하지 못한 출력이 남았는지에 따라 {@link
     * TransportClose#AFTER_PENDING_OUTPUT} 또는 {@link TransportClose#IMMEDIATE}다. 연결이 끊긴 뒤의 출력은 전송할 수 없으므로 기다리지 않는다.
     * 지시는 실제 통로 정리 완료가 아니다. 완료는 전송 모듈이 관찰한다.
     *
     * @return 지시
     */
    public TransportClose transportClose() {
        if (!phase.isEnded()) {
            return TransportClose.NONE;
        }
        boolean outputPending = !outputDisconnected && (outputWritten > 0 || outbox.size() > 0);
        return outputPending ? TransportClose.AFTER_PENDING_OUTPUT : TransportClose.IMMEDIATE;
    }

    /**
     * 가상 시계를 앞으로 보낸다. 확인 대기 시간이 지나도록 ACK가 없으면 연결 오류로 닫는다.
     *
     * @param ms 음수가 아닌 경과 밀리초
     * @throws IllegalArgumentException 경과 시간이 음수일 때
     */
    public void advance(long ms) {
        if (ms < 0) {
            throw new IllegalArgumentException("경과 시간은 음수일 수 없다: " + ms);
        }
        if (phase.isEnded()) {
            return;
        }
        now = Deadlines.plus(now, ms);
        elapsedNanos = Math.max(elapsedNanos, Deadlines.millisToNanos(now));
        LivenessTimers.Expiry expiry = timers.expiry(now);
        if (expiry == LivenessTimers.Expiry.UNAUTHENTICATED) {
            connectionError(ErrorCode.UNAUTHENTICATED);
            return;
        }
        if (expiry == LivenessTimers.Expiry.UNRESPONSIVE) {
            connectionError(ErrorCode.UNRESPONSIVE);
            return;
        }
        expireRequests();
        byte[] idlePing = phase.mayIdlePing() ? timers.idlePing(now) : null;
        if (idlePing != null) {
            // 넘기지 못한 유휴 PING의 기한(규격 3.3절): 쓰기 대기열에 든 때부터 센다.
            timers.idlePingQueued(sendPing(idlePing), now);
        }
    }

    /**
     * 연결을 시작한 때부터 흐른 나노초로 시계를 옮긴다. 같은 값은 허용하고 처리할 것이 없으면 아무 일도 하지 않는다. 시계는 밀리초 단위로 처리하므로
     * 밀리초 미만은 버린다.
     *
     * @param elapsedNanos 연결을 시작한 때부터 흐른, 이전 값보다 작지 않은 나노초
     * @throws IllegalArgumentException 음수이거나 이전에 준 값보다 작을 때. 상태를 바꾸지 않는다
     */
    public void advanceTo(long elapsedNanos) {
        if (elapsedNanos < 0 || elapsedNanos < this.elapsedNanos) {
            throw new IllegalArgumentException("경과 나노초는 음수이거나 이전 값보다 작을 수 없다: " + elapsedNanos);
        }
        long target = elapsedNanos / Deadlines.NANOS_PER_MILLI;
        long delta = target - now;
        this.elapsedNanos = elapsedNanos;
        if (delta > 0) {
            advance(delta);
        }
    }

    /**
     * {@link #nextDeadline()}를 경과 나노초로 돌려준다. 넘치면 {@link Long#MAX_VALUE}에서 멈춘다. 기한이 없으면 비어 있다.
     *
     * @return 시계를 옮겨야 하는 가장 이른 경과 나노초
     */
    public OptionalLong nextDeadlineNanos() {
        OptionalLong millis = nextDeadline();
        return millis.isPresent() ? OptionalLong.of(Deadlines.millisToNanos(millis.getAsLong())) : millis;
    }

    /**
     * 상대가 HELLO로 알린 {@code max_frame_size}. 알리지 않았으면(상대 HELLO를 받기 전 포함) 기본값이다. 이 값은 엔드포인트가 보낼 프레임의 크기 검사에 쓰는 값과 같다. 전송 계층이 쓰기 대기열의 정상 흐름 제어 범위를 셈할
     * 때 쓴다.
     *
     * @return 상대가 알린 최대 프레임 크기(바이트)
     */
    public long peerMaxFrameSize() {
        return peerMaxFrameSize;
    }

    /**
     * 상대가 HELLO로 알린 {@code max_concurrent_streams}. 내가 낼 수 있는 동시 요청의 한도다. 알리기 전에는 기본값이다(규격 3.2.1절).
     *
     * @return 상대가 알린 동시 스트림 한도
     */
    public long peerMaxConcurrentStreams() {
        return ledger.peerMaxConcurrentStreams();
    }

    /**
     * 내가 HELLO로 알린 {@code max_concurrent_streams}. 상대가 동시에 열 수 있는 요청의 한도다.
     *
     * @return 내가 알린 동시 스트림 한도
     */
    public long ownMaxConcurrentStreams() {
        return config.maxConcurrentStreams();
    }

    /**
     * 시계를 다음으로 진행해야 하는 가장 이른 가상 시각(마지막 {@link #advance(long)}까지 흐른 시각부터 센 절대 시각)을 돌려준다. 대상은 요청 기한,
     * 확인 대기(HELLO, AUTH, PING), 서버의 AUTH 대기, 유휴 PING이다. 이 시각까지 {@code advance}하면 그 기한이 처리된다. 기한은 정각이 지나야
     * 처리되므로 기한 다음 밀리초를 돌려준다. 아무 기한도 없거나 연결이 끝났으면 비어 있다.
     */
    public OptionalLong nextDeadline() {
        if (phase.isEnded()) {
            return OptionalLong.empty();
        }
        OptionalLong requests = ledger.nextExpiryAt();
        OptionalLong liveness = timers.nextAt(phase.mayIdlePing());
        if (!requests.isPresent()) {
            return liveness;
        }
        if (!liveness.isPresent()) {
            return requests;
        }
        return OptionalLong.of(Math.min(requests.getAsLong(), liveness.getAsLong()));
    }

    /**
     * 애플리케이션이 요청을 보내게 한다. AUTH 관문을 통과하기 전이면 그 뒤로 미룬다(규격 3.3절). CLOSE를 보냈거나 받은 뒤,
     * 번호 소진(규격 2.2.5절), 상대가 알린 {@code max_concurrent_streams} 도달(규격 3.2.1절)이면 프레임을 보내지 않고 로컬에서
     * 실패시킨다. 이때 스트림 번호 없는 {@link Event.Kind#RESULT} 이벤트(status {@code UNAVAILABLE})가 한 번 나온다. 한도에 닿은
     * 요청은 대기시키지 않고 즉시 실패시키며 스트림 번호를 쓰지 않는다.
     *
     * @param type 애플리케이션 {@code type}(256 이상)
     * @param payload 본문
     * @return 요청 번호표(낸 순서, 0부터). 로컬에서 실패한 요청에도 번호표가 하나 쓰인다. 크기 검사로 던진 요청은 번호표를 쓰지 않는다. {@link #requestState(long)}가 이 번호표로 요청을 찾는다.
     * @throws IllegalArgumentException 지금 보낼 수 있는 요청의 프레임이 상대가 알린 {@code max_frame_size}를 넘을 때. 규격은 그런
     *     프레임을 보내지 못하게 하고(MUST NOT, 3.2.1절) 로컬 처리를 정하지 않으므로, 아무것도 보내지 않고 스트림 번호도 쓰지 않는다.
     *     AUTH 관문 전이라 미룬 요청은 호출자가 돌아간 뒤에 보내므로 던지지 않고, 보낼 때 넘으면 로컬 {@code UNAVAILABLE}로
     *     실패시킨다.
     */
    public long request(int type, byte[] payload) {
        return requestUntil(type, payload, null);
    }

    /**
     * 요청 확장 영역(규격 2.4절)이 있는 요청을 보낸다. 그 밖의 동작은 {@link #request(int, byte[])}, {@link #request(int, byte[], long)}와
     * 같다. 확장 영역은 프레임 크기 검사에 포함된다.
     *
     * @param extensions 항목(tag 2바이트, length 2바이트, value)이 이어진 확장 영역. 없으면 빈 배열
     * @param deadlineMs 지금부터 센 기한(밀리초). 0이면 기한이 없다
     * @return 요청 번호표
     * @throws IllegalArgumentException 확장 영역이 형식이나 알려진 항목의 값 규칙(규격 2.4절)을 어기거나 8192바이트를 넘을 때, 또는 {@code
     *     deadlineMs}가 음수일 때. 아무 상태도 바꾸지 않고 번호표도 쓰지 않는다
     */
    public long request(int type, byte[] extensions, byte[] payload, long deadlineMs) {
        if (deadlineMs < 0) {
            throw new IllegalArgumentException("deadlineMs는 음수일 수 없다: " + deadlineMs);
        }
        if (extensions.length > MAX_EXTENSIONS_LENGTH
                || scanExtensions(extensions, 0, extensions.length, 0) != Extensions.VALID) {
            throw new IllegalArgumentException("요청 확장 영역이 규격 2.4절을 어긴다");
        }
        return requestUntil(type, extensions, payload, deadlineMs == 0 ? null : Deadlines.plus(now, deadlineMs));
    }

    private long requestUntil(int type, byte[] payload, @Nullable Long deadlineAt) {
        return requestUntil(type, NO_BYTES, payload, deadlineAt);
    }

    private long requestUntil(int type, byte[] extensions, byte[] payload, @Nullable Long deadlineAt) {
        ConnectionPhase.ApplicationSend send = phase.applicationSend();
        if (send == ConnectionPhase.ApplicationSend.DEFER) {
            return ledger.queue(type, extensions, payload, deadlineAt);
        }
        if (!phase.isActive()) {
            long ticket = ledger.issueTicket();
            failLocally(ticket);
            return ticket;
        }
        // 크기 검사에 걸리면 던지고 번호표를 쓰지 않는다.
        long ticket = ledger.nextTicket();
        if (refuseLocally(ticket, ledger.open(type, extensions, payload, deadlineAt, peerMaxFrameSize))
                == OwnRequestLedger.Admission.OVERSIZE) {
            throw oversizeFrame("요청");
        }
        return ticket;
    }

    /**
     * 장부가 번호를 주지 않은 요청(프레임 크기 초과는 제외)을 로컬에서 실패시킨다. 번호 소진이면 연결도 닫는다(규격 2.2.5절). 프레임이 너무 크면
     * 아무것도 하지 않고 돌려준다.
     */
    private OwnRequestLedger.Admission refuseLocally(long ticket, OwnRequestLedger.Admission admission) {
        switch (admission) {
            case CLOSING:
            case STREAM_LIMIT:
                failLocally(ticket);
                break;
            case EXHAUSTED:
                // 프레임 없이 로컬에서 실패시키고 연결을 닫는다(규격 2.2.5절).
                failLocally(ticket);
                close(ErrorCode.NO_ERROR);
                break;
            default:
                break;
        }
        return admission;
    }

    /**
     * 요청 기한을 두고 요청을 보낸다(규격 4.4.1절). 기한은 로컬 설정이고, 기한이 지난 뒤 {@link #advance(long)}가 요청을 끝낸다. 쓰기 전(번호
     * 유무 무관)이면 {@code RESET} 없이 프레임을 버리고 {@code DEADLINE_EXCEEDED}(처리 안 됨), 쓰기를 시작했으면 {@code RESET(CANCEL)}을
     * 보내고 {@code DEADLINE_EXCEEDED}(결과 모름)를 넘긴다. 요청 확장 영역의 {@code timeout}은 받는 쪽에 주는 힌트일 뿐 이 기한이 아니다.
     *
     * <p>"지금"은 실제 시각이 아니라 마지막 {@link #advance(long)}까지 흐른 가상 시계다. 이 엔드포인트는 실제 시계를 읽지 않으므로, 호출자는 기한이
     * 있는 요청을 보내기 직전에 {@code advance}로 시계를 맞춘다. {@code AUTH} 대기와 확인 대기의 기준 시각도 같은 가상 시계다.
     *
     * @param deadlineMs 지금부터 센 기한(밀리초, 1 이상)
     * @throws IllegalArgumentException {@code deadlineMs}가 1보다 작을 때
     * @see #request(int, byte[])
     */
    public long request(int type, byte[] payload, long deadlineMs) {
        if (deadlineMs < 1) {
            throw new IllegalArgumentException("deadlineMs는 1 이상이어야 한다: " + deadlineMs);
        }
        return requestUntil(type, payload, Deadlines.plus(now, deadlineMs));
    }

    /**
     * 번호표로 요청을 취소한다. 번호 없는 대기열 요청도 취소할 수 있다(규격 4.4절). 결과는 {@link #cancel(long)}과 같다. 끝난 요청이나 낸 적 없는
     * 번호표면 아무것도 하지 않는다.
     *
     * @param ticket {@link #request}가 돌려준 번호표
     */
    public void cancelRequest(long ticket) {
        endOwnRequest(ledger.cancelTicket(ticket));
    }

    /**
     * 애플리케이션이 상대의 요청에 응답하게 한다. 스트림이 끝나고 종료 중이었다면 닫는다. 대기 중이 아닌 스트림이면 아무것도 하지 않는다.
     *
     * @param streamId 상대가 연 스트림 번호
     * @param status 규격 2.5.1절의 status 값
     * @param payload 본문
     * @return 응답을 쌓았으면 true. 대기 중인 스트림이 아니어서(이미 응답했거나, 상대가 RESET했거나, 연결이 끝나서) 아무것도 하지 않았으면 false
     * @throws IllegalArgumentException 프레임이 상대가 알린 {@code max_frame_size}를 넘을 때. 규격은 그런 프레임을 보내지 못하게 하고
     *     (MUST NOT, 3.2.1절) 로컬 처리를 정하지 않으므로, 아무것도 보내지 않고 스트림도 그대로 둔다. 호출자는 줄인 payload로 다시 응답할 수 있다.
     */
    public boolean respond(long streamId, int status, byte[] payload) {
        Integer type = peerPendingStreams.get(streamId);
        if (type == null) {
            return false;
        }
        byte[] frame = Frames.response(type, streamId, status, payload);
        if (exceedsPeerFrameSize(frame)) {
            throw oversizeFrame("응답");
        }
        peerPendingStreams.remove(streamId);
        send(frame);
        conclude();
        return true;
    }

    /**
     * 애플리케이션이 받은 요청을 핸들러 시작 전에 거절하게 한다. {@code RESET(REFUSED)}를 보내고 스트림을 끝낸다(규격 4.5절). 대기 중이 아닌
     * 스트림이면 아무것도 하지 않는다. 끝낸 스트림에는 응답할 수 없다.
     *
     * @param streamId 상대가 연 스트림 번호
     */
    public void refuseRequest(long streamId) {
        if (peerPendingStreams.remove(streamId) == null) {
            return;
        }
        send(Frames.reset(streamId, ErrorCode.REFUSED));
        conclude();
    }

    /**
     * 애플리케이션이 자기 요청을 취소하게 한다. RESET(CANCEL)을 보내고 로컬 {@code CANCELLED} 결과를 넘긴다(규격 4.4절). 대기 중이
     * 아닌 스트림이면 아무것도 하지 않는다.
     *
     * @param streamId 내가 연 스트림 번호
     */
    public void cancel(long streamId) {
        endOwnRequest(ledger.cancelStream(streamId, Status.CANCELLED));
    }

    /** 기한이 지난 요청을 끝낸다(규격 4.4.1절). 순서는 장부가 정한다. 번호를 받은 요청이 끝났으면 종료 중인 연결을 닫을 수 있는지 본다. */
    private void expireRequests() {
        if (ledger.expire(now, this::applyOutcome)) {
            conclude();
        }
    }

    /**
     * 장부가 끝낸 내 요청 하나의 결과를 반영하고, 종료 중이고 마지막 스트림이었다면 닫는다(규격 3.5절). 번호를 받지 못한 요청의 결과는 스트림이
     * 아니므로 닫는 판단을 하지 않는다. 대기 중이 아니라 장부가 null을 돌려줬으면 아무것도 하지 않는다.
     */
    private void endOwnRequest(OwnRequestLedger.@Nullable Outcome outcome) {
        if (outcome == null) {
            return;
        }
        applyOutcome(outcome);
        if (outcome.numbered()) {
            // 번호 없는 요청은 대기 중인 스트림이 아니라서 닫을 수 있게 되는 일이 없다.
            conclude();
        }
    }

    /** 장부가 돌려준 결과를 반영한다: 쓰기를 시작한 요청이면 {@code RESET(CANCEL)}을 보내고 결과 이벤트를 낸다. */
    private void applyOutcome(OwnRequestLedger.Outcome outcome) {
        if (outcome.resetToSend) {
            send(Frames.reset(outcome.streamId, ErrorCode.CANCEL));
        }
        events.add(Event.result(outcome.ticket, outcome.streamId, outcome.status, outcome.certainty, outcome.body));
    }

    /**
     * 애플리케이션이 PING을 보내게 한다. AUTH 관문을 통과하기 전이면 그 뒤로 미룬다(규격 3.3절).
     *
     * @param payload 정확히 8바이트
     * @throws IllegalArgumentException payload가 8바이트가 아닐 때
     */
    public void ping(byte[] payload) {
        if (payload.length != PING_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("PING payload는 정확히 8바이트여야 한다: " + payload.length);
        }
        ConnectionPhase.ApplicationSend send = phase.applicationSend();
        if (send == ConnectionPhase.ApplicationSend.REFUSE) {
            return;
        }
        byte[] copy = payload.clone();
        if (send == ConnectionPhase.ApplicationSend.DEFER) {
            deferredPings.addLast(new DeferredPing(copy, ledger.nextTicket()));
        } else {
            sendPing(copy);
        }
    }

    /**
     * 애플리케이션이 연결을 닫게 한다. 이미 CLOSE를 보냈거나 받았다면 아무것도 하지 않는다(규격 3.5절).
     *
     * @param errorCode 규격 2.5.2절의 error_code 값
     */
    public void close(int errorCode) {
        close(errorCode, "");
    }

    /**
     * 애플리케이션이 UTF-8 설명과 함께 연결을 닫게 한다. 프레임 상한에 맞춰 reason을 자른다.
     *
     * @param errorCode 규격 2.5.2절의 error_code 값
     * @param reason non-null UTF-8 close reason; 빈 문자열은 허용한다
     * @throws NullPointerException reason이 {@code null}일 때
     */
    public void close(int errorCode, String reason) {
        Objects.requireNonNull(reason, "reason");
        if (!phase.isActive()) {
            return;
        }
        if (errorCode != ErrorCode.NO_ERROR) {
            refuseUnhandedRequests();
        }
        long maxFrameSize = phase.hasPeerHello() ? peerMaxFrameSize : EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT;
        int reasonCapacity = (int) Math.max(0, maxFrameSize - Frames.HEADER_SIZE - Frames.CLOSE_FIXED_PAYLOAD_SIZE);
        send(Frames.close(errorCode, highestPeerStreamId, utf8Prefix(reason, reasonCapacity)));
        phase.closeSent(errorCode);
        endQueuedRequests();
        conclude();
    }

    /** 인증기가 자격 증명의 만료를 알린다. 재인증 없이 정상 CLOSE로 연결을 마친다(규격 3.3.1절). */
    public void credentialExpired() {
        closeForCredentialEnd(ErrorCode.NO_ERROR);
    }

    /** 인증기가 자격 증명의 폐기를 알린다. 사유 없는 UNAUTHENTICATED CLOSE로 연결을 끝낸다(규격 3.3.1절). */
    public void credentialRevoked() {
        closeForCredentialEnd(ErrorCode.UNAUTHENTICATED);
    }

    private void closeForCredentialEnd(int errorCode) {
        if (!server || !phase.hasPassedAuthGate()) {
            throw new IllegalStateException("자격 증명 종료 통지는 서버의 AUTH 관문 통과 뒤에만 가능하다");
        }
        if (!phase.isActive()) {
            return;
        }
        close(errorCode);
    }

    /**
     * 전송 계층이 CLOSE 없이 끊겼음을 알리는 입력이다(TCP EOF·RST, WebSocket close). 대기 중인 요청을 로컬 UNAVAILABLE로 끝내고 닫는다.
     * 이미 닫혔다면 아무것도 하지 않는다(규격 3.5절).
     */
    public void disconnect() {
        phase.transportDisconnected();
        conclude();
        outputDisconnected = true;
        if (incrementalOutput) {
            selectedOutput = null;
            currentOutput = null;
            outputWritten = 0;
            outputWaitId = LivenessTimers.NO_WAIT;
            outbox.discard();
        }
    }

    /**
     * 종료 중인 연결(CLOSE를 보냈거나 받음)에서 애플리케이션이 더 기다리지 않고 닫기로 했을 때 부른다. 받아들였지만 아직 애플리케이션에
     * 넘기지 않은 상대의 요청을 {@code RESET(REFUSED)}로 돌려보낸 뒤 전송 계층을 닫는다(규격 3.5.1절). 종료 중이 아니거나 이미 닫혔으면
     * 아무것도 하지 않는다.
     */
    public void abortClosing() {
        if (phase.phase() != ConnectionPhase.Phase.CLOSING) {
            return;
        }
        refuseUnhandedRequests();
        // 더 기다리지 않으므로 대기 중인 스트림이 없어진 것으로 본다.
        phase.pendingStreamsReachedZero();
        conclude();
    }

    /**
     * 지금까지 쌓인, 상대에게 보낼 메시지를 보낸 순서대로 꺼낸다. 프레임 하나가 메시지 하나다. 바이트 스트림 전송은 메시지를 이어 붙여 보내면
     * 된다.
     *
     * @return 보낼 메시지. 없으면 빈 목록
     */
    public List<OutboundMessage> drainMessages() {
        if (incrementalOutput) {
            throw new IllegalStateException("진행 보고 방식과 일괄 수거 방식을 섞을 수 없다");
        }
        // 확인 대기는 프레임을 전송 계층에 넘긴 때부터 센다(규격 3.3절). 넘긴 때는 꺼낸 때로 본다.
        long drainedAt = now;
        List<OutboundMessage> drained = outbox.drain(id -> timers.started(id, drainedAt));
        if (!drained.isEmpty()) {
            timers.outputProgressed(drainedAt);
        }
        ledger.onWriteStarted();
        return drained;
    }

    /**
     * 아직 상대에게 다 내보내지 못한 출력의 바이트 합. 쓰기 대기열에 든 것과 쓰기를 시작해 남은 부분을 더한다. 진행 보고 방식에서 전송 모듈이 읽기를
     * 멈출지 판단할 때 쓴다.
     *
     * @return 남은 출력 바이트
     */
    public long pendingOutputBytes() {
        long writing = currentOutput != null && outputWritten > 0 ? currentOutput.length() - outputWritten : 0;
        return outbox.bytes() + writing;
    }

    /** 현재 쓸 출력. 철회 또는 우선순위 변경 뒤에는 새 식별자가 발급된다. */
    public @Nullable EndpointOutput nextOutput() {
        incrementalOutput = true;
        if (outputDisconnected) {
            return null;
        }
        Outbox.Node head = outbox.first();
        if (outputWritten == 0 && selectedOutput != head) {
            selectedOutput = head;
            currentOutput = head == null ? null : new EndpointOutput(++nextOutputId, head.message);
        }
        return currentOutput;
    }

    /** 선택한 출력에 전송된 바이트 수를 보고한다. */
    public void outputProgress(long id, int count) {
        EndpointOutput output = nextOutput();
        if (output == null || output.id() != id || count < 0 || count > output.length() - outputWritten) {
            throw new IllegalArgumentException("유효하지 않은 출력 진행");
        }
        if (count == 0) {
            return;
        }
        if (config.isWebSocket()) {
            throw new IllegalStateException("WebSocket 출력은 메시지 전체 접수만 보고한다(outputAccepted)");
        }
        if (outputWritten == 0) {
            startOutput();
        }
        timers.outputProgressed(now);
        outputWritten += count;
        if (outputWritten == output.length()) {
            completeOutput();
        }
    }

    /** 선택한 출력의 쓰기를 시작한 것으로 한다. 이제 철회할 수 없다. */
    private void startOutput() {
        Outbox.Node node = Objects.requireNonNull(selectedOutput);
        outputWaitId = node.waitId;
        ledger.onWriteStarted(node);
        outbox.removeForWriting(node);
    }

    /** 선택한 출력이 전체 전달된 것으로 한다. */
    private void completeOutput() {
        if (outputWaitId != LivenessTimers.NO_WAIT) {
            timers.started(outputWaitId, now);
            outputWaitId = LivenessTimers.NO_WAIT;
        }
        ledger.onWriteCompleted(Objects.requireNonNull(selectedOutput));
        outputWritten = 0;
        selectedOutput = null;
        currentOutput = null;
    }

    /**
     * 지금까지 쌓인 애플리케이션 이벤트를 꺼낸다.
     *
     * @return 이벤트 목록. 없으면 빈 목록
     */
    public List<Event> drainEvents() {
        List<Event> drained;
        if (refusedRequests.isEmpty()) {
            drained = new ArrayList<>(events);
        } else {
            drained = new ArrayList<>(events.size());
            for (Event event : events) {
                if (event.kind() != Event.Kind.REQUEST_RECEIVED || !refusedRequests.contains(event.streamId())) {
                    drained.add(event);
                }
            }
        }
        events.clear();
        drainedPeerStreamId = highestPeerStreamId;
        refusedRequests.clear();
        return drained;
    }

    /** 최대 개수만큼 사건을 순서대로 수거한다. */
    public List<Event> drainEvents(int maximum) {
        if (maximum < 0) {
            throw new IllegalArgumentException("사건 수거 한도는 음수일 수 없다");
        }
        List<Event> drained = new ArrayList<>();
        int consumed = 0;
        while (consumed < events.size() && drained.size() < maximum) {
            Event event = events.get(consumed++);
            if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                if (refusedRequests.remove(event.streamId())) {
                    continue;
                }
                drainedPeerStreamId = Math.max(drainedPeerStreamId, event.streamId());
            }
            drained.add(event);
        }
        events.subList(0, consumed).clear();
        return drained;
    }

    /**
     * {@code off}에서 시작하는 {@code available}바이트에서 한 단위(서문 또는 프레임)를 처리한다. 처리했으면 소비한 바이트 수를 돌려준다.
     * 모자라면 0을 돌려주고 {@link #wantedLength}에 진행하려면 있어야 하는 총 바이트 수를 둔다. 판정이 나서 닫았으면 0을 돌려준다. 도착 단위에
     * 따라 결과가 달라지면 안 되므로, 판정에 필요한 바이트가 모이기 전에는 아무것도 하지 않는다.
     */
    private int consumeUnit(byte[] bytes, int off, int available) {
        if (phase.awaitingPreface()) {
            return consumePreface(bytes, off, available);
        }
        if (available < Frames.LENGTH_FIELD_SIZE) {
            wantedLength = Frames.LENGTH_FIELD_SIZE;
            return 0;
        }
        long length = Frames.readUnsignedInt(bytes, off);
        if (phase.awaitingFirstFrame()) {
            // 첫 프레임 검사(규격 3.2.4절): 헤더를 읽을 수 없거나 HELLO가 아니면 아무것도 보내지 않고 닫는다.
            if (length < Frames.MIN_LENGTH) {
                closeWithoutClose();
                return 0;
            }
            if (available < Frames.HEADER_SIZE) {
                wantedLength = Frames.HEADER_SIZE;
                return 0;
            }
            if (!isFirstFrameHeader(bytes, off)) {
                closeWithoutClose();
                return 0;
            }
            phase.firstFrameVerified();
        }
        if (length < Frames.MIN_LENGTH || length > config.maxFrameSize() - Frames.LENGTH_FIELD_SIZE) {
            if (available < Frames.TYPE_END) {
                // type을 읽을 때까지 기다린다. CLOSE이면 받은 CLOSE로 보고 CLOSE 없이 닫아야 하므로 도착 단위에 따라 결과가 달라지면 안 된다.
                wantedLength = Frames.TYPE_END;
                return 0;
            }
            rejectBadLength(bytes, off);
            return 0;
        }
        int frameSize = (int) (Frames.LENGTH_FIELD_SIZE + length);
        if (available < frameSize) {
            wantedLength = frameSize;
            return 0;
        }
        handle(bytes, off, frameSize);
        return frameSize;
    }

    /** 서문의 앞 6바이트를 비교한다(규격 3.1절). 판정할 만큼 모였으면 서문 8바이트를 건너뛴다. 서문은 8바이트를 모두 받은 뒤 건너뛴다. */
    private int consumePreface(byte[] bytes, int off, int available) {
        byte[] preface = Frames.preface();
        if (!matchesPrefacePrefix(bytes, off, available)) {
            closeWithoutClose();
            return 0;
        }
        if (available < preface.length) {
            wantedLength = preface.length;
            return 0;
        }
        phase.prefaceVerified();
        sendHello();
        return preface.length;
    }

    /** 서문의 앞 6바이트와, 받은 만큼의 앞부분이 일치하는가. */
    private static boolean matchesPrefacePrefix(byte[] bytes, int off, int available) {
        byte[] preface = Frames.preface();
        int compared = Math.min(available, PREFACE_PREFIX_LENGTH);
        for (int i = 0; i < compared; i++) {
            if (bytes[off + i] != preface[i]) {
                return false;
            }
        }
        return true;
    }

    /** 첫 프레임의 헤더가 첫 프레임 검사(규격 3.2.4절)를 통과하는가: HELLO이고 stream_id 0, flags 0. */
    private static boolean isFirstFrameHeader(byte[] bytes, int off) {
        return Frames.typeOf(bytes, off) == FrameType.HELLO
                && Frames.streamIdOf(bytes, off) == 0
                && Frames.flagsOf(bytes, off) == 0;
    }

    /** WebSocket의 첫 프레임 메시지 검사(규격 1.2절, 3.2.4절): 바이너리이고 헤더를 읽을 수 있고 HELLO여야 한다. 통과하면 연결 단계에 알린다. */
    private boolean checkFirstMessage(WebSocketMessageKind kind, byte[] message, int available) {
        boolean valid = kind == WebSocketMessageKind.BINARY
                && available >= Frames.HEADER_SIZE
                && Frames.readUnsignedInt(message, 0) >= Frames.MIN_LENGTH
                && isFirstFrameHeader(message, 0);
        if (valid) {
            phase.firstFrameVerified();
        }
        return valid;
    }

    /** 길이가 어긋난 프레임: type이 CLOSE이면 받은 CLOSE로 보고 CLOSE 없이 닫고, 아니면 연결 오류다(규격 3.5절, 5.1절). */
    private void rejectBadLength(byte[] bytes, int off) {
        if (Frames.typeOf(bytes, off) == FrameType.CLOSE) {
            phase.connectionFailed();
            conclude();
        } else {
            connectionError();
        }
    }

    /**
     * 프레임 하나를 처리한다. 프레임은 {@code bytes}의 {@code off}부터 {@code size}바이트이고, 호출자의 배열을 그대로 읽는다. 이 호출이
     * 돌아온 뒤에는 읽지 않으며 배열을 붙잡지도 않는다(애플리케이션으로 넘기는 바이트는 복사한다).
     */
    private void handle(byte[] bytes, int off, int size) {
        dispatch(bytes, off, size);
        // 프레임 하나를 처리한 뒤 종료 경로를 한 번 지난다. 응답·RESET·CLOSE·오류로 대기 중인 스트림이 0개가 됐는지, 닫을 때가 됐는지는
        // 여기서 본다. 연결 오류를 내는 핸들러는 스스로 conclude하며, conclude는 멱등이라 여기서 한 번 더 불러도 안전하다.
        conclude();
    }

    private void dispatch(byte[] bytes, int off, int size) {
        // 불변식: 끝난 연결에서는 불리지 않는다. 입력은 enterReceive가 끝난 연결에서 막고, 프레임 하나를 처리하는 동안 연결을 끝낼 수 있는 것은
        // 애플리케이션 콜백(연결 인증)뿐인데 그 뒤로는 이 프레임에서 admit을 다시 부르지 않는다. 그래서 phase.admit은 던지지 않는다.
        timers.frameReceived(now);
        int type = Frames.typeOf(bytes, off);
        int extensionsLength = Frames.extensionsLengthOf(bytes, off);
        long streamId = Frames.streamIdOf(bytes, off);
        if (type == FrameType.CLOSE) {
            handleClose(bytes, off, size, extensionsLength, streamId);
            return;
        }
        if (extensionsLength > size - Frames.HEADER_SIZE) {
            connectionError();
            return;
        }
        if (type >= FrameType.FIRST_APPLICATION) {
            handleApplication(bytes, off, size, type, extensionsLength, streamId);
            return;
        }
        boolean ack = (Frames.flagsOf(bytes, off) & Flags.ACK) != 0;
        ConnectionPhase.Admission admission = phase.admit(type, ack);
        if (admission == ConnectionPhase.Admission.RESERVED) {
            // 예약 제어 타입은 확장 영역 검사만 받고 무시한다(규격 2.2.2절, 2.4.3절). stream_id와 관문은 보지 않는다.
            if (extensionsLength != 0) {
                connectionError();
            }
            return;
        }
        if (extensionsLength != 0 || (type != FrameType.RESET && streamId != 0)) {
            connectionError();
            return;
        }
        if (admission == ConnectionPhase.Admission.CONNECTION_ERROR) {
            // 관문(규격 3.3절)이나 HELLO·AUTH의 횟수·방향을 어겼다.
            connectionError();
            return;
        }
        int payloadAt = off + Frames.HEADER_SIZE;
        int payloadSize = size - Frames.HEADER_SIZE;
        if (type == FrameType.HELLO) {
            handleHello(ack, bytes, payloadAt, payloadSize);
        } else if (type == FrameType.PING) {
            handlePing(ack, bytes, payloadAt, payloadSize);
        } else if (type == FrameType.RESET) {
            handleReset(streamId, bytes, payloadAt, payloadSize);
        } else if (type == FrameType.AUTH) {
            handleAuth(bytes, payloadAt, payloadSize);
        }
    }

    /**
     * 받은 RESET(규격 4.4절). {@code stream_id}가 0이거나 최상위 비트가 1이거나 payload가 4바이트가 아니면 연결 오류다. 대기 중인
     * 스트림이면 끝내고, 내 요청이었다면 로컬 결과를 넘긴다. 대기 중이 아니면 조용히 버린다(규격 4.3절). flags는 모두 미정의라 보지
     * 않는다(규격 2.3절).
     */
    private void handleReset(long streamId, byte[] bytes, int payloadAt, int payloadSize) {
        if (isInvalidStreamId(streamId) || payloadSize != RESET_PAYLOAD_SIZE) {
            connectionError();
            return;
        }
        if (isOwnSpace(streamId)) {
            OwnRequestLedger.Outcome outcome = ledger.onReset(streamId, Frames.readUnsignedInt(bytes, payloadAt));
            if (outcome != null) {
                applyOutcome(outcome);
            }
        } else if (peerPendingStreams.remove(streamId) != null) {
            // 애플리케이션에 넘겼거나 넘길 요청이다. 처리를 멈출 수 있게 알린다(규격 4.4절). 같은 요청의 REQUEST_RECEIVED가 이 사건보다 앞선다.
            events.add(Event.requestReset(streamId, Frames.readUnsignedInt(bytes, payloadAt)));
        }
    }

    /** {@code stream_id}가 0이거나 최상위 비트가 1이다(규격 2.2.5절). 최상위 비트가 1이면 {@code long}으로 음수다. */
    private static boolean isInvalidStreamId(long streamId) {
        return !Frames.isValidStreamId(streamId);
    }

    private boolean isOwnSpace(long streamId) {
        return ledger.isOwnSpace(streamId);
    }

    /**
     * 애플리케이션 프레임. 확장 영역 형식 위반은 요청인지 응답인지, type을 아는지와 상관없이 연결 오류다(규격 2.4.1절). 내 번호 공간의
     * 프레임은 응답이고 상대 공간의 프레임은 새 요청이다(규격 4.2절).
     */
    private void handleApplication(byte[] bytes, int off, int size, int type, int extensionsLength, long streamId) {
        // 연결 오류 집합은 검사 순서가 없다(규격 5.1절 3번). 확장 영역 형식, 상대 ACK 전의 프레임(3.3절), 잘못된 stream_id.
        // 불변식: dispatch와 같이 끝난 연결에서는 불리지 않으므로 phase.admit은 던지지 않는다.
        Extensions extensionsResult = extensionsLength > MAX_EXTENSIONS_LENGTH
                ? Extensions.MALFORMED
                : scanExtensions(bytes, off, extensionsLength);
        if (extensionsResult == Extensions.MALFORMED
                || phase.admit(type, false) != ConnectionPhase.Admission.ALLOW
                || isInvalidStreamId(streamId)) {
            connectionError();
        } else if (isOwnSpace(streamId)) {
            handleResponse(bytes, off, size, extensionsLength, streamId);
        } else {
            handleRequest(type, extensionsResult, streamId, bytes, off, size, extensionsLength);
        }
    }

    /**
     * 상대의 새 요청(규격 5.1절 5번). (가) 거절, (나) 모르는 type, (다) 확장 값 오류 순서로 보고, 모두 지나면 애플리케이션에 넘긴다.
     * 거절한 요청과 오류로 답한 요청은 대기 스트림에 넣지 않지만 번호는 쓰인 것으로 센다(규격 4.5절, 5.1절).
     */
    private void handleRequest(
            int type,
            Extensions extensionsResult,
            long streamId,
            byte[] bytes,
            int off,
            int size,
            int extensionsLength) {
        if (streamId <= highestPeerStreamId) {
            // 번호 역행: 재사용한 번호, 건너뛰고 나중에 온 번호, 대기 중인 스트림의 번호 모두 연결 오류다(규격 4.2.1절).
            connectionError();
            return;
        }
        highestPeerStreamId = streamId;
        if (phase.closeExchanged()) {
            // CLOSE를 보냈거나 받은 뒤의 새 요청은 끝까지 읽고 조용히 버린다. 어떤 프레임도 보내지 않는다(규격 3.5절).
            return;
        }
        if ((deferredAuthentication && !handlerReady)
                || peerPendingStreams.size() + 1 > config.maxConcurrentStreams()) {
            send(Frames.reset(streamId, ErrorCode.REFUSED));
        } else if (!config.knows(type)) {
            send(Frames.response(type, streamId, Status.UNIMPLEMENTED));
        } else if (extensionsResult == Extensions.INVALID_VALUE) {
            send(Frames.response(type, streamId, Status.INVALID_ARGUMENT));
        } else {
            // 대기 중인 요청은 항상 REQUEST_RECEIVED 이벤트와 함께 생긴다. refuseUnhandedRequests가 이 짝에 기댄다.
            peerPendingStreams.put(streamId, type);
            int extensionsAt = off + Frames.HEADER_SIZE;
            events.add(Event.requestReceived(
                    streamId,
                    type,
                    Arrays.copyOfRange(bytes, extensionsAt, extensionsAt + extensionsLength),
                    bodyOf(bytes, off, size, extensionsLength)));
        }
    }

    /** 확장 영역 검사 결과. 형식 위반이 값 위반보다 우선한다. */
    private enum Extensions {
        MALFORMED,
        INVALID_VALUE,
        VALID
    }

    /**
     * 확장 영역을 한 번 훑어 검사한다. 형식(규격 2.4.1절)은 항목 길이의 합이 {@code extensions_length}와 같고 같은 tag가 없는 것이고, 값(규격
     * 2.4.2절)은 timeout이 4바이트, idempotency-key가 1~64바이트인 것이다.
     */
    private static Extensions scanExtensions(byte[] bytes, int off, int extensionsLength) {
        return scanExtensions(bytes, off, extensionsLength, Frames.HEADER_SIZE);
    }

    /** {@code headerSize}는 {@code off}에서 확장 영역이 시작하는 곳까지의 거리다. 확장 영역만 든 배열은 0이다. */
    private static Extensions scanExtensions(byte[] bytes, int off, int extensionsLength, int headerSize) {
        Set<Integer> tags = new HashSet<>();
        boolean valuesValid = true;
        int at = off + headerSize;
        int end = at + extensionsLength;
        while (at < end) {
            if (end - at < TLV_HEADER_SIZE) {
                return Extensions.MALFORMED;
            }
            int tag = Frames.readUnsignedShort(bytes, at);
            int length = Frames.readUnsignedShort(bytes, at + 2);
            at += TLV_HEADER_SIZE + length;
            if (at > end || !tags.add(tag)) {
                return Extensions.MALFORMED;
            }
            if ((tag == TIMEOUT_TAG && length != TIMEOUT_LENGTH)
                    || (tag == IDEMPOTENCY_KEY_TAG && (length < 1 || length > MAX_IDEMPOTENCY_KEY_LENGTH))) {
                valuesValid = false;
            }
        }
        return valuesValid ? Extensions.VALID : Extensions.INVALID_VALUE;
    }

    /** 애플리케이션 프레임의 본문(확장 영역 뒤)을 복사한다. 호출자의 배열은 이 호출이 돌아온 뒤 읽지 않는다. */
    private static byte[] bodyOf(byte[] bytes, int off, int size, int extensionsLength) {
        int from = off + Frames.HEADER_SIZE + extensionsLength;
        return Arrays.copyOfRange(bytes, from, off + size);
    }

    /** 내 요청의 응답. 대기 중인 요청이면 결과를 애플리케이션에 넘긴다. 종료 중이고 마지막 요청이었다면 닫는다(규격 3.5절). */
    private void handleResponse(byte[] bytes, int off, int size, int extensionsLength, long streamId) {
        OwnRequestLedger.Outcome outcome = ledger.onResponse(
                streamId,
                Frames.typeOf(bytes, off),
                Frames.statusOf(bytes, off),
                bodyOf(bytes, off, size, extensionsLength));
        if (outcome == null) {
            // 대기 중이 아닌 스트림의 프레임은 조용히 버린다(규격 4.3절).
            return;
        }
        applyOutcome(outcome);
    }

    private void handleHello(boolean ack, byte[] bytes, int payloadAt, int payloadSize) {
        if (ack) {
            // 합친 프레임(ACK이면서 payload가 있음)과 두 번째 ACK는 연결 오류다(규격 3.2.5절).
            if (payloadSize != 0) {
                connectionError();
                return;
            }
            phase.ownHelloAckReceived();
            timers.ackReceived(LivenessTimers.Kind.HELLO, null);
            return;
        }
        PeerLimits peer = peerLimitsOf(bytes, payloadAt, payloadSize);
        if (peer == null) {
            connectionError();
            return;
        }
        phase.peerHelloAcked();
        ledger.setPeerMaxConcurrentStreams(peer.maxConcurrentStreams);
        peerMaxFrameSize = peer.maxFrameSize;
        send(Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        if (server) {
            timers.authWaitStarted(now);
            return;
        }
        byte[] auth = Frames.auth(config.authMechanism(), config.authCredentials());
        if (exceedsPeerFrameSize(auth)) {
            // 3.2.1절은 상대 max_frame_size를 넘는 프레임을 금지(MUST NOT)하며 AUTH도 예외가 아니다. 상대 HELLO는 유효하므로 ACK는 3.2.3절이
            // 요구한 대로 보냈다. 그다음 닫는 처리만 규격에 정의가 없다(#196): AUTH도 CLOSE도 보내지 않고 전송 계층만 닫는다. 아무것도 보내지
            // 않았으므로 대기 요청은 연결이 끝날 때 처리 안 됨으로 끝난다.
            phase.connectionFailed();
            return;
        }
        // 클라이언트는 서버 HELLO에 ACK를 보낸 직후 AUTH를 한 번 보낸다. 서버 ACK를 기다리지 않는다(규격 3.3절 4번).
        sendAwaitingAck(auth, LivenessTimers.Kind.AUTH, null);
    }

    private static byte[] utf8Prefix(String reason, int maxBytes) {
        byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(bytes.length, maxBytes);
        CharsetDecoder decoder = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        while (length > 0) {
            try {
                decoder.reset().decode(ByteBuffer.wrap(bytes, 0, length));
                break;
            } catch (CharacterCodingException invalidPrefix) {
                length--;
            }
        }
        return Arrays.copyOf(bytes, length);
    }

    /**
     * 받은 {@code AUTH}와 {@code AUTH(ACK)}(규격 3.3.1절). 서버는 첫 {@code AUTH}를 애플리케이션에 넘기고, 클라이언트는 첫
     * {@code AUTH(ACK)}에서 {@code credential_lifetime}을 알린다. 방향 위반, 두 번째 {@code AUTH}·{@code AUTH(ACK)},
     * 형태 위반은 연결 오류다.
     */
    private void handleAuth(byte[] bytes, int payloadAt, int payloadSize) {
        // 방향과 횟수는 연결 단계가 이미 걸렀다: 서버는 첫 AUTH만, 클라이언트는 첫 AUTH(ACK)만 여기에 온다.
        if (server) {
            handleServerAuth(bytes, payloadAt, payloadSize);
        } else {
            handleClientAuthAck(bytes, payloadAt, payloadSize);
        }
    }

    private void handleServerAuth(byte[] bytes, int payloadAt, int payloadSize) {
        if (authenticationPending) {
            connectionError();
            return;
        }
        timers.authWaitEnded();
        int mechanismLength = payloadSize == 0 ? 0 : bytes[payloadAt] & 0xFF;
        if (mechanismLength < 1
                || mechanismLength > EndpointConfig.MAX_AUTH_MECHANISM_LENGTH
                || payloadSize < 1 + mechanismLength) {
            connectionError();
            return;
        }
        String mechanism = new String(bytes, payloadAt + 1, mechanismLength, StandardCharsets.US_ASCII);
        if (!EndpointConfig.isValidAuthMechanism(mechanism)) {
            connectionError();
            return;
        }
        // 자격 증명은 애플리케이션으로 넘어가므로 복사한다.
        byte[] credentials = Arrays.copyOfRange(bytes, payloadAt + 1 + mechanismLength, payloadAt + payloadSize);
        if (deferredAuthentication) {
            authenticationPending = true;
            events.add(Event.authenticationRequired(mechanism, credentials));
            return;
        }
        AuthDecision decision;
        try {
            decision = config.authenticator().authenticate(mechanism, credentials);
        } catch (RuntimeException e) {
            // 애플리케이션이 던지면 일시 실패로 본다. 연결을 매달아 두지 않는다. 원인은 CLOSED 이벤트에 싣는다.
            phase.causedBy(e);
            decision = AuthDecision.TRANSIENT_FAILURE;
        }
        finishAuthentication(decision, config.credentialLifetimeSeconds());
    }

    /** 인증 판단 식별자에 대한 결과를 입력한다. 통과여도 수명은 알리지 않는다. 늦거나 중복된 결과는 무시한다. */
    public void completeAuthentication(long id, AuthDecision decision) {
        completeAuthentication(id, decision, OptionalLong.empty());
    }

    /**
     * 인증 판단 식별자에 대한 결과를 입력한다. 늦거나 중복된 결과는 무시한다.
     *
     * @param credentialLifetime 통과일 때 {@code AUTH(ACK)}의 {@code credential_lifetime}으로 알릴 남은 수명(초, 부호 없는 32비트).
     *     비어 있으면 알리지 않는다. 통과가 아니면 쓰지 않는다
     * @throws IllegalArgumentException 수명이 부호 없는 32비트 범위를 벗어났을 때
     */
    public void completeAuthentication(long id, AuthDecision decision, OptionalLong credentialLifetime) {
        Objects.requireNonNull(decision, "decision");
        Objects.requireNonNull(credentialLifetime, "credentialLifetime");
        if (credentialLifetime.isPresent()
                && (credentialLifetime.getAsLong() < 0 || credentialLifetime.getAsLong() > 0xFFFF_FFFFL)) {
            throw new IllegalArgumentException(
                    "credentialLifetime은 부호 없는 32비트 정수여야 한다: " + credentialLifetime.getAsLong());
        }
        if (id != AUTHENTICATION_ID || !authenticationPending || !phase.isActive()) {
            return;
        }
        authenticationPending = false;
        finishAuthentication(decision, credentialLifetime);
    }

    /** 인증을 마친 연결에서 핸들러 준비가 완료되었음을 알린다. */
    public void handlerReady() {
        if (deferredAuthentication && phase.isActive() && phase.hasPassedAuthGate()) {
            handlerReady = true;
        }
    }

    private void finishAuthentication(AuthDecision decision, OptionalLong credentialLifetime) {
        if (!phase.isActive()) {
            // 콜백 안에서 CLOSE를 보냈거나 연결 오류로 CLOSE 교환으로 쳤거나 연결이 끝났다(isActive는 이 셋을 모두 거짓으로 답한다). 판정과 무관하게
            // CLOSE·CLOSED 뒤 프레임을 쌓지 않는다(규격 3.5절). AUTH 단계에는 대기 중인 스트림이 없어 콜백 안의 close()가 conclude()로 곧바로
            // 연결을 끝내므로 "닫는 중이지만 안 끝난" 상태는 여기서 도달할 수 없다(EndpointEndedConnectionTest가 고정한다).
            return;
        }
        if (decision != AuthDecision.PASS) {
            // 거부와 모르는 메커니즘은 UNAUTHENTICATED, 일시 실패는 INTERNAL_ERROR. 이유는 비운다(규격 3.3.1절).
            connectionError(decision == AuthDecision.REJECT ? ErrorCode.UNAUTHENTICATED : ErrorCode.INTERNAL_ERROR);
            return;
        }
        send(Frames.authAck(credentialLifetime));
        openAuthGate();
    }

    private void handleClientAuthAck(byte[] bytes, int payloadAt, int payloadSize) {
        if (payloadSize % AUTH_ACK_PARAM_SIZE != 0) {
            connectionError();
            return;
        }
        OptionalLong lifetime = OptionalLong.empty();
        Set<Integer> seen = new HashSet<>();
        for (int at = payloadAt; at < payloadAt + payloadSize; at += AUTH_ACK_PARAM_SIZE) {
            if (!seen.add(Frames.readUnsignedShort(bytes, at))) {
                connectionError();
                return;
            }
            // 모르는 identifier는 무시한다(규격 3.3.1절).
            if (Frames.readUnsignedShort(bytes, at) == Frames.AUTH_PARAM_CREDENTIAL_LIFETIME) {
                lifetime = OptionalLong.of(Frames.readUnsignedInt(bytes, at + 2));
            }
        }
        timers.ackReceived(LivenessTimers.Kind.AUTH, null);
        events.add(Event.authenticated(lifetime));
        openAuthGate();
    }

    /**
     * AUTH 관문을 연다. 미뤄 둔 요청과 PING을 미룬 순서대로 보낸다. 요청에 번호를 주다가 닫히면 남은 번호 없는 요청이 끝나고(CLOSE 뒤 PING은 보내지
     * 않는다), 전송 계층이 닫히면 모두 끝나므로, 한 단계마다 장부와 연결 상태를 다시 본다.
     */
    private void openAuthGate() {
        phase.authGatePassed();
        while (true) {
            DeferredPing ping = deferredPings.peekFirst();
            long ticket = ledger.firstQueuedTicket();
            OwnRequestLedger.Admission admission =
                    ledger.openQueued(ping == null ? Long.MAX_VALUE : ping.beforeTicket, peerMaxFrameSize);
            if (admission != OwnRequestLedger.Admission.NONE) {
                // 크기를 넘는 미룬 요청은 호출자가 돌아간 뒤에 보내므로 던지지 않고 로컬에서 실패시킨다.
                if (refuseLocally(ticket, admission) == OwnRequestLedger.Admission.OVERSIZE) {
                    failLocally(ticket);
                }
            } else if (ping == null) {
                return;
            } else {
                deferredPings.removeFirst();
                if (!phase.closeExchanged()) {
                    sendPing(ping.payload);
                }
            }
        }
    }

    /**
     * HELLO payload를 한 번 훑는다(규격 3.2.3절, 3.2.5절). 6의 배수, identifier 중복 없음, max_frame_size 범위를 검증하고, 유효하면 상대가 알린
     * {@code max_concurrent_streams}와 {@code max_frame_size}(없으면 각 기본값)를 돌려주고, 유효하지 않으면 null을 돌려준다.
     */
    private static @Nullable PeerLimits peerLimitsOf(byte[] bytes, int payloadAt, int payloadSize) {
        if (payloadSize % HelloParams.HELLO_PARAM_SIZE != 0) {
            return null;
        }
        long maxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
        long maxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;
        Set<Integer> seen = new HashSet<>();
        for (int at = payloadAt; at < payloadAt + payloadSize; at += HelloParams.HELLO_PARAM_SIZE) {
            int identifier = Frames.readUnsignedShort(bytes, at);
            if (!seen.add(identifier)) {
                return null;
            }
            long value = Frames.readUnsignedInt(bytes, at + 2);
            if (identifier == HelloParams.MAX_FRAME_SIZE_ID
                    && (value < EndpointConfig.MAX_FRAME_SIZE_LOWER_LIMIT
                            || value > HelloParams.MAX_FRAME_SIZE_UPPER_LIMIT)) {
                return null;
            }
            if (identifier == HelloParams.MAX_FRAME_SIZE_ID) {
                maxFrameSize = value;
            } else if (identifier == HelloParams.MAX_CONCURRENT_STREAMS_ID) {
                maxConcurrentStreams = value;
            }
        }
        return new PeerLimits(maxConcurrentStreams, maxFrameSize);
    }

    private void handlePing(boolean ack, byte[] bytes, int payloadAt, int payloadSize) {
        if (payloadSize != PING_PAYLOAD_SIZE) {
            connectionError();
            return;
        }
        // 8바이트만 복사한다. 확인 대기와 맞춰 보거나 ACK로 되돌려 보낼 때 쓴다.
        byte[] payload = Arrays.copyOfRange(bytes, payloadAt, payloadAt + PING_PAYLOAD_SIZE);
        if (ack) {
            // 보낸 PING과 맞춰 보는 일은 로컬 몫이고, 맞지 않는 ACK는 무시한다(규격 3.4절).
            timers.ackReceived(LivenessTimers.Kind.PING, payload);
        } else {
            send(Frames.control(FrameType.PING, Flags.ACK, payload));
        }
    }

    /** 받은 CLOSE(규격 3.5절). 형태가 잘못돼도 받은 CLOSE로 치고, 그때는 오류 CLOSE처럼 CLOSE 없이 닫는다. */
    private void handleClose(byte[] bytes, int off, int size, int extensionsLength, long streamId) {
        int payloadSize = size - Frames.HEADER_SIZE - extensionsLength;
        long lastStreamId = 0;
        boolean malformed = extensionsLength != 0 || streamId != 0 || payloadSize < CLOSE_MIN_PAYLOAD_SIZE;
        if (!malformed) {
            lastStreamId = Frames.readLong(bytes, off + Frames.HEADER_SIZE + CLOSE_ERROR_CODE_SIZE);
            // 최상위 비트가 1이면 long으로 음수이고 보낸 번호보다 크다. 이 값은 쓰지 않는다.
            malformed = lastStreamId != 0
                    && (!isOwnSpace(lastStreamId)
                            || lastStreamId < 0
                            || lastStreamId > ledger.highestWrittenStreamId());
        }
        if (malformed) {
            phase.connectionFailed();
            return;
        }
        if (phase.closeReceived(Frames.readUnsignedInt(bytes, off + Frames.HEADER_SIZE))) {
            for (OwnRequestLedger.Outcome outcome : ledger.endAfter(lastStreamId)) {
                applyOutcome(outcome);
            }
        }
        endQueuedRequests();
    }

    /**
     * CLOSE를 보내거나 받은 즉시, 번호를 받지 못하고 기다리던 요청(AUTH 관문 전에 미룬 요청)을 장부가 로컬 UNAVAILABLE(처리 안 됨)로 끝낸다(규격
     * 3.5절). 이후 새 요청은 번호를 받지 못한다. 미룬 PING은 그대로 둔다.
     */
    private void endQueuedRequests() {
        for (OwnRequestLedger.Outcome outcome : ledger.onClosing()) {
            applyOutcome(outcome);
        }
    }

    /**
     * 끊기 전에 돌려보낸다(규격 3.5.1절, SHOULD). 받아들였지만 아직 애플리케이션이 이벤트로 받지 않은 상대의 요청을 {@code
     * RESET(REFUSED)}로 돌려보내고 스트림을 끝낸다. 오류 CLOSE보다 먼저 호출한다.
     *
     * <p>암묵 불변식: {@code drainedPeerStreamId}보다 큰 번호의 대기 스트림은 항상 아직 꺼내지 않은 REQUEST_RECEIVED 이벤트와 짝이다
     * ({@link #handleRequest}가 둘을 함께 만들고, 이벤트는 {@link #drainEvents}가 {@code drainedPeerStreamId}를 올릴 때만 사라진다).
     * 그래서 여기서 돌려보낸 요청의 이벤트를 {@link #drainEvents}가 가려낼 수 있다.
     */
    private void refuseUnhandedRequests() {
        Iterator<Long> pending = peerPendingStreams.keySet().iterator();
        while (pending.hasNext()) {
            long streamId = pending.next();
            if (streamId > drainedPeerStreamId) {
                pending.remove();
                send(Frames.reset(streamId, ErrorCode.REFUSED));
                refusedRequests.add(streamId);
            }
        }
    }

    /**
     * 연결 오류: CLOSE(PROTOCOL_ERROR)를 보내고 닫는다. 이미 CLOSE를 보냈거나 받았다면 보내지 않는다(규격 3.5절, 5.1절). 어느 쪽이든
     * 이 호출 뒤에는 CLOSE 교환이 된 상태다.
     */
    private void connectionError() {
        connectionError(ErrorCode.PROTOCOL_ERROR);
    }

    /**
     * 오류 {@code errorCode}의 CLOSE를 보내고 닫는다(연결 오류, 확인 대기 초과, 연결 인증의 거부·일시 실패·대기 초과). 이미 CLOSE를 보냈거나
     * 받았다면 보내지 않는다(규격 3.5절).
     */
    private void connectionError(int errorCode) {
        refuseUnhandedRequests();
        if (!phase.closeExchanged()) {
            send(Frames.close(errorCode, highestPeerStreamId));
            phase.closeSent(errorCode);
        }
        phase.connectionFailed(errorCode);
        conclude();
    }

    /** CLOSE를 보내지 않고 닫는다. 서문과 첫 프레임 검사(규격 3.1절, 3.2.4절)가 어긋났을 때다. */
    private void closeWithoutClose() {
        phase.connectionFailed();
        conclude();
    }

    /**
     * 종료 경로: 전송 계층을 닫는 유일한 곳이다. 대기 중인 스트림이 0개이면 그 사건을 단계에 알리고, 단계가 닫을 때라고 하면 닫아 연결을 끝낸다.
     * {@code NO_ERROR}로 끝나는 WebSocket 연결이면 마지막 프레임 뒤에 WebSocket Close(1000)를 낸다(규격 1.2절). 오류 종료의 WebSocket
     * 상태 코드는 규격이 정하지 않아 모델링하지 않는다. 연결이 끝날 때 내 요청 장부와 생존 확인 타이머에 알리고, 결과를 받지 못한 요청마다 로컬
     * UNAVAILABLE을 한 번씩 넘긴 뒤 상대가 연 스트림을 비우고 닫은 로컬 원인과 함께 closed를 낸다(규격 3.5절). 이후 받은 바이트와 시간은
     * 처리하지 않는다.
     */
    private void conclude() {
        if (phase.isEnded()) {
            return;
        }
        if (ledger.openCount() == 0 && peerPendingStreams.isEmpty()) {
            phase.pendingStreamsReachedZero();
        }
        if (!phase.shouldCloseTransport()) {
            return;
        }
        if (phase.endsNormally() && config.isWebSocket()) {
            outbox.addLast(OutboundMessage.normalWebSocketClose());
        }
        phase.transportClosed();
        pending = NO_BYTES;
        pendingLength = 0;
        messageBuffer = NO_BYTES;
        messageLength = 0;
        timers.clear();
        for (OwnRequestLedger.Outcome outcome : ledger.onTransportClosed()) {
            applyOutcome(outcome);
        }
        deferredPings.clear();
        peerPendingStreams.clear();
        events.add(Event.closed(phase.closeCause(), phase.closeReason()));
    }

    private void sendHello() {
        sendAwaitingAck(Frames.hello(config), LivenessTimers.Kind.HELLO, null);
    }

    private long sendPing(byte[] payload) {
        return sendAwaitingAck(Frames.control(FrameType.PING, 0, payload), LivenessTimers.Kind.PING, payload);
    }

    /**
     * 요청을 낸 순서(번호표, 0부터)로 요청의 상태를 본다. 번호 없는 대기열 요청도 번호표로 찾는다. 결과가 나와 끝났거나 스트림을 열기 전에
     * 로컬에서 실패한 요청, 낸 적 없는 번호표는 null이다. 쓰기 전(번호 유무 무관)과 쓰기 시작 뒤를 가르는 기준이다.
     */
    OwnRequestLedger.@Nullable RequestState requestState(long ticket) {
        return ledger.stateOfTicket(ticket);
    }

    /** 번호를 받아 아직 끝나지 않은 내 스트림의 수. 요청 기록이 끝난 요청 뒤에 남지 않는지 보는 기준이다. */
    int openStreamCount() {
        return ledger.openCount();
    }

    /** 프레임이 상대가 알린 {@code max_frame_size}를 넘는다(규격 3.2.1절). */
    private boolean exceedsPeerFrameSize(byte[] frame) {
        return frame.length > peerMaxFrameSize;
    }

    private IllegalArgumentException oversizeFrame(String kind) {
        return new IllegalArgumentException(kind + " 프레임이 상대의 max_frame_size(" + peerMaxFrameSize + ")를 넘는다");
    }

    /** 스트림을 열기 전에 요청이 로컬에서 실패했다. 스트림 번호 없는 UNAVAILABLE 결과를 넘긴다. */
    private void failLocally(long ticket) {
        events.add(Event.localUnavailable(ticket));
    }

    /**
     * 프레임을 쓰기 대기열에 넣는다. 순서 규칙은 {@link Outbox}가 정한다. 제어 프레임은 아직 쓰기를 시작한 적 없는 애플리케이션 프레임 앞에 두고
     * (규격 3.6절), 애플리케이션 프레임끼리는 넣은 순서 그대로다. CLOSE는 쓰기 전 요청 가운데 마지막 것 바로 뒤에 둔다. 앞서 보내면 그 요청이 CLOSE
     * 뒤에 온 새 요청이 되어 상대가 버리기 때문이다(규격 3.5절). 그래서 그 뒤에 넣은 다른 제어 프레임(PING, RESET 등)이 그 요청 앞에 있는 CLOSE
     * 앞에 올 수 있다. 제어 프레임끼리의 순서는 3.6절이 정하지 않고, 3.2.2절과 3.5.1절이 정한 순서(HELLO 먼저, 돌려보내는 RESET이 오류 CLOSE보다
     * 먼저)는 CLOSE를 보내는 때에 이미 대기열에 들어 있는 RESET과 HELLO가 CLOSE 앞에 남으므로 지켜진다.
     */
    private Outbox.Node send(byte[] frame) {
        OutboundMessage message = OutboundMessage.binary(frame);
        if (message.isApplication()) {
            return outbox.addApplication(message);
        }
        if (Frames.typeOf(frame, 0) == FrameType.CLOSE) {
            Outbox.Node lastRequest = ledger.lastUnwrittenNode();
            if (lastRequest != null) {
                return outbox.addAfter(lastRequest, message);
            }
        }
        return outbox.addControl(message);
    }

    /** ACK를 기다리는 프레임을 쌓는다. 대기 시간은 {@link #drainMessages}가 이 프레임을 돌려줄 때 시작한다. */
    private long sendAwaitingAck(byte[] frame, LivenessTimers.Kind kind, byte @Nullable [] payload) {
        Outbox.Node node = send(frame);
        node.waitId = timers.await(kind, payload);
        return node.waitId;
    }

    /** AUTH 관문을 통과할 때까지 미룬 PING 하나. */
    private static final class DeferredPing {
        final byte[] payload;
        /** 미룰 때의 다음 번호표. 이보다 작은 번호표의 미룬 요청이 이 PING보다 먼저 나간다. */
        final long beforeTicket;

        DeferredPing(byte[] payload, long beforeTicket) {
            this.payload = payload;
            this.beforeTicket = beforeTicket;
        }
    }

    /** 상대 HELLO가 알린 한도. 없는 파라미터는 규격 기본값이다. */
    private static final class PeerLimits {
        final long maxConcurrentStreams;
        final long maxFrameSize;

        PeerLimits(long maxConcurrentStreams, long maxFrameSize) {
            this.maxConcurrentStreams = maxConcurrentStreams;
            this.maxFrameSize = maxFrameSize;
        }
    }
}
