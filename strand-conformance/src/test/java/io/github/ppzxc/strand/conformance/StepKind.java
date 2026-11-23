package io.github.ppzxc.strand.conformance;

import static io.github.ppzxc.strand.conformance.Protocol.errorCode;
import static io.github.ppzxc.strand.conformance.Protocol.statusCode;
import static io.github.ppzxc.strand.conformance.Protocol.streamId;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 벡터 형식이 정한 단계 종류와 각 종류의 필드, 실행 동작, 확인 대기 해석용 성질을 정의한다. 벡터의 단계별 형태와 일반 실행은 여기서
 * 가져오고, 여러 단계에 걸친 제약은 {@link VectorSchema}에서 검증한다.
 */
final class StepKind {

    static final String TAG = "kind";
    static final String REQUEST_NAME = "request";
    static final String CANCEL_NAME = "cancel";
    static final String CREDENTIAL_EXPIRED_NAME = "credential_expired";
    static final String CREDENTIAL_REVOKED_NAME = "credential_revoked";
    static final String HOLD_OUTPUT_FIELD = "hold_output";
    static final String TIME_ADVANCE_FIELD = "ms";
    static final String LABEL = "단계 kind";

    /** 단계가 엔드포인트에 넣는 들어오는 바이트의 성격. */
    enum Inbound {
        NONE,
        /** 연결 하나의 바이트 흐름에 이어 붙는다(프레임 경계와 무관하게 잘릴 수 있다). */
        STREAM,
        /** 메시지 하나가 따로 들어온다. */
        MESSAGE
    }

    private static final Shape BASE = Shape.EMPTY
            .require(TAG, FieldCheck.DEFERRED)
            .require("send", VectorSchema::frames)
            .require("events", VectorSchema::events);

    /** 정의 등록부. 정의를 만들면 여기 들어가므로 별도 목록을 맞출 필요가 없다. */
    private static final Map<String, StepKind> REGISTRY = new LinkedHashMap<>();

    private static final StepKind RECV = define(
                    "recv",
                    BASE.require("hex", Check::hex),
                    (driver, step) ->
                            driver.receiveStream(Bytes.fromHex(step.get("hex").getAsString())))
            .inbound(Inbound.STREAM)
            .onlyOnTransport(VectorSchema.TCP);

    private static final StepKind RECV_MESSAGE = define(
                    "recv_message",
                    BASE.require("opcode", FieldCheck.oneOf(VectorSchema.OPCODES))
                            .require("hex", Check::hex),
                    (driver, step) -> {
                        // 메시지 하나는 한 단위다. 텍스트 메시지는 바이트로 넘길 수 없는 입력이다.
                        if (step.get("opcode").getAsString().equals("binary")) {
                            driver.receiveUnit(Bytes.fromHex(step.get("hex").getAsString()));
                        } else {
                            driver.receiveUnsupportedInput();
                        }
                    })
            .inbound(Inbound.MESSAGE)
            .onlyOnTransport(VectorSchema.WEBSOCKET);

    private static final StepKind PING = define(
                    "ping",
                    BASE.require("payload", Check::pingPayload),
                    (driver, step) -> driver.endpoint()
                            .ping(Bytes.fromHex(step.get("payload").getAsString())))
            .pingSending();

    private static final StepKind REQUEST = register(
                    REQUEST_NAME,
                    BASE.require("type", VectorSchema.APP_TYPE)
                            .allow("name", Check::string)
                            .allow(HOLD_OUTPUT_FIELD, Check::bool)
                            .allow("write_limit_bytes", VectorSchema.POSITIVE_U32)
                            .allow("extensions", Check::extensions)
                            .allow("payload", Check::hex)
                            .allow("deadline_ms", VectorSchema.POSITIVE_U32),
                    (driver, step, requestTickets) -> {
                        byte[] payload = Bytes.fromHex(VectorSchema.hexOrEmpty(step, "payload"));
                        long ticket = step.has("deadline_ms")
                                ? driver.endpoint()
                                        .request(
                                                step.get("type").getAsInt(),
                                                payload,
                                                step.get("deadline_ms").getAsLong())
                                : driver.endpoint().request(step.get("type").getAsInt(), payload);
                        if (step.has("name"))
                            requestTickets.put(step.get("name").getAsString(), ticket);
                    })
            .unsupportedParts(step -> {
                List<String> unsupported = new ArrayList<>();
                if (step.has("extensions") && step.getAsJsonArray("extensions").size() > 0) {
                    unsupported.add("request.extensions");
                }
                return unsupported;
            });

    private static final StepKind RESPOND = define(
            "respond",
            BASE.require("stream_id", VectorSchema.STREAM_ID)
                    .require("status", VectorSchema.STATUS)
                    .allow("extensions", Check::extensions)
                    .allow("payload", Check::hex),
            (driver, step) -> driver.endpoint()
                    .respond(
                            streamId(step.get("stream_id")),
                            statusCode(step.get("status")),
                            Bytes.fromHex(VectorSchema.hexOrEmpty(step, "payload"))));

    private static final StepKind CANCEL = register(
            CANCEL_NAME,
            BASE.allow("stream_id", VectorSchema.STREAM_ID).allow("request", Check::string),
            (driver, step, requestTickets) -> {
                if (step.has("request")) {
                    driver.endpoint()
                            .cancelRequest(
                                    requestTickets.get(step.get("request").getAsString()));
                } else {
                    driver.endpoint().cancel(streamId(step.get("stream_id")));
                }
            });

    /** CLOSE를 보낼 뿐 연결은 끝나지 않는다(규격 3.3절). */
    private static final StepKind CLOSE = define(
            "close",
            BASE.require("error_code", VectorSchema.ERROR_CODE).allow("reason", Check::string),
            (driver, step) -> driver.endpoint()
                    .close(
                            errorCode(step.get("error_code")),
                            step.has("reason") ? step.get("reason").getAsString() : ""));

    private static final StepKind CREDENTIAL_EXPIRED = define(
                    CREDENTIAL_EXPIRED_NAME,
                    BASE,
                    (driver, step) -> driver.endpoint().credentialExpired())
            .credentialNotification();

    private static final StepKind CREDENTIAL_REVOKED = define(
                    CREDENTIAL_REVOKED_NAME,
                    BASE,
                    (driver, step) -> driver.endpoint().credentialRevoked())
            .credentialNotification();

    private static final StepKind ADVANCE = define(
                    "advance",
                    BASE.require(TIME_ADVANCE_FIELD, VectorSchema.POSITIVE_U32),
                    (driver, step) -> driver.advanceMilliseconds(
                            step.get(TIME_ADVANCE_FIELD).getAsLong()))
            .timeAdvancing();

    private static final StepKind DISCONNECT = define(
                    "disconnect", BASE, (driver, step) -> driver.endpoint().disconnect())
            .connectionEnding();

    private static final Map<String, Shape> SHAPES_BY_NAME = shapes();

    private final String name;
    private final Shape shape;
    private final StepExecution execution;
    private Inbound inbound = Inbound.NONE;
    private boolean advancesTime;
    private boolean endsConnection;
    private boolean sendsPing;
    private boolean credentialNotification;
    private String onlyOnTransport;
    private Function<JsonObject, List<String>> unsupportedParts = step -> Collections.emptyList();

    private StepKind(String name, Shape shape, StepExecution execution) {
        this.name = name;
        this.shape = shape;
        this.execution = execution;
    }

    private static StepKind define(String name, Shape shape, BiConsumer<EndpointDriver, JsonObject> execution) {
        return register(name, shape, (driver, step, requestTickets) -> execution.accept(driver, step));
    }

    private static StepKind register(String name, Shape shape, StepExecution execution) {
        StepKind kind = new StepKind(name, shape, execution);
        if (REGISTRY.put(name, kind) != null) {
            throw new IllegalStateException("단계 종류 정의가 겹친다: " + name);
        }
        return kind;
    }

    // 성질 지정은 정의 시점(클래스 초기화)에만 쓴다.
    private StepKind inbound(Inbound value) {
        this.inbound = value;
        return this;
    }

    private StepKind timeAdvancing() {
        this.advancesTime = true;
        return this;
    }

    private StepKind connectionEnding() {
        this.endsConnection = true;
        return this;
    }

    private StepKind pingSending() {
        this.sendsPing = true;
        return this;
    }

    private StepKind credentialNotification() {
        this.credentialNotification = true;
        return this;
    }

    private StepKind onlyOnTransport(String transport) {
        this.onlyOnTransport = transport;
        return this;
    }

    private StepKind unsupportedParts(Function<JsonObject, List<String>> value) {
        this.unsupportedParts = value;
        return this;
    }

    private static Map<String, Shape> shapes() {
        Map<String, Shape> shapes = new LinkedHashMap<>();
        for (StepKind kind : REGISTRY.values()) {
            shapes.put(kind.name, kind.shape);
        }
        return Collections.unmodifiableMap(shapes);
    }

    /** 형태 검사 표: 단계 종류 이름마다 형태. */
    static Map<String, Shape> shapesByName() {
        return SHAPES_BY_NAME;
    }

    /**
     * 이 단계의 종류. 적재기를 거친 벡터에는 정의에 없는 종류가 없으므로, 없으면 {@link IllegalArgumentException}을 던진다. 이름만으로
     * 모르는 종류를 물어야 하면 {@link #named}를 쓴다.
     */
    static StepKind of(JsonObject step) {
        String name = step.get(TAG).getAsString();
        StepKind kind = REGISTRY.get(name);
        if (kind == null) {
            throw new IllegalArgumentException("알 수 없는 단계 kind: " + name);
        }
        return kind;
    }

    /** 이 종류를 정의에서 찾는다. 던지지 않고 모르는 이름이면 null을 돌려준다(적재기 앞에서 지원 검사를 하는 러너가 쓴다). */
    static StepKind named(String name) {
        return REGISTRY.get(name);
    }

    String name() {
        return name;
    }

    /** 단계를 진행 보고 구동기로 엔드포인트에 적용한다. */
    void execute(EndpointDriver driver, JsonObject step, Map<String, Long> requestTickets) {
        execution.execute(driver, step, requestTickets);
    }

    /** 지원 종류여도 러너가 실행하지 못하는 단계 내용(예: request의 확장). */
    List<String> unsupportedParts(JsonObject step) {
        return unsupportedParts.apply(step);
    }

    Inbound inbound() {
        return inbound;
    }

    /** 단계의 {@code ms}만큼 시간을 진행하는가. */
    boolean advancesTime() {
        return advancesTime;
    }

    boolean holdsOutput(JsonObject step) {
        return step.has(HOLD_OUTPUT_FIELD) && step.get(HOLD_OUTPUT_FIELD).getAsBoolean();
    }

    long timeAdvanceMilliseconds(JsonObject step) {
        return step.get(TIME_ADVANCE_FIELD).getAsLong();
    }

    /** 이 단계만으로 연결이 끝나는가(상대가 끊었다). */
    boolean endsConnection() {
        return endsConnection;
    }

    /** 단계가 PING을 보내며 그 payload가 단계의 {@code payload}인가. */
    boolean sendsPing() {
        return sendsPing;
    }

    boolean isCredentialNotification() {
        return credentialNotification;
    }

    /** 이 종류를 쓸 수 있는 transport. 제한이 없으면 null. */
    String onlyOnTransport() {
        return onlyOnTransport;
    }

    @FunctionalInterface
    private interface StepExecution {
        void execute(EndpointDriver driver, JsonObject step, Map<String, Long> requestTickets);
    }
}
