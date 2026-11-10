package io.github.ppzxc.strand.conformance;

import static io.github.ppzxc.strand.conformance.Protocol.errorCode;
import static io.github.ppzxc.strand.conformance.Protocol.statusCode;
import static io.github.ppzxc.strand.conformance.Protocol.streamId;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 벡터 형식이 정한 단계 종류의 단일 정의. 종류마다 형태(필드 규칙), 실행(엔드포인트에 적용), 지원 여부, 확인 대기 해석용 타이밍
 * 성질(들어오는 바이트인가, 연결을 끝내는가, 시간을 진행하는가)을 함께 갖는다. 형태 검사 표, 러너의 실행·지원 검사, 확인 대기
 * 해석기의 종류 판정이 모두 이 정의에서 나오므로 새 단계 종류는 여기에만 추가한다.
 */
final class StepKind {

    static final String TAG = "kind";
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
                    (endpoint, step) ->
                            endpoint.receive(Bytes.fromHex(step.get("hex").getAsString())))
            .inbound(Inbound.STREAM)
            .onlyOnTransport(VectorSchema.TCP);

    private static final StepKind RECV_MESSAGE = define(
                    "recv_message",
                    BASE.require("opcode", FieldCheck.oneOf(VectorSchema.OPCODES))
                            .require("hex", Check::hex),
                    (endpoint, step) -> endpoint.receiveMessage(
                            step.get("opcode").getAsString().equals("binary")
                                    ? WebSocketMessageKind.BINARY
                                    : WebSocketMessageKind.TEXT,
                            Bytes.fromHex(step.get("hex").getAsString())))
            .inbound(Inbound.MESSAGE)
            .onlyOnTransport(VectorSchema.WEBSOCKET);

    private static final StepKind PING = define(
                    "ping",
                    BASE.require("payload", Check::pingPayload),
                    (endpoint, step) ->
                            endpoint.ping(Bytes.fromHex(step.get("payload").getAsString())))
            .pingSending();

    private static final StepKind REQUEST = define(
                    "request",
                    BASE.require("type", VectorSchema.APP_TYPE)
                            .allow("extensions", Check::extensions)
                            .allow("payload", Check::hex)
                            .allow("deadline_ms", VectorSchema.POSITIVE_U32),
                    (endpoint, step) -> {
                        byte[] payload = Bytes.fromHex(
                                step.has("payload") ? step.get("payload").getAsString() : "");
                        if (step.has("deadline_ms")) {
                            endpoint.request(
                                    step.get("type").getAsInt(),
                                    payload,
                                    step.get("deadline_ms").getAsLong());
                        } else {
                            endpoint.request(step.get("type").getAsInt(), payload);
                        }
                    })
            .unsupportedPart(step ->
                    step.has("extensions") && step.getAsJsonArray("extensions").size() > 0
                            ? "request.extensions"
                            : null);

    private static final StepKind RESPOND = define(
            "respond",
            BASE.require("stream_id", VectorSchema.STREAM_ID)
                    .require("status", VectorSchema.STATUS)
                    .allow("extensions", Check::extensions)
                    .allow("payload", Check::hex),
            (endpoint, step) -> endpoint.respond(
                    streamId(step.get("stream_id")),
                    statusCode(step.get("status")),
                    Bytes.fromHex(step.has("payload") ? step.get("payload").getAsString() : "")));

    private static final StepKind CANCEL = define(
            "cancel",
            BASE.require("stream_id", VectorSchema.STREAM_ID),
            (endpoint, step) -> endpoint.cancel(streamId(step.get("stream_id"))));

    /** CLOSE를 보낼 뿐 연결은 끝나지 않는다(규격 3.3절). */
    private static final StepKind CLOSE = define(
            "close",
            BASE.require("error_code", VectorSchema.ERROR_CODE),
            (endpoint, step) -> endpoint.close(errorCode(step.get("error_code"))));

    private static final StepKind ADVANCE = define(
                    "advance",
                    BASE.require("ms", VectorSchema.POSITIVE_U32),
                    (endpoint, step) -> endpoint.advance(step.get("ms").getAsLong()))
            .timeAdvancing();

    private static final StepKind DISCONNECT = define("disconnect", BASE, (endpoint, step) -> endpoint.disconnect())
            .connectionEnding();

    private static final Map<String, Shape> SHAPES_BY_NAME = shapes();

    private final String name;
    private final Shape shape;
    private final BiConsumer<Endpoint, JsonObject> execution;
    private Inbound inbound = Inbound.NONE;
    private boolean advancesTime;
    private boolean endsConnection;
    private boolean sendsPing;
    private String onlyOnTransport;
    private Function<JsonObject, String> unsupportedPart = step -> null;

    private StepKind(String name, Shape shape, BiConsumer<Endpoint, JsonObject> execution) {
        this.name = name;
        this.shape = shape;
        this.execution = execution;
    }

    private static StepKind define(String name, Shape shape, BiConsumer<Endpoint, JsonObject> execution) {
        StepKind kind = new StepKind(name, shape, execution);
        REGISTRY.put(name, kind);
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

    private StepKind onlyOnTransport(String transport) {
        this.onlyOnTransport = transport;
        return this;
    }

    private StepKind unsupportedPart(Function<JsonObject, String> value) {
        this.unsupportedPart = value;
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

    /** 이 단계의 종류. 정의에 없는 종류는 오류다(적재기를 거친 벡터에는 없다). */
    static StepKind of(JsonObject step) {
        String name = step.get(TAG).getAsString();
        StepKind kind = REGISTRY.get(name);
        if (kind == null) {
            throw new IllegalArgumentException("알 수 없는 단계 kind: " + name);
        }
        return kind;
    }

    /** 이 종류를 정의에서 찾는다. 모르는 이름이면 null. */
    static StepKind named(String name) {
        return REGISTRY.get(name);
    }

    String name() {
        return name;
    }

    /** 러너가 이 종류를 실행할 수 있는가. */
    boolean isSupported() {
        return execution != null;
    }

    /** 단계를 엔드포인트에 적용한다. */
    void execute(Endpoint endpoint, JsonObject step) {
        execution.accept(endpoint, step);
    }

    /** 지원 종류여도 러너가 실행하지 못하는 단계 내용(예: request의 확장). 없으면 null. */
    String unsupportedPart(JsonObject step) {
        return unsupportedPart.apply(step);
    }

    Inbound inbound() {
        return inbound;
    }

    /** 단계의 {@code ms}만큼 시간을 진행하는가. */
    boolean advancesTime() {
        return advancesTime;
    }

    /** 이 단계만으로 연결이 끝나는가(상대가 끊었다). */
    boolean endsConnection() {
        return endsConnection;
    }

    /** 단계가 PING을 보내며 그 payload가 단계의 {@code payload}인가. */
    boolean sendsPing() {
        return sendsPing;
    }

    /** 이 종류를 쓸 수 있는 transport. 제한이 없으면 null. */
    String onlyOnTransport() {
        return onlyOnTransport;
    }
}
