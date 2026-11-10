package io.github.ppzxc.strand.conformance;

import static io.github.ppzxc.strand.conformance.Protocol.errorCode;
import static io.github.ppzxc.strand.conformance.Protocol.statusCode;
import static io.github.ppzxc.strand.conformance.Protocol.streamId;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 벡터 형식이 정한 단계 종류의 단일 정의. 종류마다 형태(필드 규칙), 실행(엔드포인트에 적용), 지원 여부, 확인 대기 해석용 타이밍
 * 성질(들어오는 바이트인가, 연결을 끝내는가, 시간을 진행하는가)을 함께 갖는다. 형태 검사 표, 러너의 실행·지원 검사, 확인 대기
 * 해석기의 종류 판정이 모두 이 정의에서 나오므로 새 단계 종류는 여기에만 추가한다.
 */
final class StepKind {

    static final String TAG = "kind";
    static final String LABEL = "단계 kind";
    private static final String TCP = "tcp";
    private static final String WEBSOCKET = "websocket";

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

    private static final StepKind RECV = new StepKind(
            "recv",
            BASE.require("hex", Check::hex),
            (endpoint, step) -> endpoint.receive(Bytes.fromHex(step.get("hex").getAsString())),
            Inbound.STREAM,
            false,
            false,
            false,
            TCP);

    private static final StepKind RECV_MESSAGE = new StepKind(
            "recv_message",
            BASE.require("opcode", FieldCheck.oneOf(VectorSchema.OPCODES)).require("hex", Check::hex),
            (endpoint, step) -> endpoint.receiveMessage(
                    step.get("opcode").getAsString().equals("binary")
                            ? WebSocketMessageKind.BINARY
                            : WebSocketMessageKind.TEXT,
                    Bytes.fromHex(step.get("hex").getAsString())),
            Inbound.MESSAGE,
            false,
            false,
            false,
            WEBSOCKET);

    private static final StepKind PING = new StepKind(
            "ping",
            BASE.require("payload", Check::pingPayload),
            (endpoint, step) -> endpoint.ping(Bytes.fromHex(step.get("payload").getAsString())),
            Inbound.NONE,
            false,
            false,
            true,
            null);

    private static final StepKind REQUEST = new StepKind(
            "request",
            BASE.require("type", VectorSchema.APP_TYPE)
                    .allow("extensions", Check::extensions)
                    .allow("payload", Check::hex)
                    .allow("deadline_ms", VectorSchema.POSITIVE_U32),
            (endpoint, step) -> {
                byte[] payload =
                        Bytes.fromHex(step.has("payload") ? step.get("payload").getAsString() : "");
                if (step.has("deadline_ms")) {
                    endpoint.request(
                            step.get("type").getAsInt(),
                            payload,
                            step.get("deadline_ms").getAsLong());
                } else {
                    endpoint.request(step.get("type").getAsInt(), payload);
                }
            },
            Inbound.NONE,
            false,
            false,
            false,
            null);

    private static final StepKind RESPOND = new StepKind(
            "respond",
            BASE.require("stream_id", VectorSchema.STREAM_ID)
                    .require("status", VectorSchema.STATUS)
                    .allow("extensions", Check::extensions)
                    .allow("payload", Check::hex),
            (endpoint, step) -> endpoint.respond(
                    streamId(step.get("stream_id")),
                    statusCode(step.get("status")),
                    Bytes.fromHex(step.has("payload") ? step.get("payload").getAsString() : "")),
            Inbound.NONE,
            false,
            false,
            false,
            null);

    private static final StepKind CANCEL = new StepKind(
            "cancel",
            BASE.require("stream_id", VectorSchema.STREAM_ID),
            (endpoint, step) -> endpoint.cancel(streamId(step.get("stream_id"))),
            Inbound.NONE,
            false,
            false,
            false,
            null);

    /** CLOSE를 보낼 뿐 연결은 끝나지 않는다(규격 3.3절). */
    private static final StepKind CLOSE = new StepKind(
            "close",
            BASE.require("error_code", VectorSchema.ERROR_CODE),
            (endpoint, step) -> endpoint.close(errorCode(step.get("error_code"))),
            Inbound.NONE,
            false,
            false,
            false,
            null);

    private static final StepKind ADVANCE = new StepKind(
            "advance",
            BASE.require("ms", VectorSchema.POSITIVE_U32),
            (endpoint, step) -> endpoint.advance(step.get("ms").getAsLong()),
            Inbound.NONE,
            true,
            false,
            false,
            null);

    private static final StepKind DISCONNECT = new StepKind(
            "disconnect", BASE, (endpoint, step) -> endpoint.disconnect(), Inbound.NONE, false, true, false, null);

    private static final List<StepKind> ALL =
            Arrays.asList(RECV, RECV_MESSAGE, PING, REQUEST, RESPOND, CANCEL, CLOSE, ADVANCE, DISCONNECT);

    private final String name;
    private final Shape shape;
    private final BiConsumer<Endpoint, JsonObject> execution;
    private final Inbound inbound;
    private final boolean advancesTime;
    private final boolean endsConnection;
    private final boolean sendsPing;
    private final String onlyOnTransport;

    private StepKind(
            String name,
            Shape shape,
            BiConsumer<Endpoint, JsonObject> execution,
            Inbound inbound,
            boolean advancesTime,
            boolean endsConnection,
            boolean sendsPing,
            String onlyOnTransport) {
        this.name = name;
        this.shape = shape;
        this.execution = execution;
        this.inbound = inbound;
        this.advancesTime = advancesTime;
        this.endsConnection = endsConnection;
        this.sendsPing = sendsPing;
        this.onlyOnTransport = onlyOnTransport;
    }

    private static final Map<String, StepKind> BY_NAME = byName();
    private static final Map<String, Shape> SHAPES_BY_NAME = shapes();

    private static Map<String, StepKind> byName() {
        Map<String, StepKind> map = new LinkedHashMap<>();
        for (StepKind kind : ALL) {
            map.put(kind.name, kind);
        }
        return Collections.unmodifiableMap(map);
    }

    private static Map<String, Shape> shapes() {
        Map<String, Shape> shapes = new LinkedHashMap<>();
        for (StepKind kind : ALL) {
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
        StepKind kind = BY_NAME.get(name);
        if (kind == null) {
            throw new IllegalArgumentException("알 수 없는 단계 kind: " + name);
        }
        return kind;
    }

    /** 이 종류를 정의에서 찾는다. 모르는 이름이면 null. */
    static StepKind named(String name) {
        return BY_NAME.get(name);
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
