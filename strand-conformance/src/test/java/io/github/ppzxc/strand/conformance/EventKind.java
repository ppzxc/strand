package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * 벡터 형식이 정한 이벤트 종류의 단일 정의. 종류마다 형태(필드 규칙), 기대 JSON 렌더링, 실제 {@link Event} 렌더링을 함께 갖는다. 두
 * 렌더링은 종류마다 하나의 형식 함수를 거치므로 같은 문자열을 낸다. 렌더링은 벡터 형식이 정한 필드만 그리고, 벡터 형식에 없는
 * 이벤트(AUTHENTICATED)는 그리지 않는다.
 */
final class EventKind {

    static final String TAG = "event";

    private static final Shape BASE = Shape.EMPTY.require(TAG, FieldCheck.DEFERRED);

    private static final EventKind RESULT = new EventKind(
            "result",
            Event.Kind.RESULT,
            BASE.allow("stream_id", VectorSchema.STREAM_ID)
                    .require("status", VectorSchema.STATUS)
                    .require("certainty", FieldCheck.oneOf(VectorSchema.CERTAINTIES)),
            event -> result(
                    event.has("stream_id") ? String.valueOf(VectorRunner.streamId(event.get("stream_id"))) : null,
                    VectorRunner.nameOfStatus(event.get("status")),
                    event.get("certainty").getAsString()),
            // 스트림을 열기 전에 로컬에서 실패한 요청은 stream_id 없는 result다(벡터 형식 6.2절). core는 번호 0으로 알린다.
            event -> result(
                    event.streamId() == 0 ? null : String.valueOf(event.streamId()),
                    VectorRunner.nameOfStatus(event.status()),
                    event.certainty().orElseThrow(AssertionError::new).name().toLowerCase(Locale.ROOT)));

    private static final EventKind REQUEST_RECEIVED = new EventKind(
            "request_received",
            Event.Kind.REQUEST_RECEIVED,
            BASE.require("stream_id", VectorSchema.STREAM_ID).require("type", VectorSchema.APP_TYPE),
            event -> requestReceived(
                    VectorRunner.streamId(event.get("stream_id")),
                    event.get("type").getAsInt()),
            event -> requestReceived(event.streamId(), event.type()));

    private static final EventKind CLOSED =
            new EventKind("closed", Event.Kind.CLOSED, BASE, event -> "closed", event -> "closed");

    private static final List<EventKind> ALL = Arrays.asList(RESULT, REQUEST_RECEIVED, CLOSED);

    private final String name;
    private final Event.Kind coreKind;
    private final Shape shape;
    private final Function<JsonObject, String> expected;
    private final Function<Event, String> actual;

    private EventKind(
            String name,
            Event.Kind coreKind,
            Shape shape,
            Function<JsonObject, String> expected,
            Function<Event, String> actual) {
        this.name = name;
        this.coreKind = coreKind;
        this.shape = shape;
        this.expected = expected;
        this.actual = actual;
    }

    /** 형태 검사 표: 이벤트 이름마다 형태. */
    static Map<String, Shape> shapesByName() {
        Map<String, Shape> shapes = new LinkedHashMap<>();
        for (EventKind kind : ALL) {
            shapes.put(kind.name, kind.shape);
        }
        return Collections.unmodifiableMap(shapes);
    }

    /** 러너가 비교할 수 있는 이벤트인가. */
    static boolean isSupported(String name) {
        return find(name) != null;
    }

    /** 벡터의 이벤트 항목을 기대 표기로. */
    static String expected(JsonObject event) {
        EventKind kind = find(event.get(TAG).getAsString());
        if (kind == null) {
            throw new IllegalArgumentException("알 수 없는 이벤트: " + event);
        }
        return kind.expected.apply(event);
    }

    /** 실제 이벤트를 같은 표기로. 벡터 형식에 없는 이벤트(AUTHENTICATED 등)는 null이다. */
    static String actual(Event event) {
        for (EventKind kind : ALL) {
            if (kind.coreKind == event.kind()) {
                return kind.actual.apply(event);
            }
        }
        return null;
    }

    private static EventKind find(String name) {
        for (EventKind kind : ALL) {
            if (kind.name.equals(name)) {
                return kind;
            }
        }
        return null;
    }

    /** streamId가 null이면 스트림 없는(로컬) result다. */
    private static String result(String streamId, String status, String certainty) {
        return "result stream_id=" + (streamId == null ? "-" : streamId) + " status=" + status + " certainty="
                + certainty;
    }

    private static String requestReceived(long streamId, int type) {
        return "request_received stream_id=" + streamId + " type=" + type;
    }
}
