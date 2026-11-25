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
                    event.has("stream_id") ? String.valueOf(Protocol.streamId(event.get("stream_id"))) : null,
                    Protocol.nameOfStatus(event.get("status")),
                    event.get("certainty").getAsString()),
            event -> result(
                    event.streamId() == 0 ? null : String.valueOf(event.streamId()),
                    Protocol.nameOfStatus(event.status()),
                    event.certainty().orElseThrow(AssertionError::new).name().toLowerCase(Locale.ROOT)));

    private static final EventKind REQUEST_RECEIVED = new EventKind(
            "request_received",
            Event.Kind.REQUEST_RECEIVED,
            BASE.require("stream_id", VectorSchema.STREAM_ID).require("type", VectorSchema.APP_TYPE),
            event -> requestReceived(
                    Protocol.streamId(event.get("stream_id")), event.get("type").getAsInt()),
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

    private static final Map<String, Shape> SHAPES_BY_NAME = shapes();

    private static Map<String, Shape> shapes() {
        Map<String, Shape> shapes = new LinkedHashMap<>();
        for (EventKind kind : ALL) {
            shapes.put(kind.name, kind.shape);
        }
        return Collections.unmodifiableMap(shapes);
    }

    static Map<String, Shape> shapesByName() {
        return SHAPES_BY_NAME;
    }

    static boolean isSupported(String name) {
        return find(name) != null;
    }

    static String expected(JsonObject event) {
        EventKind kind = find(event.get(TAG).getAsString());
        if (kind == null) {
            throw new IllegalArgumentException("알 수 없는 이벤트: " + event);
        }
        return kind.expected.apply(event);
    }

    private static final java.util.Set<Event.Kind> NOT_IN_VECTOR_FORMAT =
            java.util.EnumSet.of(Event.Kind.AUTHENTICATED, Event.Kind.REQUEST_RESET);

    static String actual(Event event) {
        if (NOT_IN_VECTOR_FORMAT.contains(event.kind())) {
            return null;
        }
        for (EventKind kind : ALL) {
            if (kind.coreKind == event.kind()) {
                return kind.actual.apply(event);
            }
        }
        throw new AssertionError("정의 없는 이벤트 종류: " + event.kind());
    }

    private static EventKind find(String name) {
        for (EventKind kind : ALL) {
            if (kind.name.equals(name)) {
                return kind;
            }
        }
        return null;
    }

    private static String result(String streamId, String status, String certainty) {
        return "result stream_id=" + (streamId == null ? "-" : streamId) + " status=" + status + " certainty="
                + certainty;
    }

    private static String requestReceived(long streamId, int type) {
        return "request_received stream_id=" + streamId + " type=" + type;
    }
}
