package io.github.ppzxc.strand.conformance;

import static io.github.ppzxc.strand.conformance.Protocol.streamId;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * 벡터 하나를 엔드포인트에 실행한다. `setup`으로 엔드포인트를 만들고 단계마다 입력을 넣어, 보낸 바이트를 프레임으로 풀어 `send`와,
 * 애플리케이션 이벤트를 `events`와 순서까지 비교한다(벡터 형식 문서 6절, 7절). 엔드포인트의 외부 동작만 본다.
 *
 * <p>실행할 수 없는 설정·단계·프레임이 하나라도 있으면 아무것도 실행하지 않고 실패한다. 건너뛰거나 통과로 위장하지 않으므로 skipped는 늘 0이다.
 */
final class VectorRunner {

    /**
     * 러너가 지원하는 setup 키. 형태 검사의 setup 키 목록에서 나오므로 스키마가 허용하는 키는 모두 지원으로 센다. 이 검사가 스키마와의
     * 어긋남을 잡아 주지는 않는다. 스키마에 키를 더하면 {@link #configOf}가 그 키를 읽게 함께 고쳐야 한다.
     */
    private static final Set<String> SUPPORTED_SETUP = new HashSet<>(VectorSchema.setupKeys());

    private static final class AuthScenario {
        private final String mechanism;
        private final String credentialsHex;
        private final OptionalLong lifetimeSeconds;
        private final boolean transientFailure;

        private AuthScenario(
                String mechanism, String credentialsHex, OptionalLong lifetimeSeconds, boolean transientFailure) {
            this.mechanism = mechanism;
            this.credentialsHex = credentialsHex;
            this.lifetimeSeconds = lifetimeSeconds;
            this.transientFailure = transientFailure;
        }

        private static AuthScenario from(JsonObject setup) {
            JsonObject auth = setup.has("auth") ? setup.getAsJsonObject("auth") : null;
            String mechanism = auth != null && auth.has("mechanism")
                    ? auth.get("mechanism").getAsString()
                    : Protocol.DEFAULT_AUTH_MECHANISM;
            String credentialsHex = auth != null && auth.has("credentials")
                    ? auth.get("credentials").getAsString()
                    : "";
            OptionalLong lifetimeSeconds = auth != null && auth.has("lifetime_seconds")
                    ? OptionalLong.of(auth.get("lifetime_seconds").getAsLong())
                    : OptionalLong.empty();
            boolean transientFailure = auth != null
                    && auth.has("decision")
                    && "transient_failure".equals(auth.get("decision").getAsString());
            return new AuthScenario(mechanism, credentialsHex, lifetimeSeconds, transientFailure);
        }
    }

    private final boolean client;
    private final boolean webSocket;
    private final Endpoint endpoint;
    private final VirtualCredentialLifetime credentialLifetime;

    VectorRunner(boolean client, boolean webSocket, Endpoint endpoint, OptionalLong credentialLifetimeSeconds) {
        this.client = client;
        this.webSocket = webSocket;
        this.endpoint = endpoint;
        this.credentialLifetime = new VirtualCredentialLifetime(credentialLifetimeSeconds);
    }

    static void run(ConformanceVector vector) {
        VectorOutcome outcome = evaluate(vector);
        assertThat(outcome.status())
                .as("벡터 %s: %s", vector.id(), outcome.details())
                .isEqualTo(VectorOutcome.Status.PASS);
    }

    static VectorOutcome evaluate(ConformanceVector vector) {
        return evaluate(vector, vector.id());
    }

    static VectorOutcome evaluate(ConformanceVector vector, String vectorId) {
        List<String> unsupported = unsupportedFeatures(vector);
        if (!unsupported.isEmpty()) {
            return VectorOutcome.unsupported(vectorId, unsupported);
        }
        try {
            runSupported(vector);
            return VectorOutcome.pass(vectorId);
        } catch (AssertionError | RuntimeException failure) {
            return VectorOutcome.fail(vectorId, failure.getMessage());
        }
    }

    private static void runSupported(ConformanceVector vector) {
        JsonObject setup = vector.setup();

        boolean client = setup.get("role").getAsString().equals("client");
        AuthScenario auth = AuthScenario.from(setup);
        boolean webSocket = isWebSocket(setup);
        EndpointConfig config = configOf(setup, webSocket, auth);
        VectorRunner runner = new VectorRunner(
                client, webSocket, client ? Endpoint.client(config) : Endpoint.server(config), auth.lifetimeSeconds);
        // 클라이언트 역할 구현은 연결이 열리자마자 서문과 HELLO를 내보낸다. 수립하지 않는 벡터에서는 첫 단계의 send에 합쳐 비교한다.
        List<OutboundMessage> opening = runner.endpoint.drainMessages();
        if (setup.get("established").getAsBoolean()) {
            runner.establish(opening, setup.getAsJsonObject("peer_hello"), auth);
            runner.credentialLifetime.startAtCurrentTime();
            opening = new ArrayList<>();
        }
        int number = 0;
        Map<String, Long> requestTickets = new HashMap<>();
        for (JsonObject step : vector.steps()) {
            number++;
            runner.apply(step, requestTickets);
            runner.advanceVirtualCredentialClock(step);
            List<OutboundMessage> sent = new ArrayList<>(opening);
            if (!StepKind.of(step).holdsOutput(step)) {
                sent.addAll(runner.endpoint.drainMessages());
            }
            runner.credentialLifetime.observeSentMessages(sent);
            boolean startsWithPreface = !opening.isEmpty();
            opening = new ArrayList<>();
            List<Event> events = runner.endpoint.drainEvents();
            List<String> observed = runner.describeMessages(sent, startsWithPreface);
            if (runner.webSocketCloses(events)) {
                observed.add(FrameKind.webSocketClose(Protocol.NORMAL_CLOSURE));
            }
            assertThat(FrameKind.comparable(observed, step.getAsJsonArray("send")))
                    .as("%d번째 단계의 send", number)
                    .isEqualTo(expectedFrames(step));
            assertThat(describe(events)).as("%d번째 단계의 events", number).isEqualTo(expectedEvents(step));
        }
    }

    /** 실행하지 못하는 부분이 있으면 벡터를 실패시킨다(건너뛰지 않는다). 한 단계도 실행하기 전에 판정한다. */
    static List<String> unsupportedFeatures(ConformanceVector vector) {
        return unsupportedFeatures(vector, vector.setup());
    }

    private static List<String> unsupportedFeatures(ConformanceVector vector, JsonObject setup) {
        List<String> unsupported = new ArrayList<>();
        for (String key : setup.keySet()) {
            addIfUnsupported(unsupported, SUPPORTED_SETUP.contains(key), "setup." + key);
        }
        if (setup.has("config")) {
            for (String key : setup.getAsJsonObject("config").keySet()) {
                addIfUnsupported(unsupported, Protocol.CONFIG_DEFAULTS.containsKey(key), "config." + key);
            }
        }
        for (JsonObject step : vector.steps()) {
            String kindName = step.get(StepKind.TAG).getAsString();
            StepKind kind = StepKind.named(kindName);
            addIfUnsupported(unsupported, kind != null, "단계 " + kindName);
            if (kind != null) {
                for (String unsupportedPart : kind.unsupportedParts(step)) {
                    addIfUnsupported(unsupported, false, unsupportedPart);
                }
            }
            for (JsonElement element : step.getAsJsonArray("send")) {
                JsonObject frame = element.getAsJsonObject();
                addIfUnsupported(unsupported, FrameKind.isSupported(frame), "프레임 " + frame.get("type"));
                addIfUnsupported(unsupported, FrameKind.hasSupportedFlags(frame), "프레임 flags");
            }
            for (JsonElement event : step.getAsJsonArray("events")) {
                addIfUnsupported(
                        unsupported,
                        EventKind.isSupported(
                                event.getAsJsonObject().get("event").getAsString()),
                        "이벤트 " + event);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(unsupported));
    }

    private static void addIfUnsupported(List<String> unsupported, boolean supported, String what) {
        if (!supported && what != null && !unsupported.contains(what)) {
            unsupported.add(what);
        }
    }

    private static boolean isWebSocket(JsonObject setup) {
        return setup.has("transport") && setup.get("transport").getAsString().equals(VectorSchema.WEBSOCKET);
    }

    private static EndpointConfig configOf(JsonObject setup, boolean webSocket, AuthScenario auth) {
        Map<String, Long> hello = Protocol.helloParams(setup.getAsJsonObject("local_hello"));
        long ackTimeout = Protocol.configLimit(setup, Protocol.ACK_TIMEOUT_MS);
        long authTimeout = Protocol.configLimit(setup, Protocol.AUTH_TIMEOUT_MS);
        EndpointConfig config = EndpointConfig.defaults()
                .idleIntervalMs(0)
                .maxFrameSize(hello.get("max_frame_size"))
                .maxConcurrentStreams(hello.get("max_concurrent_streams"))
                .capabilities(hello.get("capabilities"))
                .ackTimeoutMs(ackTimeout)
                .authTimeoutMs(authTimeout);
        if (setup.has("next_stream_id")) {
            config = config.nextStreamId(streamId(setup.get("next_stream_id")));
        }
        if (setup.has("auth")) {
            byte[] expectedCredentials = Bytes.fromHex(auth.credentialsHex);
            config = config.auth(auth.mechanism, expectedCredentials).authenticator((mechanism, credentials) -> {
                if (auth.transientFailure) {
                    return AuthDecision.TRANSIENT_FAILURE;
                }
                return mechanism.equals(auth.mechanism) && Arrays.equals(credentials, expectedCredentials)
                        ? AuthDecision.PASS
                        : AuthDecision.REJECT;
            });
            if (auth.lifetimeSeconds.isPresent()) {
                config = config.credentialLifetimeSeconds(auth.lifetimeSeconds.getAsLong());
            }
        }
        if (setup.has("known_types")) {
            Set<Integer> known = new HashSet<>();
            for (JsonElement type : setup.getAsJsonArray("known_types")) {
                known.add(type.getAsInt());
            }
            config = config.knownTypes(known);
        }
        return config;
    }

    /**
     * 규격 3.3절 표준 수립 순서(HELLO 교환 뒤 AUTH 왕복)를 실행하고, 성공을 확인한 뒤 출력과 이벤트를 비운다. 메커니즘과 자격 증명은
     * {@code setup.auth}에서 정했다.
     */
    private void establish(List<OutboundMessage> opening, JsonObject peerHello, AuthScenario auth) {
        JsonObject peerHelloFrame = control("HELLO", false);
        if (peerHello != null) {
            peerHelloFrame.add("params", peerHello);
        }
        JsonObject helloAck = control("HELLO", true);
        JsonObject authAck = control("AUTH", true);
        if (auth.lifetimeSeconds.isPresent()) {
            JsonObject params = new JsonObject();
            params.addProperty("credential_lifetime", auth.lifetimeSeconds.getAsLong());
            authAck.add("params", params);
        }
        JsonObject authFrame = control("AUTH", false);
        authFrame.addProperty("mechanism", auth.mechanism);
        authFrame.addProperty("credentials", auth.credentialsHex);

        List<OutboundMessage> hello = opening;
        if (!client) {
            deliver(FrameKind.encode(control("PREFACE", false)));
            hello = endpoint.drainMessages();
        }
        assertThat(describeMessages(hello, client)).as("수립: 연결을 연 뒤의 출력").hasSize(client ? 2 : 1);
        deliver(FrameKind.encode(peerHelloFrame));
        List<String> afterPeerHello = describeMessages(endpoint.drainMessages(), false);
        if (client) {
            // 클라이언트는 서버 HELLO에 ACK를 보낸 직후 AUTH를 보낸다(규격 3.3절 4번).
            assertThat(afterPeerHello)
                    .as("수립: 상대 HELLO에 대한 ACK와 AUTH")
                    .containsExactly(FrameKind.expected(helloAck), FrameKind.expected(authFrame));
        } else {
            assertThat(afterPeerHello).as("수립: 상대 HELLO에 대한 ACK").containsExactly(FrameKind.expected(helloAck));
        }
        deliver(FrameKind.encode(helloAck));
        assertThat(endpoint.drainMessages()).as("수립: ACK 뒤의 출력").isEmpty();
        if (client) {
            deliver(FrameKind.encode(authAck));
        } else {
            deliver(FrameKind.encode(authFrame));
            assertThat(describeMessages(endpoint.drainMessages(), false))
                    .as("수립: AUTH에 대한 AUTH(ACK)")
                    .containsExactly(FrameKind.expected(authAck));
        }
        assertThat(endpoint.drainMessages()).as("수립: AUTH 관문 통과 뒤의 출력").isEmpty();
        assertThat(describe(endpoint.drainEvents())).as("수립: 이벤트").isEmpty();
    }

    /**
     * WebSocket 전송 모듈이 WebSocket Close 1000을 보낼지. core의 전송 종료 지시가 닫으라고 하고(남은 출력을 다 넘겼으므로 이 단계에서 끝난
     * 연결이다) 종료 원인이 {@code NO_ERROR} 연결 종료일 때다. core는 WebSocket을 모르므로 이 판단은 전송 모듈의 일이고, 예제의 WebSocket
     * 통로가 같은 판단을 한다. 벡터의 {@code WEBSOCKET_CLOSE} 기대 항목은 이 판단으로 확인한다.
     */
    private boolean webSocketCloses(List<Event> events) {
        if (!webSocket || endpoint.transportClose() == Endpoint.TransportClose.NONE) {
            return false;
        }
        for (Event event : events) {
            if (event.kind() == Event.Kind.CLOSED
                    && event.closeReason()
                            .map(r -> r.kind() == CloseReason.Kind.CLOSE)
                            .orElse(false)) {
                return true;
            }
        }
        return false;
    }

    /** 수립에 쓰는 입력 하나를 전송 방식에 맞게 넣는다. WebSocket이면 메시지 하나로, 바이트 스트림이면 바이트로 넣는다. */
    private void deliver(byte[] bytes) {
        if (webSocket) {
            endpoint.receiveUnit(bytes, 0, bytes.length, CollectionSpace.UNLIMITED);
        } else {
            endpoint.receive(bytes);
        }
    }

    /** 수립 스크립트에 쓰는 기대 프레임 항목. 프레임 인코딩과 렌더링은 {@link FrameKind}가 한다. */
    private static JsonObject control(String type, boolean ack) {
        JsonObject frame = new JsonObject();
        frame.addProperty("type", type);
        if (ack) {
            JsonArray flags = new JsonArray();
            flags.add("ACK");
            frame.add("flags", flags);
        }
        return frame;
    }

    private void apply(JsonObject step, Map<String, Long> requestTickets) {
        StepKind.of(step).execute(endpoint, step, requestTickets);
    }

    private void advanceVirtualCredentialClock(JsonObject step) {
        if (StepKind.of(step).advancesTime()) {
            if (credentialLifetime.advanceTime(StepKind.of(step).timeAdvanceMilliseconds(step))
                    == ExpiryTransition.EXPIRED) {
                endpoint.credentialExpired();
            }
        }
    }

    private static final class VirtualCredentialLifetime {
        private final OptionalLong lifetimeMs;
        private long nowMs;
        private long expiresAtMs;
        private boolean running;

        private VirtualCredentialLifetime(OptionalLong lifetimeSeconds) {
            this.lifetimeMs = lifetimeSeconds.isPresent()
                    ? OptionalLong.of(lifetimeSeconds.getAsLong() * 1000)
                    : OptionalLong.empty();
        }

        private ExpiryTransition advanceTime(long milliseconds) {
            nowMs += milliseconds;
            if (running && nowMs > expiresAtMs) {
                running = false;
                return ExpiryTransition.EXPIRED;
            }
            return ExpiryTransition.NONE;
        }

        private void observeSentMessages(List<OutboundMessage> messages) {
            if (messages.stream().anyMatch(VirtualCredentialLifetime::isAuthAck)) {
                startAtCurrentTime();
            }
        }

        private void startAtCurrentTime() {
            if (lifetimeMs.isPresent()) {
                expiresAtMs = nowMs + lifetimeMs.getAsLong();
                running = true;
            }
        }

        private static boolean isAuthAck(OutboundMessage message) {
            byte[] bytes = message.bytes();
            return message.kind() == OutboundMessage.Kind.BINARY
                    && bytes.length >= Protocol.HEADER_SIZE
                    && Protocol.frameType(bytes) == Protocol.TYPE_AUTH
                    && (Protocol.frameFlags(bytes) & Protocol.ACK_FLAG) != 0;
        }
    }

    private enum ExpiryTransition {
        NONE,
        EXPIRED
    }

    private static List<String> expectedFrames(JsonObject step) {
        List<String> result = new ArrayList<>();
        for (JsonElement element : step.getAsJsonArray("send")) {
            JsonObject frame = element.getAsJsonObject();
            result.add(FrameKind.expected(frame));
        }
        return result;
    }

    private static List<String> expectedEvents(JsonObject step) {
        List<String> result = new ArrayList<>();
        for (JsonElement element : step.getAsJsonArray("events")) {
            JsonObject event = element.getAsJsonObject();
            result.add(EventKind.expected(event));
        }
        return result;
    }

    private static List<String> describe(List<Event> events) {
        List<String> result = new ArrayList<>();
        for (Event event : events) {
            String text = EventKind.actual(event);
            // 벡터 형식에 없는 이벤트(AUTHENTICATED, 6.2절)는 그리지 않는다. credential_lifetime 통지는 core 테스트가 본다.
            if (text != null) {
                result.add(text);
            }
        }
        return result;
    }

    /**
     * 엔드포인트가 보낸 메시지를 벡터의 `send`와 같은 표기로 바꾼다. WebSocket에서는 바이너리 메시지마다 프레임이 정확히 하나여야 한다
     * (벡터 형식 6.1절). WebSocket Close는 core의 출력이 아니라 {@link #webSocketCloses}가 판단한다. 바이트 스트림에서는 메시지를 이어 붙여 프레임으로 푼다.
     */
    List<String> describeMessages(List<OutboundMessage> messages, boolean startsWithPreface) {
        List<String> result = new ArrayList<>();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        boolean first = true;
        for (OutboundMessage message : messages) {
            boolean withPreface = startsWithPreface && first;
            first = false;
            if (webSocket) {
                List<String> frames = FrameKind.describe(message.bytes(), withPreface, client);
                assertThat(frames).as("WebSocket 메시지마다 프레임이 하나").hasSize(1);
                result.addAll(frames);
            } else {
                byte[] bytes = message.bytes();
                stream.write(bytes, 0, bytes.length);
            }
        }
        result.addAll(FrameKind.describe(stream.toByteArray(), startsWithPreface && result.isEmpty(), client));
        return result;
    }
}
