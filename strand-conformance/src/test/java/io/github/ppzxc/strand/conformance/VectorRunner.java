package io.github.ppzxc.strand.conformance;

import static io.github.ppzxc.strand.conformance.Protocol.streamId;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 벡터 하나를 엔드포인트에 실행한다. `setup`으로 엔드포인트를 만들고 단계마다 입력을 넣어, 보낸 바이트를 프레임으로 풀어 `send`와,
 * 애플리케이션 이벤트를 `events`와 순서까지 비교한다(벡터 형식 문서 6절, 7절). 엔드포인트의 외부 동작만 본다.
 *
 * <p>실행할 수 없는 설정·단계·프레임이 하나라도 있으면 아무것도 실행하지 않고 실패한다. 건너뛰거나 통과로 위장하지 않으므로 skipped는 늘 0이다.
 */
final class VectorRunner {

    /** 러너가 지원하는 setup 키. 형태 검사의 setup 키 목록에서 나온다. */
    private static final Set<String> SUPPORTED_SETUP = new HashSet<>(VectorSchema.setupKeys());
    private final boolean client;
    private final boolean webSocket;
    private final Endpoint endpoint;

    VectorRunner(boolean client, boolean webSocket, Endpoint endpoint) {
        this.client = client;
        this.webSocket = webSocket;
        this.endpoint = endpoint;
    }

    static void run(ConformanceVector vector) {
        JsonObject setup = vector.setup();
        requireSupported(vector, setup);

        boolean client = setup.get("role").getAsString().equals("client");
        String authMechanism = authMechanism(setup);
        String authCredentials = authCredentialsHex(setup);
        boolean webSocket = isWebSocket(setup);
        EndpointConfig config = configOf(setup, webSocket, authMechanism, authCredentials);
        VectorRunner runner =
                new VectorRunner(client, webSocket, client ? Endpoint.client(config) : Endpoint.server(config));
        // 클라이언트 역할 구현은 연결이 열리자마자 서문과 HELLO를 내보낸다. 수립하지 않는 벡터에서는 첫 단계의 send에 합쳐 비교한다.
        List<OutboundMessage> opening = runner.endpoint.drainMessages();
        if (setup.get("established").getAsBoolean()) {
            runner.establish(opening, setup.getAsJsonObject("peer_hello"), authMechanism, authCredentials);
            opening = new ArrayList<>();
        }
        int number = 0;
        for (JsonObject step : vector.steps()) {
            number++;
            runner.apply(step);
            List<OutboundMessage> sent = new ArrayList<>(opening);
            sent.addAll(runner.endpoint.drainMessages());
            boolean startsWithPreface = !opening.isEmpty();
            opening = new ArrayList<>();
            assertThat(runner.describeMessages(sent, startsWithPreface))
                    .as("%d번째 단계의 send", number)
                    .isEqualTo(expectedFrames(step));
            assertThat(describe(runner.endpoint.drainEvents()))
                    .as("%d번째 단계의 events", number)
                    .isEqualTo(expectedEvents(step));
        }
    }

    /** 실행하지 못하는 부분이 있으면 벡터를 건너뛴다. 한 단계도 실행하기 전에 판정한다. */
    private static void requireSupported(ConformanceVector vector, JsonObject setup) {
        for (String key : setup.keySet()) {
            requireSupportedPart(SUPPORTED_SETUP.contains(key), vector, "setup." + key);
        }
        if (setup.has("config")) {
            for (String key : setup.getAsJsonObject("config").keySet()) {
                requireSupportedPart(Protocol.CONFIG_DEFAULTS.containsKey(key), vector, "config." + key);
            }
        }
        for (JsonObject step : vector.steps()) {
            String kindName = step.get(StepKind.TAG).getAsString();
            StepKind kind = StepKind.named(kindName);
            requireSupportedPart(kind != null && kind.isSupported(), vector, "단계 " + kindName);
            String unsupportedPart = kind.unsupportedPart(step);
            requireSupportedPart(unsupportedPart == null, vector, unsupportedPart);
            for (JsonElement element : step.getAsJsonArray("send")) {
                JsonObject frame = element.getAsJsonObject();
                requireSupportedPart(FrameKind.isSupported(frame), vector, "프레임 " + frame.get("type"));
                requireSupportedPart(FrameKind.hasSupportedFlags(frame), vector, "프레임 flags");
            }
            for (JsonElement event : step.getAsJsonArray("events")) {
                requireSupportedPart(
                        EventKind.isSupported(
                                event.getAsJsonObject().get("event").getAsString()),
                        vector,
                        "이벤트 " + event);
            }
        }
    }

    private static void requireSupportedPart(boolean supported, ConformanceVector vector, String what) {
        assertThat(supported).as("러너 미지원: %s (%s)", what, vector.id()).isTrue();
    }

    private static boolean isWebSocket(JsonObject setup) {
        return setup.has("transport") && setup.get("transport").getAsString().equals("websocket");
    }

    /** {@code setup.auth}의 메커니즘. 생략하면 {@code ANONYMOUS}다(벡터 형식 5절). */
    private static String authMechanism(JsonObject setup) {
        JsonObject auth = setup.has("auth") ? setup.getAsJsonObject("auth") : null;
        return auth != null && auth.has("mechanism")
                ? auth.get("mechanism").getAsString()
                : Protocol.DEFAULT_AUTH_MECHANISM;
    }

    /** {@code setup.auth}의 자격 증명(hex). 생략하면 빈 값이다. */
    private static String authCredentialsHex(JsonObject setup) {
        JsonObject auth = setup.has("auth") ? setup.getAsJsonObject("auth") : null;
        return auth != null && auth.has("credentials") ? auth.get("credentials").getAsString() : "";
    }

    private static EndpointConfig configOf(
            JsonObject setup, boolean webSocket, String authMechanism, String authCredentialsHex) {
        Map<String, Long> hello = Protocol.helloParams(setup.getAsJsonObject("local_hello"));
        long ackTimeout = Protocol.configLimit(setup, Protocol.ACK_TIMEOUT_MS);
        long authTimeout = Protocol.configLimit(setup, Protocol.AUTH_TIMEOUT_MS);
        EndpointConfig config = EndpointConfig.defaults()
                .maxFrameSize(hello.get("max_frame_size"))
                .maxConcurrentStreams(hello.get("max_concurrent_streams"))
                .capabilities(hello.get("capabilities"))
                .ackTimeoutMs(ackTimeout)
                .authTimeoutMs(authTimeout);
        if (webSocket) {
            config = config.webSocket();
        }
        if (setup.has("next_stream_id")) {
            config = config.nextStreamId(streamId(setup.get("next_stream_id")));
        }
        if (setup.has("auth")) {
            byte[] expectedCredentials = Bytes.fromHex(authCredentialsHex);
            config = config.auth(authMechanism, expectedCredentials)
                    .authenticator((mechanism, credentials) ->
                            mechanism.equals(authMechanism) && Arrays.equals(credentials, expectedCredentials)
                                    ? AuthDecision.PASS
                                    : AuthDecision.REJECT);
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
     * {@code setup.auth}에서 정했고 없으면 {@code ANONYMOUS}와 빈 자격 증명이다.
     */
    private void establish(List<OutboundMessage> opening, JsonObject peerHello, String mechanism, String credentials) {
        JsonObject peerHelloFrame = control("HELLO", false);
        if (peerHello != null) {
            peerHelloFrame.add("params", peerHello);
        }
        JsonObject helloAck = control("HELLO", true);
        JsonObject authAck = control("AUTH", true);
        JsonObject auth = control("AUTH", false);
        auth.addProperty("mechanism", mechanism);
        auth.addProperty("credentials", credentials);

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
                    .containsExactly(FrameKind.expected(helloAck), FrameKind.expected(auth));
        } else {
            assertThat(afterPeerHello).as("수립: 상대 HELLO에 대한 ACK").containsExactly(FrameKind.expected(helloAck));
        }
        deliver(FrameKind.encode(helloAck));
        assertThat(endpoint.drainMessages()).as("수립: ACK 뒤의 출력").isEmpty();
        if (client) {
            deliver(FrameKind.encode(authAck));
        } else {
            deliver(FrameKind.encode(auth));
            assertThat(describeMessages(endpoint.drainMessages(), false))
                    .as("수립: AUTH에 대한 AUTH(ACK)")
                    .containsExactly(FrameKind.expected(authAck));
        }
        assertThat(endpoint.drainMessages()).as("수립: AUTH 관문 통과 뒤의 출력").isEmpty();
        assertThat(describe(endpoint.drainEvents())).as("수립: 이벤트").isEmpty();
    }

    /** 수립에 쓰는 입력 하나를 전송 방식에 맞게 넣는다. WebSocket이면 메시지 하나로, 바이트 스트림이면 바이트로 넣는다. */
    private void deliver(byte[] bytes) {
        if (webSocket) {
            endpoint.receiveMessage(WebSocketMessageKind.BINARY, bytes);
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

    private void apply(JsonObject step) {
        StepKind.of(step).execute(endpoint, step);
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
     * 엔드포인트가 보낸 메시지를 벡터의 `send`와 같은 표기로 바꾼다. WebSocket에서는 바이너리 메시지마다 프레임이 정확히 하나여야 하고
     * (벡터 형식 6.1절), WebSocket Close는 자기 항목이다. 바이트 스트림에서는 메시지를 이어 붙여 프레임으로 푼다.
     */
    List<String> describeMessages(List<OutboundMessage> messages, boolean startsWithPreface) {
        List<String> result = new ArrayList<>();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        boolean first = true;
        for (OutboundMessage message : messages) {
            if (message.kind() == OutboundMessage.Kind.WEBSOCKET_CLOSE
                    && message.webSocketCloseCode() != Protocol.NORMAL_CLOSURE) {
                // 1000이 아닌 상태 코드는 규격이 정하지 않아 관측하지 않는다(벡터 형식 6.1절).
                continue;
            }
            boolean withPreface = startsWithPreface && first;
            first = false;
            if (message.kind() == OutboundMessage.Kind.WEBSOCKET_CLOSE) {
                result.addAll(FrameKind.describe(stream.toByteArray(), startsWithPreface && result.isEmpty(), client));
                stream.reset();
                result.add(FrameKind.webSocketClose(message.webSocketCloseCode()));
            } else if (webSocket) {
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
