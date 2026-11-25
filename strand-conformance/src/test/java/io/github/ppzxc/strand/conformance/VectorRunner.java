package io.github.ppzxc.strand.conformance;

import static io.github.ppzxc.strand.conformance.Protocol.streamId;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
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

final class VectorRunner {

    private static final Set<String> SUPPORTED_SETUP = new HashSet<>(VectorSchema.setupKeys());

    private static final class AuthScenario implements EndpointDriver.Authentication {
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

        private static AuthScenario anonymous() {
            return new AuthScenario(Protocol.DEFAULT_AUTH_MECHANISM, "", OptionalLong.empty(), false);
        }

        @Override
        public AuthDecision decide(String mechanism, byte[] credentials) {
            if (transientFailure) {
                return AuthDecision.TRANSIENT_FAILURE;
            }
            return mechanism.equals(this.mechanism) && Arrays.equals(credentials, Bytes.fromHex(credentialsHex))
                    ? AuthDecision.PASS
                    : AuthDecision.REJECT;
        }

        @Override
        public OptionalLong credentialLifetimeSeconds() {
            return lifetimeSeconds;
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

    interface Medium {

        List<String> unsupportedSetup(JsonObject setup);

        boolean carriesFrame(JsonObject frame);

        Transmission open(boolean client, boolean webSocket, int maxFrameSize);
    }

    static final Medium DIRECT = new Medium() {
        @Override
        public List<String> unsupportedSetup(JsonObject setup) {
            return Collections.emptyList();
        }

        @Override
        public boolean carriesFrame(JsonObject frame) {
            return !"WEBSOCKET_CLOSE".equals(VectorSchema.frameType(frame));
        }

        @Override
        public Transmission open(boolean client, boolean webSocket, int maxFrameSize) {
            return new DirectTransmission(webSocket);
        }
    };

    private static final class DirectTransmission implements Transmission {
        private final boolean webSocket;

        private DirectTransmission(boolean webSocket) {
            this.webSocket = webSocket;
        }

        @Override
        public void deliver(EndpointDriver driver, byte[] bytes) {
            if (webSocket) {
                driver.receiveUnit(bytes);
            } else {
                driver.receiveStream(bytes);
            }
        }

        @Override
        public void apply(EndpointDriver driver, JsonObject step, Map<String, Long> requestTickets) {
            StepKind.of(step).execute(driver, step, requestTickets);
        }

        @Override
        public List<byte[]> transmit(List<OutboundMessage> sent) {
            return bytesOf(sent);
        }

        @Override
        public List<String> observeAfter(List<Event> events) {
            return Collections.emptyList();
        }
    }

    private final boolean client;
    private final boolean webSocket;
    private final Transmission transmission;

    private final Endpoint endpoint;
    private final EndpointDriver driver;
    private final VirtualCredentialLifetime credentialLifetime;

    VectorRunner(boolean client, boolean webSocket, Endpoint endpoint, OptionalLong credentialLifetimeSeconds) {
        this(
                client,
                webSocket,
                endpoint,
                credentialLifetimeSeconds,
                AuthScenario.anonymous(),
                new DirectTransmission(webSocket));
    }

    private VectorRunner(
            boolean client,
            boolean webSocket,
            Endpoint endpoint,
            OptionalLong credentialLifetimeSeconds,
            AuthScenario auth,
            Transmission transmission) {
        this.client = client;
        this.webSocket = webSocket;
        this.transmission = transmission;
        this.endpoint = endpoint;
        this.driver = EndpointDriver.of(endpoint)
                .authentication(auth)
                .space(CollectionSpace.of(64, 1 << 16))
                .eventBatch(8)
                .eagerEvents(true)
                .messageChannel(webSocket);
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
        return evaluate(vector, vectorId, DIRECT);
    }

    static VectorOutcome evaluate(ConformanceVector vector, String vectorId, Medium medium) {
        List<String> unsupported = unsupportedFeatures(vector, medium);
        if (!unsupported.isEmpty()) {
            return VectorOutcome.unsupported(vectorId, unsupported);
        }
        try {
            runSupported(vector, medium);
            return VectorOutcome.pass(vectorId);
        } catch (AssertionError | RuntimeException failure) {
            return VectorOutcome.fail(vectorId, failure.getMessage());
        }
    }

    private static void runSupported(ConformanceVector vector, Medium medium) {
        JsonObject setup = vector.setup();

        boolean client = setup.get("role").getAsString().equals("client");
        AuthScenario auth = AuthScenario.from(setup);
        boolean webSocket = isWebSocket(setup);
        EndpointConfig config = configOf(setup, webSocket, auth);

        Transmission transmission = medium.open(
                client,
                webSocket,
                Protocol.helloParams(setup.getAsJsonObject("local_hello"))
                        .get("max_frame_size")
                        .intValue());
        VectorRunner runner = new VectorRunner(
                client,
                webSocket,
                client ? Endpoint.client(config) : Endpoint.server(config),
                auth.lifetimeSeconds,
                auth,
                transmission);

        List<OutboundMessage> opening = runner.driver.flush();
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

                sent.addAll(runner.driver.flush(writeLimit(step)));
            }
            runner.credentialLifetime.observeSentMessages(sent);
            boolean startsWithPreface = !opening.isEmpty();
            opening = new ArrayList<>();
            List<Event> events = runner.driver.takeEvents();
            List<String> observed = runner.describeBytes(transmission.transmit(sent), startsWithPreface);
            observed.addAll(transmission.observeAfter(events));
            assertThat(FrameKind.comparable(observed, step.getAsJsonArray("send")))
                    .as("%d번째 단계의 send", number)
                    .isEqualTo(expectedFrames(step));
            assertThat(describe(events)).as("%d번째 단계의 events", number).isEqualTo(expectedEvents(step));
        }
    }

    private static long writeLimit(JsonObject step) {
        return step.has("write_limit_bytes") ? step.get("write_limit_bytes").getAsLong() : Long.MAX_VALUE;
    }

    static List<String> unsupportedFeatures(ConformanceVector vector) {
        return unsupportedFeatures(vector, DIRECT);
    }

    static List<String> unsupportedFeatures(ConformanceVector vector, Medium medium) {
        JsonObject setup = vector.setup();
        List<String> unsupported = new ArrayList<>();
        unsupported.addAll(medium.unsupportedSetup(setup));
        for (String key : setup.keySet()) {
            addIfUnsupported(unsupported, SUPPORTED_SETUP.contains(key), "setup." + key);
        }
        if (setup.has("config")) {
            for (String key : setup.getAsJsonObject("config").keySet()) {
                addIfUnsupported(unsupported, Protocol.CONFIG_DEFAULTS.containsKey(key), "config." + key);
            }
        }
        for (JsonObject step : vector.steps()) {

            addIfUnsupported(
                    unsupported, !(step.has("write_limit_bytes") && isWebSocket(setup)), "request.write_limit_bytes");
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
                addIfUnsupported(unsupported, medium.carriesFrame(frame), "프레임 " + frame.get("type"));
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
            config = config.auth(auth.mechanism, expectedCredentials);
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
            hello = driver.flush();
        }
        assertThat(describeMessages(hello, client)).as("수립: 연결을 연 뒤의 출력").hasSize(client ? 2 : 1);
        deliver(FrameKind.encode(peerHelloFrame));
        List<String> afterPeerHello = describeMessages(driver.flush(), false);
        if (client) {

            assertThat(afterPeerHello)
                    .as("수립: 상대 HELLO에 대한 ACK와 AUTH")
                    .containsExactly(FrameKind.expected(helloAck), FrameKind.expected(authFrame));
        } else {
            assertThat(afterPeerHello).as("수립: 상대 HELLO에 대한 ACK").containsExactly(FrameKind.expected(helloAck));
        }
        deliver(FrameKind.encode(helloAck));
        assertThat(driver.flush()).as("수립: ACK 뒤의 출력").isEmpty();
        if (client) {
            deliver(FrameKind.encode(authAck));
        } else {
            deliver(FrameKind.encode(authFrame));
            assertThat(describeMessages(driver.flush(), false))
                    .as("수립: AUTH에 대한 AUTH(ACK)")
                    .containsExactly(FrameKind.expected(authAck));
        }
        assertThat(driver.flush()).as("수립: AUTH 관문 통과 뒤의 출력").isEmpty();
        assertThat(describe(driver.takeEvents())).as("수립: 이벤트").isEmpty();
    }

    private void deliver(byte[] bytes) {
        transmission.deliver(driver, bytes);
    }

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
        transmission.apply(driver, step, requestTickets);
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
            return bytes.length >= Protocol.HEADER_SIZE
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

            if (text != null) {
                result.add(text);
            }
        }
        return result;
    }

    List<String> describeMessages(List<OutboundMessage> messages, boolean startsWithPreface) {
        return describeBytes(bytesOf(messages), startsWithPreface);
    }

    private static List<byte[]> bytesOf(List<OutboundMessage> messages) {
        List<byte[]> bytes = new ArrayList<>();
        for (OutboundMessage message : messages) {
            bytes.add(message.bytes());
        }
        return bytes;
    }

    private List<String> describeBytes(List<byte[]> messages, boolean startsWithPreface) {
        List<String> result = new ArrayList<>();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        boolean first = true;
        for (byte[] message : messages) {
            boolean withPreface = startsWithPreface && first;
            first = false;
            if (webSocket) {
                List<String> frames = FrameKind.describe(message, withPreface, client);
                assertThat(frames).as("WebSocket 메시지마다 프레임이 하나").hasSize(1);
                result.addAll(frames);
            } else {
                stream.write(message, 0, message.length);
            }
        }
        result.addAll(FrameKind.describe(stream.toByteArray(), startsWithPreface && result.isEmpty(), client));
        return result;
    }
}
