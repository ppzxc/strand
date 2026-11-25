package io.github.ppzxc.strand.core.internal;

import java.util.HashSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

public final class EndpointConfig {

    public static final long MAX_FRAME_SIZE_LOWER_LIMIT = 16_384L;

    public static final long DEFAULT_MAX_FRAME_SIZE = HelloParams.DEFAULT_MAX_FRAME_SIZE;

    private static final long MAX_U32 = 0xFFFF_FFFFL;

    private static final long MAX_TIMEOUT_MS = Long.MAX_VALUE / 2;

    public static final long DEFAULT_ACK_TIMEOUT_MS = 5000L;
    private static final long DEFAULT_AUTH_TIMEOUT_MS = 10000L;

    public static final long DEFAULT_IDLE_INTERVAL_MS = 30_000L;

    public static final String ANONYMOUS = "ANONYMOUS";

    private final long maxFrameSize;
    private final long maxConcurrentStreams;
    private final long capabilities;
    private final long ackTimeoutMs;

    private final long idleIntervalMs;

    private final long authTimeoutMs;

    private final @Nullable Set<Integer> knownTypes;

    private final @Nullable Long nextStreamId;

    private final String authMechanism;

    private final byte[] authCredentials;

    private final OptionalLong credentialLifetimeSeconds;

    private EndpointConfig(ConfigDraft draft) {
        this.maxFrameSize = draft.maxFrameSize;
        this.maxConcurrentStreams = draft.maxConcurrentStreams;
        this.capabilities = draft.capabilities;
        this.ackTimeoutMs = draft.ackTimeoutMs;
        this.idleIntervalMs = draft.idleIntervalMs;
        this.authTimeoutMs = draft.authTimeoutMs;
        this.knownTypes = draft.knownTypes;
        this.nextStreamId = draft.nextStreamId;
        this.authMechanism = draft.authMechanism;
        this.authCredentials = draft.authCredentials;
        this.credentialLifetimeSeconds = draft.credentialLifetimeSeconds;
    }

    private static final class ConfigDraft {
        long maxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;
        long maxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
        long capabilities;
        long ackTimeoutMs = DEFAULT_ACK_TIMEOUT_MS;
        long idleIntervalMs = DEFAULT_IDLE_INTERVAL_MS;
        long authTimeoutMs = DEFAULT_AUTH_TIMEOUT_MS;

        @Nullable
        Set<Integer> knownTypes;

        @Nullable
        Long nextStreamId;

        String authMechanism = ANONYMOUS;
        byte[] authCredentials = new byte[0];

        OptionalLong credentialLifetimeSeconds = OptionalLong.empty();
    }

    private ConfigDraft draft() {
        ConfigDraft draft = new ConfigDraft();
        draft.maxFrameSize = maxFrameSize;
        draft.maxConcurrentStreams = maxConcurrentStreams;
        draft.capabilities = capabilities;
        draft.ackTimeoutMs = ackTimeoutMs;
        draft.idleIntervalMs = idleIntervalMs;
        draft.authTimeoutMs = authTimeoutMs;
        draft.knownTypes = knownTypes;
        draft.nextStreamId = nextStreamId;
        draft.authMechanism = authMechanism;
        draft.authCredentials = authCredentials;
        draft.credentialLifetimeSeconds = credentialLifetimeSeconds;
        return draft;
    }

    public static EndpointConfig defaults() {
        return new EndpointConfig(new ConfigDraft());
    }

    public EndpointConfig maxFrameSize(long value) {
        ConfigDraft draft = draft();
        if (value < MAX_FRAME_SIZE_LOWER_LIMIT || value > HelloParams.MAX_FRAME_SIZE_UPPER_LIMIT) {
            throw new IllegalArgumentException("maxFrameSize는 " + MAX_FRAME_SIZE_LOWER_LIMIT + " 이상 "
                    + HelloParams.MAX_FRAME_SIZE_UPPER_LIMIT + " 이하여야 한다: " + value);
        }
        draft.maxFrameSize = value;
        return new EndpointConfig(draft);
    }

    public EndpointConfig maxConcurrentStreams(long value) {
        ConfigDraft draft = draft();
        if (!isUnsigned32(value)) {
            throw new IllegalArgumentException("maxConcurrentStreams는 0 이상 4294967295 이하여야 한다: " + value);
        }
        draft.maxConcurrentStreams = value;
        return new EndpointConfig(draft);
    }

    public EndpointConfig capabilities(long value) {
        ConfigDraft draft = draft();
        draft.capabilities = value;
        return new EndpointConfig(draft);
    }

    public EndpointConfig ackTimeoutMs(long value) {
        ConfigDraft draft = draft();
        if (!isPositiveTimeoutMs(value)) {
            throw new IllegalArgumentException("ackTimeoutMs는 양수이고 Long.MAX_VALUE/2 이하여야 한다: " + value);
        }
        draft.ackTimeoutMs = value;
        return new EndpointConfig(draft);
    }

    public EndpointConfig idleIntervalMs(long value) {
        ConfigDraft draft = draft();
        if (!isValidIdleIntervalMs(value)) {
            throw new IllegalArgumentException("idleIntervalMs는 0 이상 Long.MAX_VALUE/2 이하여야 한다: " + value);
        }
        draft.idleIntervalMs = value;
        return new EndpointConfig(draft);
    }

    public EndpointConfig authTimeoutMs(long value) {
        ConfigDraft draft = draft();
        if (!isPositiveTimeoutMs(value)) {
            throw new IllegalArgumentException("authTimeoutMs는 양수이고 Long.MAX_VALUE/2 이하여야 한다: " + value);
        }
        draft.authTimeoutMs = value;
        return new EndpointConfig(draft);
    }

    public EndpointConfig knownTypes(Set<Integer> types) {
        ConfigDraft draft = draft();
        draft.knownTypes = new HashSet<>(types);
        return new EndpointConfig(draft);
    }

    public EndpointConfig nextStreamId(long value) {
        ConfigDraft draft = draft();
        if (value < 1) {
            throw new IllegalArgumentException("nextStreamId는 1 이상이어야 한다: " + value);
        }
        draft.nextStreamId = value;
        return new EndpointConfig(draft);
    }

    public EndpointConfig auth(String mechanism, byte[] credentials) {
        if (!isValidAuthMechanism(mechanism)) {
            throw new IllegalArgumentException("AUTH 메커니즘이 문법(대문자·숫자·하이픈·밑줄 1~20자)을 어긴다: " + mechanism);
        }
        ConfigDraft draft = draft();
        draft.authMechanism = mechanism;
        draft.authCredentials = credentials.clone();
        return new EndpointConfig(draft);
    }

    public static boolean isPositiveTimeoutMs(long value) {
        return value > 0 && value <= MAX_TIMEOUT_MS;
    }

    public static boolean isValidIdleIntervalMs(long value) {
        return value >= 0 && value <= MAX_TIMEOUT_MS;
    }

    public static boolean isUnsigned32(long value) {
        return value >= 0 && value <= MAX_U32;
    }

    public EndpointConfig credentialLifetimeSeconds(long value) {
        if (!isUnsigned32(value)) {
            throw new IllegalArgumentException("credentialLifetimeSeconds는 부호 없는 32비트 정수여야 한다: " + value);
        }
        ConfigDraft draft = draft();
        draft.credentialLifetimeSeconds = OptionalLong.of(value);
        return new EndpointConfig(draft);
    }

    static final int MAX_AUTH_MECHANISM_LENGTH = 20;

    private static final Pattern AUTH_MECHANISM_GRAMMAR = Pattern.compile("[A-Z0-9_-]+");

    static boolean isValidAuthMechanism(String mechanism) {
        return mechanism.length() >= 1
                && mechanism.length() <= MAX_AUTH_MECHANISM_LENGTH
                && AUTH_MECHANISM_GRAMMAR.matcher(mechanism).matches();
    }

    String authMechanism() {
        return authMechanism;
    }

    byte[] authCredentials() {
        return authCredentials.clone();
    }

    OptionalLong credentialLifetimeSeconds() {
        return credentialLifetimeSeconds;
    }

    boolean knows(int type) {
        return knownTypes == null || knownTypes.contains(type);
    }

    long firstStreamId(long roleDefault) {
        return nextStreamId == null ? roleDefault : nextStreamId;
    }

    long ackTimeoutMs() {
        return ackTimeoutMs;
    }

    long idleIntervalMs() {
        return idleIntervalMs;
    }

    long authTimeoutMs() {
        return authTimeoutMs;
    }

    long maxFrameSize() {
        return maxFrameSize;
    }

    long maxConcurrentStreams() {
        return maxConcurrentStreams;
    }

    long capabilities() {
        return capabilities;
    }
}
