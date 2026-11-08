package io.github.ppzxc.strand.core.internal;

import java.util.HashSet;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/** 엔드포인트의 설정: 보내는 HELLO의 파라미터(규격 3.2.1절)와 확인 대기 시간. 값을 바꾼 복사본을 돌려주는 불변 객체다. */
public final class EndpointConfig {

    private static final long DEFAULT_ACK_TIMEOUT_MS = 5000L;
    /** 인증이 필요 없는 연결의 메커니즘(규격 3.3.1절). */
    static final String ANONYMOUS = "ANONYMOUS";

    private final long maxFrameSize;
    private final long maxConcurrentStreams;
    private final long capabilities;
    private final long ackTimeoutMs;
    /** 이 시간 동안 받은 프레임이 없으면 PING을 보낸다(규격 3.4.1절). 0이면 유휴 PING을 보내지 않는다. */
    private final long idleIntervalMs;
    /** 구현이 아는 애플리케이션 {@code type}. null이면 모든 type을 안다(규격 4.1절). */
    private final @Nullable Set<Integer> knownTypes;
    /** 다음에 쓸 스트림 번호. null이면 역할의 첫 번호다(규격 2.2.5절). */
    private final @Nullable Long nextStreamId;
    /** WebSocket 위에서 쓴다(규격 1.2절). */
    private final boolean webSocket;
    /** 클라이언트가 {@code AUTH}로 보낼 메커니즘(규격 3.3.1절). */
    private final String authMechanism;
    /** 클라이언트가 {@code AUTH}로 보낼 자격 증명. */
    private final byte[] authCredentials;
    /** 서버가 받은 {@code AUTH}를 넘길 애플리케이션. */
    private final Authenticator authenticator;

    private EndpointConfig(ConfigDraft draft) {
        this.maxFrameSize = draft.maxFrameSize;
        this.maxConcurrentStreams = draft.maxConcurrentStreams;
        this.capabilities = draft.capabilities;
        this.ackTimeoutMs = draft.ackTimeoutMs;
        this.idleIntervalMs = draft.idleIntervalMs;
        this.knownTypes = draft.knownTypes;
        this.nextStreamId = draft.nextStreamId;
        this.webSocket = draft.webSocket;
        this.authMechanism = draft.authMechanism;
        this.authCredentials = draft.authCredentials;
        this.authenticator = draft.authenticator;
    }

    /** 설정 값의 모음. 복사본을 만들 때 한 필드만 고치는 데 쓴다. 필드를 더해도 생성자 인자 나열은 늘지 않는다. */
    private static final class ConfigDraft {
        long maxFrameSize = HelloParams.DEFAULT_MAX_FRAME_SIZE;
        long maxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
        long capabilities;
        long ackTimeoutMs = DEFAULT_ACK_TIMEOUT_MS;
        long idleIntervalMs;

        @Nullable
        Set<Integer> knownTypes;

        @Nullable
        Long nextStreamId;

        boolean webSocket;
        String authMechanism = ANONYMOUS;
        byte[] authCredentials = new byte[0];
        Authenticator authenticator = EndpointConfig::passesAnonymousOnly;
    }

    private ConfigDraft draft() {
        ConfigDraft draft = new ConfigDraft();
        draft.maxFrameSize = maxFrameSize;
        draft.maxConcurrentStreams = maxConcurrentStreams;
        draft.capabilities = capabilities;
        draft.ackTimeoutMs = ackTimeoutMs;
        draft.idleIntervalMs = idleIntervalMs;
        draft.knownTypes = knownTypes;
        draft.nextStreamId = nextStreamId;
        draft.webSocket = webSocket;
        draft.authMechanism = authMechanism;
        draft.authCredentials = authCredentials;
        draft.authenticator = authenticator;
        return draft;
    }

    /**
     * 규격 기본값의 설정.
     *
     * @return 기본 설정
     */
    public static EndpointConfig defaults() {
        return new EndpointConfig(new ConfigDraft());
    }

    /**
     * 받을 수 있는 프레임의 최대 크기를 바꾼 복사본.
     *
     * @param value 바이트 수
     * @return 새 설정
     */
    public EndpointConfig maxFrameSize(long value) {
        ConfigDraft draft = draft();
        draft.maxFrameSize = value;
        return new EndpointConfig(draft);
    }

    /**
     * 상대가 동시에 열 수 있는 스트림 수를 바꾼 복사본.
     *
     * @param value 스트림 수
     * @return 새 설정
     */
    public EndpointConfig maxConcurrentStreams(long value) {
        ConfigDraft draft = draft();
        draft.maxConcurrentStreams = value;
        return new EndpointConfig(draft);
    }

    /**
     * 기능 비트셋을 바꾼 복사본.
     *
     * @param value 비트셋
     * @return 새 설정
     */
    public EndpointConfig capabilities(long value) {
        ConfigDraft draft = draft();
        draft.capabilities = value;
        return new EndpointConfig(draft);
    }

    /**
     * HELLO나 PING을 보내고 ACK를 기다리는 시간(확인 대기, 규격 3.3절)을 바꾼 복사본. 규격은 값을 정하지 않는다.
     *
     * @param value 밀리초
     * @return 새 설정
     */
    public EndpointConfig ackTimeoutMs(long value) {
        ConfigDraft draft = draft();
        draft.ackTimeoutMs = value;
        return new EndpointConfig(draft);
    }

    /**
     * 유휴 간격(규격 3.4.1절)을 바꾼 복사본. 이 시간 동안 상대에게서 받은 프레임이 없으면 PING을 보낸다. 정하지 않으면(0) 유휴 PING을
     * 보내지 않는다. 규격은 값을 정하지 않는다.
     *
     * @param value 밀리초. 0이면 유휴 PING을 끈다
     * @return 새 설정
     */
    public EndpointConfig idleIntervalMs(long value) {
        ConfigDraft draft = draft();
        draft.idleIntervalMs = value;
        return new EndpointConfig(draft);
    }

    /**
     * 구현이 아는 애플리케이션 type 목록을 정한 복사본. 정하지 않으면 모든 type을 안다. 모르는 type의 새 요청에는
     * {@code UNIMPLEMENTED}로 답한다(규격 4.1절).
     *
     * @param types 아는 type들
     * @return 새 설정
     */
    public EndpointConfig knownTypes(Set<Integer> types) {
        ConfigDraft draft = draft();
        draft.knownTypes = new HashSet<>(types);
        return new EndpointConfig(draft);
    }

    /**
     * 내 번호 공간에서 다음에 쓸 스트림 번호를 정한 복사본. 번호가 소진되는 경우를 만들어 볼 때 쓴다. 정하지 않으면 클라이언트는 1,
     * 서버는 2에서 시작한다(규격 2.2.5절).
     *
     * @param value 다음 스트림 번호. 역할의 번호 공간(클라이언트는 홀수, 서버는 짝수)에 속해야 한다
     * @return 새 설정
     */
    public EndpointConfig nextStreamId(long value) {
        ConfigDraft draft = draft();
        draft.nextStreamId = value;
        return new EndpointConfig(draft);
    }

    /**
     * WebSocket 위에서 쓰는 복사본. 바이트 스트림이 아니라 메시지 하나가 프레임 하나이며, 입력은 {@link Endpoint#receiveMessage}로
     * 넣는다(규격 1.2절).
     *
     * @return 새 설정
     */
    public EndpointConfig webSocket() {
        ConfigDraft draft = draft();
        draft.webSocket = true;
        return new EndpointConfig(draft);
    }

    /**
     * 클라이언트가 {@code AUTH}로 보낼 메커니즘과 자격 증명을 정한 복사본. 정하지 않으면 {@code ANONYMOUS}와 빈 자격 증명이다(규격
     * 3.3.1절).
     *
     * @param mechanism 메커니즘 이름. 대문자·숫자·하이픈·밑줄 1~20자
     * @param credentials 메커니즘이 정한 불투명 자격 증명
     * @return 새 설정
     * @throws IllegalArgumentException 메커니즘이 문법을 어길 때
     */
    public EndpointConfig auth(String mechanism, byte[] credentials) {
        if (!mechanism.matches("[A-Z0-9_-]{1,20}")) {
            throw new IllegalArgumentException("AUTH 메커니즘이 문법(대문자·숫자·하이픈·밑줄 1~20자)을 어긴다: " + mechanism);
        }
        ConfigDraft draft = draft();
        draft.authMechanism = mechanism;
        draft.authCredentials = credentials.clone();
        return new EndpointConfig(draft);
    }

    /**
     * 서버가 받은 {@code AUTH}를 넘길 애플리케이션을 정한 복사본. 정하지 않으면 {@code ANONYMOUS}와 빈 자격 증명만 통과시킨다.
     *
     * @param value 판단하는 애플리케이션
     * @return 새 설정
     */
    public EndpointConfig authenticator(Authenticator value) {
        ConfigDraft draft = draft();
        draft.authenticator = value;
        return new EndpointConfig(draft);
    }

    private static AuthDecision passesAnonymousOnly(String mechanism, byte[] credentials) {
        return mechanism.equals(ANONYMOUS) && credentials.length == 0 ? AuthDecision.PASS : AuthDecision.REJECT;
    }

    String authMechanism() {
        return authMechanism;
    }

    byte[] authCredentials() {
        return authCredentials.clone();
    }

    Authenticator authenticator() {
        return authenticator;
    }

    boolean isWebSocket() {
        return webSocket;
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
