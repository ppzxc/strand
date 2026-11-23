package io.github.ppzxc.strand.core.internal;

import java.util.Optional;
import java.util.OptionalLong;
import org.jspecify.annotations.Nullable;

/** 엔드포인트가 애플리케이션에 전달하는 이벤트(적합성 벡터 형식 6.2절). */
public final class Event {

    /** 응답 {@code type}이 요청과 다를 때 애플리케이션에 넘기는 로컬 status(규격 4.1절). 규격 표의 이름이 아니라 {@code INTERNAL}의 별칭이다. */
    static final int RESPONSE_TYPE_MISMATCH = Status.INTERNAL;

    /** 이벤트 종류. */
    public enum Kind {
        /** 자기가 보낸 요청의 결과가 애플리케이션에 전달됨. */
        RESULT,
        /** 연결이 닫혔음이 애플리케이션에 전달됨. */
        CLOSED,
        /** 상대가 연 스트림의 요청을 받음. */
        REQUEST_RECEIVED,
        /** 클라이언트가 {@code AUTH(ACK)}를 받아 AUTH 관문을 통과함(규격 3.3절). 적합성 벡터에는 나오지 않는다. */
        AUTHENTICATED,
        /** 전송 모듈에 인증 판단을 요청한다. */
        AUTHENTICATION_REQUIRED,
        /** 상대가 애플리케이션에 넘긴 요청의 스트림을 RESET했음(규격 4.4절). 같은 요청의 {@link #REQUEST_RECEIVED} 뒤에 나온다. */
        REQUEST_RESET
    }

    /** {@code AUTH(ACK)}가 자격 증명의 수명을 알리지 않았음을 나타내는 값. */
    private static final long NO_CREDENTIAL_LIFETIME = -1;

    /** {@link Kind#RESULT}가 아니어서 번호표가 없음을 나타내는 값. 실제 번호표는 0 이상이다. */
    private static final long NO_TICKET = -1;

    /** {@link Kind#REQUEST_RESET}가 아니어서 RESET의 오류 코드가 없음을 나타내는 값. 실제 오류 코드는 0 이상이다. */
    private static final long NO_RESET_ERROR_CODE = -1;

    private static final byte[] NO_BODY = new byte[0];

    private final Kind kind;
    private final long streamId;
    private final int status;
    private final int type;
    private final long credentialLifetime;
    private final @Nullable Certainty certainty;
    private final @Nullable CloseReason closeReason;
    private final long ticket;
    private final byte[] body;
    private final byte[] extensions;
    private final @Nullable String authenticationMechanism;
    private final long resetErrorCode;

    private Event(
            Kind kind,
            long streamId,
            int status,
            int type,
            long credentialLifetime,
            @Nullable Certainty certainty,
            @Nullable CloseReason closeReason,
            long ticket,
            byte[] body) {
        this(kind, streamId, status, type, credentialLifetime, certainty, closeReason, ticket, body, null);
    }

    private Event(
            Kind kind,
            long streamId,
            int status,
            int type,
            long credentialLifetime,
            @Nullable Certainty certainty,
            @Nullable CloseReason closeReason,
            long ticket,
            byte[] body,
            @Nullable String authenticationMechanism) {
        this(
                kind,
                streamId,
                status,
                type,
                credentialLifetime,
                certainty,
                closeReason,
                ticket,
                body,
                NO_BODY,
                authenticationMechanism);
    }

    private Event(
            Kind kind,
            long streamId,
            int status,
            int type,
            long credentialLifetime,
            @Nullable Certainty certainty,
            @Nullable CloseReason closeReason,
            long ticket,
            byte[] body,
            byte[] extensions,
            @Nullable String authenticationMechanism) {
        this(
                kind,
                streamId,
                status,
                type,
                credentialLifetime,
                certainty,
                closeReason,
                ticket,
                body,
                extensions,
                authenticationMechanism,
                NO_RESET_ERROR_CODE);
    }

    private Event(
            Kind kind,
            long streamId,
            int status,
            int type,
            long credentialLifetime,
            @Nullable Certainty certainty,
            @Nullable CloseReason closeReason,
            long ticket,
            byte[] body,
            byte[] extensions,
            @Nullable String authenticationMechanism,
            long resetErrorCode) {
        this.resetErrorCode = resetErrorCode;
        this.closeReason = closeReason;
        this.certainty = certainty;
        this.kind = kind;
        this.streamId = streamId;
        this.status = status;
        this.type = type;
        this.credentialLifetime = credentialLifetime;
        this.ticket = ticket;
        this.body = body;
        this.extensions = extensions;
        this.authenticationMechanism = authenticationMechanism;
    }

    static Event authenticated(OptionalLong credentialLifetime) {
        return new Event(
                Kind.AUTHENTICATED,
                0,
                0,
                0,
                credentialLifetime.isPresent() ? credentialLifetime.getAsLong() : NO_CREDENTIAL_LIFETIME,
                null,
                null,
                NO_TICKET,
                NO_BODY);
    }

    static Event authenticationRequired(String mechanism, byte[] credentials) {
        return new Event(
                Kind.AUTHENTICATION_REQUIRED,
                0,
                0,
                0,
                NO_CREDENTIAL_LIFETIME,
                null,
                null,
                NO_TICKET,
                credentials,
                mechanism);
    }

    public Optional<String> authenticationMechanism() {
        return Optional.ofNullable(authenticationMechanism);
    }

    public long authenticationId() {
        if (kind != Kind.AUTHENTICATION_REQUIRED) {
            throw new IllegalStateException("인증 판단 사건이 아니다");
        }
        return Endpoint.AUTHENTICATION_ID;
    }

    /** @param errorCode 받은 RESET의 {@code error_code}(부호 없는 32비트 값) */
    static Event requestReset(long streamId, long errorCode) {
        return new Event(
                Kind.REQUEST_RESET,
                streamId,
                0,
                0,
                NO_CREDENTIAL_LIFETIME,
                null,
                null,
                NO_TICKET,
                NO_BODY,
                NO_BODY,
                null,
                errorCode);
    }

    static Event closed(CloseReason closeReason) {
        return new Event(Kind.CLOSED, 0, 0, 0, NO_CREDENTIAL_LIFETIME, null, closeReason, NO_TICKET, NO_BODY);
    }

    /** @param body 응답 본문. 호출자가 이후 바꾸지 않는 배열이어야 한다(복사해서 넘긴다). */
    static Event result(long ticket, long streamId, int status, Certainty certainty, byte[] body) {
        return new Event(Kind.RESULT, streamId, status, 0, NO_CREDENTIAL_LIFETIME, certainty, null, ticket, body);
    }

    /** 스트림을 열기 전에 로컬에서 실패한 요청의 결과. 스트림 번호가 없다(규격 2.2.5절, 3.2.1절, 3.5절). */
    static Event localUnavailable(long ticket) {
        return new Event(
                Kind.RESULT,
                0,
                Status.UNAVAILABLE,
                0,
                NO_CREDENTIAL_LIFETIME,
                Certainty.NOT_PROCESSED,
                null,
                ticket,
                NO_BODY);
    }

    /** @param body 요청 본문. 호출자가 이후 바꾸지 않는 배열이어야 한다(복사해서 넘긴다). */
    static Event requestReceived(long streamId, int type, byte[] body) {
        return requestReceived(streamId, type, NO_BODY, body);
    }

    /**
     * @param extensions 요청 확장 영역(항목으로 인코딩된 바이트). 호출자가 이후 바꾸지 않는 배열이어야 한다(복사해서 넘긴다).
     * @param body 요청 본문. 호출자가 이후 바꾸지 않는 배열이어야 한다(복사해서 넘긴다).
     */
    static Event requestReceived(long streamId, int type, byte[] extensions, byte[] body) {
        return new Event(
                Kind.REQUEST_RECEIVED,
                streamId,
                0,
                type,
                NO_CREDENTIAL_LIFETIME,
                null,
                null,
                NO_TICKET,
                body,
                extensions,
                null);
    }

    /**
     * 이벤트 종류.
     *
     * @return 종류
     */
    public Kind kind() {
        return kind;
    }

    /**
     * {@link Kind#REQUEST_RESET}이 담은 받은 RESET의 {@code error_code}. 닫기 전용 코드나 정의하지 않은 코드도 받은 값 그대로다. 다른 종류에서는 비어
     * 있다.
     *
     * @return 오류 코드(부호 없는 32비트 값)
     */
    public OptionalLong resetErrorCode() {
        return resetErrorCode == NO_RESET_ERROR_CODE ? OptionalLong.empty() : OptionalLong.of(resetErrorCode);
    }

    /**
     * {@link Kind#RESULT}, {@link Kind#REQUEST_RECEIVED}, {@link Kind#REQUEST_RESET}의 스트림 번호. 다른 종류에서는 0이다. 스트림을 열기 전에 로컬에서 실패한
     * 요청의 {@link Kind#RESULT}도 0이다. 실제 스트림 번호는 0이 될 수 없다.
     *
     * @return 스트림 번호
     */
    public long streamId() {
        return streamId;
    }

    /**
     * {@link Kind#RESULT}의 status(규격 2.5.1절). 다른 종류에서는 0이다.
     *
     * @return status 값
     */
    public int status() {
        return status;
    }

    /**
     * {@link Kind#RESULT}의 결과 확정성(규격 1.3절). 다른 종류에서는 비어 있다.
     *
     * @return 결과 확정성
     */
    public Optional<Certainty> certainty() {
        return Optional.ofNullable(certainty);
    }

    /**
     * {@link Kind#CLOSED}의 종료 원인: 연결 종료(CLOSE)로 끝났는지, 오류 CLOSE로 끝났는지, CLOSE 없이 끊겼는지와 error_code, CLOSE를 보낸 쪽.
     * 다른 종류의 이벤트면 비어 있다.
     *
     * @return 종료 원인
     */
    public Optional<CloseReason> closeReason() {
        return Optional.ofNullable(closeReason);
    }

    /**
     * {@link Kind#REQUEST_RECEIVED}의 애플리케이션 {@code type}. 다른 종류에서는 0이다.
     *
     * @return type 값
     */
    public int type() {
        return type;
    }

    /**
     * {@link Kind#AUTHENTICATED}가 알린 자격 증명의 남은 수명(초, 규격 3.3.1절 {@code credential_lifetime}). 서버가 알리지 않았거나 다른
     * 종류의 이벤트면 비어 있다.
     *
     * @return 남은 수명
     */
    public OptionalLong credentialLifetime() {
        return credentialLifetime == NO_CREDENTIAL_LIFETIME
                ? OptionalLong.empty()
                : OptionalLong.of(credentialLifetime);
    }

    /**
     * {@link Kind#RESULT}가 짝지어지는 요청의 번호표({@link Endpoint#request}가 돌려준 값). 번호를 받은 요청, AUTH 관문 전에 미룬 요청,
     * 스트림을 열기 전에 로컬에서 실패한 요청의 결과 모두 그 요청에 쓰인 번호표를 담는다. 다른 종류에서는 비어 있다.
     *
     * @return 번호표
     */
    public OptionalLong ticket() {
        return ticket == NO_TICKET ? OptionalLong.empty() : OptionalLong.of(ticket);
    }

    /**
     * 본문. 응답을 받은 {@link Kind#RESULT}에서는 응답 본문, {@link Kind#REQUEST_RECEIVED}에서는 요청 본문이다. 본문이 없는 결과(처리
     * 안 됨, 결과 모름, 로컬 실패)와 다른 종류에서는 빈 배열이다. 부를 때마다 복사본을 돌려주므로 고쳐도 이벤트와 엔드포인트의 상태는 바뀌지 않는다.
     *
     * @return 본문의 복사본
     */
    public byte[] body() {
        return body.clone();
    }

    /**
     * {@link Kind#REQUEST_RECEIVED}의 요청 확장 영역(규격 2.4절). 항목(tag 2바이트, length 2바이트, value)이 이어진 바이트이고 없으면 빈 배열이다.
     * 다른 종류에서는 빈 배열이다. 부를 때마다 복사본을 돌려준다.
     *
     * @return 확장 영역의 복사본
     */
    public byte[] extensions() {
        return extensions.clone();
    }
}
