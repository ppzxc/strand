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
        AUTHENTICATED
    }

    /** {@code AUTH(ACK)}가 자격 증명의 수명을 알리지 않았음을 나타내는 값. */
    private static final long NO_CREDENTIAL_LIFETIME = -1;

    /** {@link Kind#RESULT}가 아니어서 번호표가 없음을 나타내는 값. 실제 번호표는 0 이상이다. */
    private static final long NO_TICKET = -1;

    private static final byte[] NO_BODY = new byte[0];

    private final Kind kind;
    private final long streamId;
    private final int status;
    private final int type;
    private final long credentialLifetime;
    private final @Nullable Certainty certainty;
    private final @Nullable Throwable cause;
    private final long ticket;
    private final byte[] body;

    private Event(Kind kind, long streamId, int status, int type, byte[] body) {
        this(kind, streamId, status, type, NO_CREDENTIAL_LIFETIME, null, null, NO_TICKET, body);
    }

    private Event(
            Kind kind,
            long streamId,
            int status,
            int type,
            long credentialLifetime,
            @Nullable Certainty certainty,
            @Nullable Throwable cause,
            long ticket,
            byte[] body) {
        this.certainty = certainty;
        this.cause = cause;
        this.kind = kind;
        this.streamId = streamId;
        this.status = status;
        this.type = type;
        this.credentialLifetime = credentialLifetime;
        this.ticket = ticket;
        this.body = body;
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

    static Event closed(@Nullable Throwable cause) {
        return new Event(Kind.CLOSED, 0, 0, 0, NO_CREDENTIAL_LIFETIME, null, cause, NO_TICKET, NO_BODY);
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
        return new Event(Kind.REQUEST_RECEIVED, streamId, 0, type, body);
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
     * {@link Kind#RESULT}와 {@link Kind#REQUEST_RECEIVED}의 스트림 번호. 다른 종류에서는 0이다. 스트림을 열기 전에 로컬에서 실패한
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
     * {@link Kind#CLOSED}의 로컬 원인. 서버의 {@link Authenticator}가 예외를 던져 닫았을 때 그 예외다. 와이어로는 알리지 않는 정보이며,
     * 다른 이유로 닫혔거나 다른 종류의 이벤트면 비어 있다.
     *
     * @return 닫힌 원인
     */
    public Optional<Throwable> cause() {
        return Optional.ofNullable(cause);
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
}
