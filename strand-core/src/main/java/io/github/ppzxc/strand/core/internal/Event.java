package io.github.ppzxc.strand.core.internal;

import java.util.Optional;
import java.util.OptionalLong;
import org.jspecify.annotations.Nullable;

public final class Event {

    static final int RESPONSE_TYPE_MISMATCH = Status.INTERNAL;

    public enum Kind {
        RESULT,

        CLOSED,

        REQUEST_RECEIVED,

        AUTHENTICATED,

        AUTHENTICATION_REQUIRED,

        REQUEST_RESET
    }

    private static final long NO_CREDENTIAL_LIFETIME = -1;

    private static final long NO_TICKET = -1;

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

    static Event result(long ticket, long streamId, int status, Certainty certainty, byte[] body) {
        return new Event(Kind.RESULT, streamId, status, 0, NO_CREDENTIAL_LIFETIME, certainty, null, ticket, body);
    }

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

    static Event requestReceived(long streamId, int type, byte[] body) {
        return requestReceived(streamId, type, NO_BODY, body);
    }

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

    public Kind kind() {
        return kind;
    }

    public OptionalLong resetErrorCode() {
        return resetErrorCode == NO_RESET_ERROR_CODE ? OptionalLong.empty() : OptionalLong.of(resetErrorCode);
    }

    public long streamId() {
        return streamId;
    }

    public int status() {
        return status;
    }

    public Optional<Certainty> certainty() {
        return Optional.ofNullable(certainty);
    }

    public Optional<CloseReason> closeReason() {
        return Optional.ofNullable(closeReason);
    }

    public int type() {
        return type;
    }

    public OptionalLong credentialLifetime() {
        return credentialLifetime == NO_CREDENTIAL_LIFETIME
                ? OptionalLong.empty()
                : OptionalLong.of(credentialLifetime);
    }

    public OptionalLong ticket() {
        return ticket == NO_TICKET ? OptionalLong.empty() : OptionalLong.of(ticket);
    }

    public byte[] body() {
        return body.clone();
    }

    public byte[] extensions() {
        return extensions.clone();
    }
}
