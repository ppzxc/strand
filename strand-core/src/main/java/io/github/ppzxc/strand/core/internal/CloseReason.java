package io.github.ppzxc.strand.core.internal;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * 연결이 왜 끝났는지(규격 3.5절). {@link Event.Kind#CLOSED}가 싣는다. 끝난 방식({@link Kind}), CLOSE의 {@code error_code}, CLOSE를 보낸
 * 쪽({@link Sender})으로 이뤄진다. 값은 불변이다.
 */
public final class CloseReason {

    /** 연결이 끝난 방식. */
    public enum Kind {
        /** {@code NO_ERROR} CLOSE를 교환하고 끝났다(연결 종료). */
        CLOSE,
        /** 오류 CLOSE로 끝났다. 연결 오류로 닫은 연결을 포함한다. */
        ERROR_CLOSE,
        /** CLOSE 없이 전송 계층이 끊겼다. 종료 중에 끊긴 연결도 포함한다. */
        DISCONNECTED
    }

    /** CLOSE를 보낸 쪽. */
    public enum Sender {
        /** 내가 보냈다. */
        LOCAL,
        /** 상대가 보냈다. */
        PEER
    }

    private final Kind kind;
    private final long errorCode;
    private final @Nullable Sender sender;

    private CloseReason(Kind kind, long errorCode, @Nullable Sender sender) {
        this.kind = kind;
        this.errorCode = errorCode;
        this.sender = sender;
    }

    static CloseReason close(Sender sender) {
        return new CloseReason(Kind.CLOSE, ErrorCode.NO_ERROR, sender);
    }

    static CloseReason errorClose(long errorCode, Sender sender) {
        return new CloseReason(Kind.ERROR_CLOSE, errorCode, sender);
    }

    static CloseReason disconnected() {
        return new CloseReason(Kind.DISCONNECTED, ErrorCode.NO_ERROR, null);
    }

    /**
     * 끝난 방식.
     *
     * @return 끝난 방식
     */
    public Kind kind() {
        return kind;
    }

    /**
     * CLOSE의 {@code error_code}(규격 2.5.2절). {@link Kind#CLOSE}와 {@link Kind#DISCONNECTED}에서는 {@code NO_ERROR}(0)다.
     *
     * @return error_code 값
     */
    public long errorCode() {
        return errorCode;
    }

    /**
     * CLOSE를 보낸 쪽. CLOSE 없이 끊겼으면 비어 있다. 연결 오류로 닫았는데 실제로 CLOSE를 보내지 않았다면(서문·첫 프레임 검사 실패, 형태가 잘못된
     * CLOSE) 로컬이 닫은 것으로 본다.
     *
     * @return 보낸 쪽
     */
    public Optional<Sender> sender() {
        return Optional.ofNullable(sender);
    }
}
