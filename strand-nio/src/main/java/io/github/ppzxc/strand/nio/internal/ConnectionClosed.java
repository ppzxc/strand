package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.CloseReason;
import java.io.IOException;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * 연결이 끝났다는 알림. core가 판단한 종료 원인({@link CloseReason})에 전송 모듈이 아는 입출력 예외와 {@code Authenticator}가 던진 예외를
 * 더한다. 값은 불변이다.
 */
public final class ConnectionClosed {

    private final CloseReason reason;
    private final @Nullable IOException ioException;
    private final @Nullable Throwable authenticatorFailure;

    ConnectionClosed(CloseReason reason, @Nullable IOException ioException, @Nullable Throwable authenticatorFailure) {
        this.reason = reason;
        this.ioException = ioException;
        this.authenticatorFailure = authenticatorFailure;
    }

    /**
     * core가 낸 종료 원인: 연결 종료(CLOSE) / 오류 CLOSE / CLOSE 없이 끊김, error_code, CLOSE를 보낸 쪽.
     *
     * @return 종료 원인
     */
    public CloseReason reason() {
        return reason;
    }

    /**
     * 연결을 끊은 입출력 예외. 상대가 CLOSE 없이 끊었으면 {@link java.io.EOFException}, 읽기·쓰기가 실패했으면 그 예외다. 입출력 때문에 끝나지
     * 않았으면(CLOSE 교환, 내가 끊음) 비어 있다.
     *
     * @return 입출력 예외
     */
    public Optional<IOException> ioException() {
        return Optional.ofNullable(ioException);
    }

    /**
     * 서버의 {@code Authenticator}가 던져 연결이 닫혔을 때 그 예외. 와이어로 알리지 않는 정보다.
     *
     * @return {@code Authenticator}가 던진 예외
     */
    public Optional<Throwable> authenticatorFailure() {
        return Optional.ofNullable(authenticatorFailure);
    }
}
