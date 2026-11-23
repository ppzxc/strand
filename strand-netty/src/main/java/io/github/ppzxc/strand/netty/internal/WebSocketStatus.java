package io.github.ppzxc.strand.netty.internal;

import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.ErrorCode;

/**
 * 연결이 끝난 원인에 대응하는 WebSocket Close 상태 코드. {@code strand-netty}의 동작이며 규격 1.2절은 {@code NO_ERROR}의 1000 말고는 정하지 않는다.
 *
 * <table>
 *   <caption>종료 원인과 상태 코드</caption>
 *   <tr><td>{@code NO_ERROR} 연결 종료</td><td>1000</td></tr>
 *   <tr><td>{@code PROTOCOL_ERROR}</td><td>1002</td></tr>
 *   <tr><td>{@code EXCESSIVE_USE}, {@code UNAUTHENTICATED}</td><td>1008</td></tr>
 *   <tr><td>{@code INTERNAL_ERROR}, {@code UNRESPONSIVE}</td><td>1011</td></tr>
 *   <tr><td>CLOSE 없이 닫음(서문 오류, 첫 프레임 오류, CLOSE 뒤의 연결 오류, 텍스트 메시지, 메시지 크기 ≠ 4 + {@code length})</td><td>1002</td></tr>
 * </table>
 *
 * <p>CLOSE 없이 닫는 경우는 core가 {@code PROTOCOL_ERROR}로 끝내므로 같은 줄이다. 사유 문자열은 늘 비운다.
 */
public final class WebSocketStatus {
    /** 닫을 때 보낼 Close가 없다(전송 계층이 이미 끊겼거나 앱이 즉시 끊었다). */
    public static final int NONE = -1;

    private static final int NORMAL = 1000;
    private static final int PROTOCOL_ERROR = 1002;
    private static final int POLICY_VIOLATION = 1008;
    private static final int INTERNAL_ERROR = 1011;

    private WebSocketStatus() {}

    /**
     * 종료 원인에 대응하는 Close 상태 코드.
     *
     * @param reason core가 알린 종료 원인
     * @return 상태 코드. 전송 계층이 끊겨 끝난 연결은 {@link #NONE}
     */
    public static int of(CloseReason reason) {
        switch (reason.kind()) {
            case CLOSE:
                return NORMAL;
            case ERROR_CLOSE:
                return ofError(reason.errorCode());
            default:
                return NONE;
        }
    }

    private static int ofError(long errorCode) {
        if (errorCode == ErrorCode.EXCESSIVE_USE || errorCode == ErrorCode.UNAUTHENTICATED) {
            return POLICY_VIOLATION;
        }
        if (errorCode == ErrorCode.INTERNAL_ERROR || errorCode == ErrorCode.UNRESPONSIVE) {
            return INTERNAL_ERROR;
        }
        // PROTOCOL_ERROR와, 오류 종료에 1000을 쓰지 않으려는 그 밖의 값.
        return PROTOCOL_ERROR;
    }
}
