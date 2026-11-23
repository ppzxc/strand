package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Extensions;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.github.ppzxc.strand.api.Status;
import java.util.Objects;

/**
 * 상대가 낸 요청. 내부 스트림 번호 없이 어느 스레드에서든 {@link #respond}로 응답한다. 핸들러가 응답 없이 반환해도 되고, 반환한 뒤 다른 스레드에서
 * 응답해도 된다. {@link #payload()}는 앱이 보관·수정할 수 있는 수신 배열이다. 같은 배열을 돌려주므로 반복 조회와 여러 관찰자 사이의 공유·동시 수정은 앱이
 * 책임진다.
 */
public final class IncomingRequest {
    private final ConnectionEngine engine;
    private final NettyStrandConnection connection;
    private final IncomingState state;
    private final int type;
    private final byte[] payload;
    private final Extensions extensions;
    private final ResultCompletion<RequestEnd> ended;

    IncomingRequest(
            ConnectionEngine engine,
            NettyStrandConnection connection,
            IncomingState state,
            int type,
            Extensions extensions,
            byte[] payload,
            ResultCompletion<RequestEnd> ended) {
        this.engine = engine;
        this.connection = connection;
        this.state = state;
        this.type = type;
        this.extensions = extensions;
        this.payload = payload;
        this.ended = ended;
    }

    /**
     * 애플리케이션 {@code type}.
     *
     * @return type 값
     */
    public int type() {
        return type;
    }

    /**
     * 요청 본문. 앱이 보관·수정할 수 있다.
     *
     * @return 본문
     */
    public byte[] payload() {
        return payload;
    }

    /**
     * 요청 확장 영역(규격 2.4절)의 항목.
     *
     * @return 확장 항목
     */
    public Extensions extensions() {
        return extensions;
    }

    /**
     * 이 요청을 받은 연결. 같은 연결로 상대에게 요청을 낼 수 있다.
     *
     * @return 연결
     */
    public NettyStrandConnection connection() {
        return connection;
    }

    /**
     * 응답한다. 최초 유효 응답만 접수한다. 응답 접수는 상대 수신 보장이 아니다. 자원이 모자라면 본문을 보관하지 않고 응답 권한도 소비하지 않으므로 살아 있는
     * 요청에서 다시 응답할 수 있다.
     *
     * @param status 규격 2.5.1절의 status(0~16). 17~255는 예약이라 보내지 않는다
     * @param body 응답 본문. 호출 중 복사한다
     * @return 접수 결과(ACCEPTED, ALREADY_RESPONDED, REQUEST_ENDED, RESOURCE_EXHAUSTED)
     * @throws IllegalArgumentException status가 0~16이 아닐 때
     */
    public Result<ResponseAcceptance> respond(int status, byte[] body) {
        if (status < Status.OK || status > Status.UNAUTHENTICATED) {
            throw new IllegalArgumentException("status는 0~16이어야 한다: " + status);
        }
        return engine.respond(this, status, Objects.requireNonNull(body, "body"));
    }

    /**
     * 이 요청이 끝났음을 알리는 보존된 결과: 응답 접수, 상대 RESET(오류 코드 포함), 연결 끝남 가운데 하나다. 끝난 요청의 핸들러가 실행 대기 중이면 시작하지
     * 않는다. 처리 중인 핸들러는 이 결과로 업무 중단을 시도할 수 있다. 완료 여부는 {@code isDone()}으로 조회하고 완료 전 {@code resultNow()}는 즉시 예외다.
     *
     * @return 요청 종료 결과
     */
    public Result<RequestEnd> ended() {
        return ended;
    }

    IncomingState state() {
        return state;
    }
}
