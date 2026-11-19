package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.Result;
import io.netty.util.concurrent.Future;
import java.util.Objects;

/** 공통 요청 결과를 Netty {@link Future}로 바꾸는 명시적 변환. */
public final class NettyResults {
    private NettyResults() {}

    /**
     * 이 전송 모듈이 낸 요청의 결과를 그 연결의 EventLoop에 연결된 Future로 바꾼다. 앱이 EventLoop를 다시 넘기지 않는다.
     *
     * <p>요청 오류·로컬 접수 실패·취소·기한 만료·끊김도 {@link RequestResult} 성공 값이다. Future는 앱 실행기나 {@code observe}를 거치지 않고 결과가 확정되는 경로에서
     * 완료된다. 같은 결과를 여러 번 바꾸면 서로 독립인 Future를 얻고, Future를 취소해도 그 Future만 취소되며 원래 결과·다른 Future·실제 요청은 그대로다. 실제 요청 취소는
     * {@code RequestHandle.cancel()}로 한다.
     *
     * <p>관찰 한도가 차서 등록하지 못하면 실패한 Future({@link ResourceExhaustedException})를 돌려주며 원래 결과는 그대로다. Future에 앱이 붙인 리스너는 Netty 계약에 따라
     * EventLoop에서 실행된다. 리스너 예외나 EventLoop의 실행 거절은 Netty가 진단하고 다른 스레드에서 대신 실행하지 않으며, Future 완료와 리스너 실행 성공은 다르다. 리스너 안에서는
     * 짧게 처리하고 업무는 앱 실행기로 넘긴다.
     *
     * @param result 이 전송 모듈의 {@code RequestHandle.result()}
     * @return 결과로 완료되는 Future
     * @throws IllegalArgumentException 이 전송 모듈이 낸 요청의 결과가 아닌 경우
     */
    public static Future<RequestResult> toFuture(Result<RequestResult> result) {
        Objects.requireNonNull(result);
        if (!(result instanceof LoopBoundResult)) {
            throw new IllegalArgumentException("not a result of a Netty strand request");
        }
        return ((LoopBoundResult) result).toFuture();
    }
}
