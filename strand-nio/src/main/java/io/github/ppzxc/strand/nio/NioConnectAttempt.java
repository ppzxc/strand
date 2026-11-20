package io.github.ppzxc.strand.nio;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 진행 중인 연결 시도. 결과 관찰({@link #result()})·취소({@link #cancel()})·통로 정리 관찰({@link #transportReleased()})을 따로 제공한다.
 * {@link #result()}가 돌려주는 Stage를 취소해도 연결 시도는 취소되지 않는다. 시도를 취소하는 길은 {@link #cancel()}뿐이다.
 */
public final class NioConnectAttempt {
    private final CompletionStage<NioStrandConnection> result;
    private final CompletableFuture<Void> released;
    private final ConnectAttemptState state;

    NioConnectAttempt(
            CompletionStage<NioStrandConnection> result, CompletableFuture<Void> released, ConnectAttemptState state) {
        this.result = result;
        this.released = released;
        this.state = state;
    }

    /**
     * 연결 시도의 최종 결과. 성공은 보안·HELLO·연결 인증·핸들러 등록이 끝났다는 뜻이다. 취소가 먼저 처리되면 {@link PreparationFailedException}으로
     * 실패하고 원인은 {@link ConnectionAttemptCancelledException}이다.
     *
     * @return 결과
     */
    public CompletionStage<NioStrandConnection> result() {
        return result;
    }

    /**
     * 연결 시도 취소를 접수한다. 반환값은 접수 여부일 뿐 최종 결과가 아니다: 아직 확정되지 않아 접수했더라도 전송 스레드가 성공을 먼저 처리했다면 시도는 성공하고
     * 그때는 취소가 효과가 없다. 최종 결과는 {@link #result()}로 본다. 이미 확정됐으면 접수하지 않고 false를 돌려준다. 접수한 취소는 진행 중인 주소 해석
     * 대기·TCP 연결을 끊고 늦은 인증·팩터리 완료로 연결을 활성화하지 않는다. 반복해도 추가 효과가 없다.
     *
     * @return 취소를 접수했으면 true
     */
    public boolean cancel() {
        return state.cancel();
    }

    /**
     * 이 시도가 소유한 통로·타이머·등록 자원의 정리가 끝나면 완료한다. 실패·취소한 시도는 실제 통로를 닫은 뒤, 성공한 시도는 통로를 연결에 넘긴 때 완료한다. 사용자
     * 작업(인증·팩터리) 반환은 기다리지 않는다.
     *
     * @return 정리 완료
     */
    public CompletionStage<Void> transportReleased() {
        return released;
    }
}
