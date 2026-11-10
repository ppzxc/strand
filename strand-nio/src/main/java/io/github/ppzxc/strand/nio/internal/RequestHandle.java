package io.github.ppzxc.strand.nio.internal;

import java.util.concurrent.CompletableFuture;

/**
 * 낸 요청의 핸들. 결과는 {@link #result()}로 받는다. 요청의 번호표는 전송 스레드에서 정해지므로 핸들은 그 사실을 숨긴다. 요청을 다룰 동작(취소 등)은
 * 이 핸들에 더한다.
 */
public final class RequestHandle {

    private final CompletableFuture<RequestResult> result;
    private final Runnable canceller;

    RequestHandle(CompletableFuture<RequestResult> result, Runnable canceller) {
        this.result = result;
        this.canceller = canceller;
    }

    /**
     * 요청을 취소한다. 번호를 받기 전에 미뤄 둔 요청도 취소할 수 있다. 결과 Future는 core가 내는 결과({@code CANCELLED}와 결과 확정성)로 끝난다.
     * 이미 끝난 요청이면 아무 일도 없다. 어느 스레드에서든 부를 수 있고, 요청을 낸 뒤에 부르면 그 요청보다 먼저 처리되는 일은 없다.
     */
    public void cancel() {
        canceller.run();
    }

    /**
     * 요청의 결과(status, 결과 확정성, 응답 본문)가 오면 끝나는 Future. 보낼 수 없는 요청이면 예외로 끝난다. 이 Future를 {@code cancel}해도 요청은
     * 취소되지 않는다.
     *
     * @return 결과 Future
     */
    public CompletableFuture<RequestResult> result() {
        return result;
    }
}
