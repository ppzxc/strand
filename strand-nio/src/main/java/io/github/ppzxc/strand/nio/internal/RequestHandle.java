package io.github.ppzxc.strand.nio.internal;

import java.util.concurrent.CompletableFuture;

/**
 * 낸 요청의 핸들. 결과는 {@link #result()}로 받는다. 요청의 번호표는 전송 스레드에서 정해지므로 핸들은 그 사실을 숨긴다. 요청을 다룰 동작(취소 등)은
 * 이 핸들에 더한다.
 */
public final class RequestHandle {

    private final CompletableFuture<RequestResult> result;

    RequestHandle(CompletableFuture<RequestResult> result) {
        this.result = result;
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
