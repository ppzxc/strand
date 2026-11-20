package io.github.ppzxc.strand.api;

/** A request and its independently retained result. */
public interface RequestHandle {
    /**
     * 요청 결과를 한 번만 확정해 보존한 결과. 응답, 취소, 기한 만료, 끊김 가운데 전송 처리 순서로 먼저 일어난 것이 확정하고 이후 사건은 바꾸지 않는다.
     *
     * @return 보존된 요청 결과
     */
    Result<RequestResult> result();

    /**
     * 취소를 요청한다. 취소가 성공했다는 뜻이 아니다. 응답이나 만료가 먼저 처리되었거나 연결이 이미 끝났으면 그 결과가 그대로 남으므로, 취소의 효과는
     * {@link #result()}의 보존된 결과로만 확인한다. 반복해도 추가 효과가 없다.
     */
    void cancel();
}
