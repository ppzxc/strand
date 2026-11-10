package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.Certainty;

/** 내가 낸 요청의 결과. status, 결과 확정성(규격 1.3절), 응답 본문으로 이뤄진다. 값은 불변이다. */
public final class RequestResult {

    private final int status;
    private final Certainty certainty;
    private final byte[] body;

    RequestResult(int status, Certainty certainty, byte[] body) {
        this.status = status;
        this.certainty = certainty;
        this.body = body;
    }

    /**
     * 결과의 status(규격 2.5.1절).
     *
     * @return status 값
     */
    public int status() {
        return status;
    }

    /**
     * 결과 확정성. 응답을 받았는지, 처리 안 됨인지, 결과 모름인지.
     *
     * @return 결과 확정성
     */
    public Certainty certainty() {
        return certainty;
    }

    /**
     * 응답 본문. 응답을 받지 않은 결과에서는 빈 배열이다. 부를 때마다 복사본을 돌려준다.
     *
     * @return 본문의 복사본
     */
    public byte[] body() {
        return body.clone();
    }
}
