package io.github.ppzxc.strand.core.internal;

/** 로컬 결과의 결과 확정성(규격 1.3절). 와이어에는 싣지 않는다. */
public enum Certainty {
    /** 상대의 응답을 받아 결과가 정해졌다. */
    RESPONSE,
    /** 상대가 요청 처리를 시작하지 않았음이 확실하다. 다시 보내도 안전하다. */
    NOT_PROCESSED,
    /** 상대가 요청을 처리했는지 알 수 없다. */
    UNKNOWN
}
