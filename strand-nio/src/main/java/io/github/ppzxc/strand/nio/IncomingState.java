package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.ResultCompletion;

/** 받은 요청 하나의 전송 쪽 상태. 스트림 번호와 예약량, 응답 접수 여부는 전송 스레드만 바꾼다. */
final class IncomingState {
    final long streamId;
    final long reservedBytes;
    final ResultCompletion<RequestEnd> ended;
    /** 응답을 접수했다(전송 스레드만 읽고 쓴다). */
    boolean responded;
    /** 요청 수·바이트 예약을 돌려줬다(전송 스레드만 읽고 쓴다). */
    boolean reservationReleased;

    IncomingState(long streamId, long reservedBytes, ResultCompletion<RequestEnd> ended) {
        this.streamId = streamId;
        this.reservedBytes = reservedBytes;
        this.ended = ended;
    }
}
