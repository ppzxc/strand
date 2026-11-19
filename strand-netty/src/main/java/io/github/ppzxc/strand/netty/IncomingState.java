package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.ResultCompletion;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 받은 요청 하나의 전송 쪽 상태. 스트림 번호와 예약량은 불변이고 응답 접수 여부({@link #responded})는 이벤트 루프만 바꾼다. 요청 수 예약은 요청이 끝날 때, 바이트
 * 예약은 요청이 끝나고 핸들러 작업도 본문 참조를 놓은 뒤(작업이 끝났거나 시작하지 못하고 사라졌을 때) 돌려준다. 두 시점은 서로 다른 스레드에서 오므로 반환 판정만 잠근다.
 */
final class IncomingState {
    final long streamId;
    final long reservedBytes;
    final ResultCompletion<RequestEnd> ended;
    /** 응답을 접수했다(이벤트 루프만 읽고 쓴다). */
    boolean responded;
    /** 응답 접수 명령이 이벤트 루프의 처리를 기다리는 중이다. 요청마다 하나만 대기시켜 명령 대기가 예약 없이 늘지 않게 한다. */
    final AtomicBoolean responding = new AtomicBoolean();

    private boolean countReturned;
    private boolean bytesReturned;
    private boolean handlerSettled;

    IncomingState(long streamId, long reservedBytes, ResultCompletion<RequestEnd> ended) {
        this.streamId = streamId;
        this.reservedBytes = reservedBytes;
        this.ended = ended;
    }

    /** 요청 수 예약을 돌려줄 차례인가. 처음 부를 때만 true다. */
    synchronized boolean claimCountReturn() {
        if (countReturned) {
            return false;
        }
        countReturned = true;
        return true;
    }

    /** 핸들러 작업이 본문 참조를 놓았다(끝났거나 시작하지 않고 사라졌다). */
    synchronized void handlerSettled() {
        handlerSettled = true;
    }

    /** 바이트 예약을 돌려줄 차례인가: 요청이 끝났고 핸들러 작업도 참조를 놓았을 때 처음 한 번만 true다. */
    synchronized boolean claimBytesReturn() {
        if (bytesReturned || !countReturned || !handlerSettled) {
            return false;
        }
        bytesReturned = true;
        return true;
    }
}
