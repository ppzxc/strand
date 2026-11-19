package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;

/** 연결 하나가 라이브러리 안에 복사·보관한 준비 데이터(주소 입력·자격 증명·해석 결과)의 바이트 예약. 보관 전에 예약하고 참조를 놓을 때 돌려준다. */
final class PreparationBytes {
    private final ConnectionBudgets budgets;
    private long held;

    PreparationBytes(ConnectionBudgets budgets) {
        this.budgets = budgets;
    }

    /** 기다리지 않고 예약한다. 모자라면 아무것도 예약하지 않는다. */
    synchronized boolean tryReserve(long bytes) {
        if (!budgets.tryReserve(Resource.PREPARATION_BYTES, bytes)) {
            return false;
        }
        held += bytes;
        return true;
    }

    /** 예약한 것 가운데 {@code bytes}를 돌려준다. */
    synchronized void release(long bytes) {
        if (bytes > 0) {
            budgets.release(Resource.PREPARATION_BYTES, bytes);
            held -= bytes;
        }
    }

    /** 예약한 전부를 돌려준다. 반복해도 한 번만 돌려준다. */
    synchronized void releaseAll() {
        if (held > 0) {
            budgets.release(Resource.PREPARATION_BYTES, held);
            held = 0;
        }
    }
}
