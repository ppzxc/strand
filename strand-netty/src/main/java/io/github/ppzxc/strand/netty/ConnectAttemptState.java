package io.github.ppzxc.strand.netty;

import org.jspecify.annotations.Nullable;

/**
 * 연결 시도 하나의 취소 상태. 앱 스레드의 취소 요청과 EventLoop의 확정이 만나는 곳이다. 취소는 확정되기 전에 접수된 것만 효과가 있으며 성공과 취소 가운데
 * 무엇이 이기는지는 전송 스레드(EventLoop)가 처리하는 순서가 정한다.
 */
final class ConnectAttemptState {
    private boolean cancelRequested;
    private boolean confirmed;
    private @Nullable ConnectionEngine engine;

    /**
     * 취소를 접수한다. 엔진이 묶여 있으면 EventLoop에 취소를 넘긴다.
     *
     * @return 확정되기 전이라 접수했으면 true
     */
    boolean cancel() {
        ConnectionEngine target;
        synchronized (this) {
            if (confirmed) {
                return false;
            }
            if (cancelRequested) {
                return true;
            }
            cancelRequested = true;
            target = engine;
        }
        if (target != null) {
            target.cancelAttempt();
        }
        return true;
    }

    /** 연결 엔진을 묶는다. 묶기 전에 접수된 취소는 지금 전달한다. */
    void bind(ConnectionEngine value) {
        boolean pending;
        synchronized (this) {
            engine = value;
            pending = cancelRequested;
        }
        if (pending) {
            value.cancelAttempt();
        }
    }

    /** 준비 결과(성공·실패·취소)가 확정됐다. 이후 취소는 효과가 없다. */
    synchronized void confirm() {
        confirmed = true;
    }
}
