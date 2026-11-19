package io.github.ppzxc.strand.nio;

import org.jspecify.annotations.Nullable;

/**
 * 연결 시도 하나의 취소 상태. 앱 스레드의 취소 요청과 연결 스레드의 확정이 만나는 곳이다. 취소는 확정되기 전에 접수된 것만 효과가 있으며 성공과 취소 가운데
 * 무엇이 이기는지는 전송 스레드가 처리하는 순서가 정한다.
 */
final class ConnectAttemptState {
    private boolean cancelRequested;
    private boolean confirmed;
    private @Nullable Runnable inProgress;
    private @Nullable ConnectionEngine engine;

    /**
     * 취소를 접수한다. 진행 중인 통로 열기(주소 해석 대기·TCP 연결)는 곧바로 끊는다.
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
            // 잠금 안에서 끊는다. 통로 열기가 끝나 등록을 해제한 뒤에는 끊기가 이미 끝났거나 아예 일어나지 않아, 연결에 넘어간 통로를 닫지 않는다.
            if (inProgress != null) {
                inProgress.run();
            }
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

    synchronized boolean cancelRequested() {
        return cancelRequested;
    }

    /** 준비 결과(성공·실패·취소)가 확정됐다. 이후 취소는 효과가 없다. */
    synchronized void confirm() {
        confirmed = true;
    }

    /**
     * 취소하면 끊어야 할 진행 중인 작업을 등록하거나(null이면 해제) 한다. 이미 취소가 접수됐다면 등록 즉시 끊는다.
     */
    void registerAbort(@Nullable Runnable abort) {
        synchronized (this) {
            inProgress = abort;
            if (abort != null && cancelRequested) {
                abort.run();
            }
        }
    }
}
