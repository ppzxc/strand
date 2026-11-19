package io.github.ppzxc.strand.nio;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 폐기 실행이 거절돼 앱이 회수할 수 있는 활성화되지 못한 팩터리 결과. 앱이 {@link #claim()}으로 인수하기 전까지 팩터리 작업 예약이 유지된다.
 */
public final class UnreleasedHandler {
    private final PreparedHandler prepared;
    private final UnreleasedHandlers registry;
    private final Runnable releaseReservation;
    private final AtomicBoolean claimed = new AtomicBoolean();

    UnreleasedHandler(PreparedHandler prepared, UnreleasedHandlers registry, Runnable releaseReservation) {
        this.prepared = prepared;
        this.registry = registry;
        this.releaseReservation = releaseReservation;
    }

    /**
     * 소유권을 한 번 인수한다. 인수하면 보존된 목록에서 빠지고 예약이 돌아오며, 이후 정리(폐기 동작 실행)는 앱 책임이다. 여러 스레드가 동시에 불러도 한
     * 호출만 값을 받는다.
     *
     * @return 인수한 결과. 이미 인수됐으면 빈 값
     */
    public Optional<PreparedHandler> claim() {
        if (!claimed.compareAndSet(false, true)) {
            return Optional.empty();
        }
        registry.remove(this);
        releaseReservation.run();
        return Optional.of(prepared);
    }
}
