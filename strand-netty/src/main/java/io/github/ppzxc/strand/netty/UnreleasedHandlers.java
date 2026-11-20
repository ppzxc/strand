package io.github.ppzxc.strand.netty;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** 소유 객체가 보존하는 회수 대기 팩터리 결과. 각 항목이 팩터리 작업 슬롯을 하나씩 쥐고 있으므로 개수는 팩터리 작업 한도를 넘지 않는다. */
final class UnreleasedHandlers {
    private final Set<UnreleasedHandler> entries = new LinkedHashSet<>();
    private volatile Runnable changeObserver = () -> {};

    /** 항목이 빠질 때마다 호출할 관찰자를 정한다. 전체 종료가 미정리 결과가 해결되길 기다릴 때 쓴다. */
    void onRemove(Runnable observer) {
        this.changeObserver = observer;
    }

    synchronized void add(UnreleasedHandler handler) {
        entries.add(handler);
    }

    void remove(UnreleasedHandler handler) {
        synchronized (this) {
            entries.remove(handler);
        }
        changeObserver.run();
    }

    synchronized int size() {
        return entries.size();
    }

    synchronized List<UnreleasedHandler> snapshot() {
        return new ArrayList<>(entries);
    }
}
