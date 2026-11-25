package io.github.ppzxc.strand.nio;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

final class UnreleasedHandlers {
    private final Set<UnreleasedHandler> entries = new LinkedHashSet<>();
    private volatile Runnable changeObserver = () -> {};

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
