package io.github.ppzxc.strand.core.internal;

public final class CollectionSpace {
    private final int events;
    private final int outputs;

    private CollectionSpace(int events, int outputs) {
        this.events = events;
        this.outputs = outputs;
    }

    public static CollectionSpace of(int events, int outputs) {
        if (events < 1 || outputs < 1) {
            throw new IllegalArgumentException("수거 공간은 양수여야 한다");
        }
        return new CollectionSpace(events, outputs);
    }

    int events() {
        return events;
    }

    int outputs() {
        return outputs;
    }
}
