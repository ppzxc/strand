package io.github.ppzxc.strand.core.internal;

public final class ReceiveProgress {
    private final int consumed;
    private final boolean collectionRequired;

    ReceiveProgress(int consumed, boolean collectionRequired) {
        this.consumed = consumed;
        this.collectionRequired = collectionRequired;
    }

    public int consumed() {
        return consumed;
    }

    public boolean collectionRequired() {
        return collectionRequired;
    }
}
