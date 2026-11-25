package io.github.ppzxc.strand.nio.internal;

import org.jspecify.annotations.Nullable;

public final class PassageWrapping {

    public enum Side {
        CLIENT,
        SERVER
    }

    @FunctionalInterface
    public interface Wrapper {

        Passage wrap(Side side, Passage passage);
    }

    private PassageWrapping() {}

    public static Passage wrap(@Nullable Wrapper wrapper, Side side, Passage passage) {
        if (wrapper == null || passage instanceof TlsPassage) {
            return passage;
        }
        return wrapper.wrap(side, passage);
    }
}
