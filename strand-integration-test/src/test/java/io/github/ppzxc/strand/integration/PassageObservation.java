package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class PassageObservation implements BeforeEachCallback, AfterEachCallback {

    private static final ConcurrentLinkedQueue<ControlledPassage> CLIENT = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<ControlledPassage> SERVER = new ConcurrentLinkedQueue<>();
    private static volatile int readChunkBytes = Integer.MAX_VALUE;

    @Override
    public void beforeEach(ExtensionContext context) {
        reset();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        reset();
    }

    static PassageWrapping.Wrapper wrapper() {
        return (side, passage) -> {
            ControlledPassage controlled = new ControlledPassage(passage);
            if (readChunkBytes != Integer.MAX_VALUE) {
                controlled.readChunkBytes(readChunkBytes);
            }
            queue(side).add(controlled);
            return controlled;
        };
    }

    private static void reset() {
        CLIENT.clear();
        SERVER.clear();
        readChunkBytes = Integer.MAX_VALUE;
    }

    private static ConcurrentLinkedQueue<ControlledPassage> queue(PassageWrapping.Side side) {
        return side == PassageWrapping.Side.CLIENT ? CLIENT : SERVER;
    }

    static void splitReads(int bytes) {
        readChunkBytes = bytes;
    }

    static @Nullable ControlledPassage claim(PassageWrapping.Side side) {
        return queue(side).poll();
    }
}
