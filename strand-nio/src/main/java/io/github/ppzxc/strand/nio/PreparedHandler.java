package io.github.ppzxc.strand.nio;

import java.util.Objects;

public final class PreparedHandler {
    private final RequestHandler handler;
    private final Runnable disposal;
    private final java.util.concurrent.atomic.AtomicBoolean disposed = new java.util.concurrent.atomic.AtomicBoolean();

    private PreparedHandler(RequestHandler handler, Runnable disposal) {
        this.handler = handler;
        this.disposal = disposal;
    }

    public static PreparedHandler of(RequestHandler handler, Runnable disposal) {
        return new PreparedHandler(
                Objects.requireNonNull(handler, "handler"), Objects.requireNonNull(disposal, "disposal"));
    }

    public static PreparedHandler of(RequestHandler handler) {
        return of(handler, () -> {});
    }

    public RequestHandler handler() {
        return handler;
    }

    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            disposal.run();
        }
    }
}
