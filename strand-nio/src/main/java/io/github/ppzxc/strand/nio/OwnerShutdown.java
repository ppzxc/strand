package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.github.ppzxc.strand.api.ShutdownDeadlineExceededException;
import io.github.ppzxc.strand.api.ShutdownKind;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

final class OwnerShutdown {

    private static final Resource[] TASKS = {
        Resource.HANDLER_TASKS, Resource.FACTORY_TASKS, Resource.AUTHENTICATION_TASKS, Resource.RESOLVE_TASKS
    };

    private final Executor executor;
    private final OwnerBudgets owner;
    private final @Nullable UnreleasedHandlers unreleased;
    private final long graceNanos;
    private final long deadlineNanos;
    private final LongSupplier clock;

    private final Object lock = new Object();
    private final Set<NioStrandConnection> connections = new LinkedHashSet<>();
    private final Set<Listener> listeners = new LinkedHashSet<>();
    private boolean shuttingDown;
    private long startedAt;

    private final ResultCompletion<ShutdownResult> result;
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();

    OwnerShutdown(CommonSettings.Common common, @Nullable UnreleasedHandlers unreleased) {
        this.executor = common.executor;
        this.owner = common.owner;
        this.unreleased = unreleased;
        this.graceNanos = common.shutdownGrace.toNanos();
        this.deadlineNanos = common.shutdownDeadline.toNanos();
        this.clock = common.nanoClock;
        this.result = new ResultCompletion<>(
                common.executor,
                new ObservationBudget(common.owner.usage(Resource.OBSERVATIONS).limit()),
                common.owner.observationBudget());
        common.owner.onRelease(this::poke);
        if (unreleased != null) {
            unreleased.onRemove(this::poke);
        }
    }

    boolean accepting() {
        synchronized (lock) {
            return !shuttingDown;
        }
    }

    boolean register(NioStrandConnection connection) {
        synchronized (lock) {
            if (shuttingDown) {
                return false;
            }
            connections.add(connection);
            return true;
        }
    }

    void unregister(NioStrandConnection connection) {
        synchronized (lock) {
            connections.remove(connection);
            lock.notifyAll();
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    boolean register(Listener listener) {
        synchronized (lock) {
            if (shuttingDown) {
                return false;
            }
            listeners.removeIf(known -> known.terminated().isDone());
            listeners.add(listener);
        }
        listener.terminated().thenRun(this::poke);
        return true;
    }

    private void poke() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    CompletionStage<Void> shutdown() {
        List<NioStrandConnection> toClose = new ArrayList<>();
        List<Listener> toStop = new ArrayList<>();
        boolean first;
        synchronized (lock) {
            first = !shuttingDown;
            if (first) {
                shuttingDown = true;

                startedAt = clock.getAsLong();
                toClose.addAll(connections);
                toStop.addAll(listeners);
            }
            lock.notifyAll();
        }
        if (first) {
            toStop.forEach(Listener::close);
            toClose.forEach(NioStrandConnection::shutdownClose);
            Thread thread = new Thread(this::supervise, "strand-nio-shutdown");
            thread.setDaemon(true);
            thread.start();
        }
        return stopped.thenApply(done -> null);
    }

    Result<ShutdownResult> result() {
        return result;
    }

    CompletionStage<Void> terminated() {
        return terminated.thenApply(done -> null);
    }

    private static long add(long at, long nanos) {
        return Saturating.add(at, nanos);
    }

    private void supervise() {
        long graceAt = add(startedAt, graceNanos);
        long deadlineAt = add(startedAt, deadlineNanos);
        boolean aborted = false;
        boolean resultFixed = false;
        boolean interrupted = false;
        for (; ; ) {
            List<NioStrandConnection> toAbort = null;
            @Nullable ShutdownResult fix = null;
            boolean done = false;
            synchronized (lock) {
                if (isTerminated()) {
                    done = true;
                    fix = resultFixed ? null : ShutdownResult.completed();
                } else {
                    long now = clock.getAsLong();
                    if (!aborted && now - graceAt >= 0) {
                        aborted = true;
                        toAbort = new ArrayList<>(connections);
                    } else if (!resultFixed && now - deadlineAt >= 0) {
                        fix = ShutdownResult.deadlineExceeded(remaining());
                    } else {
                        long wait = Long.MAX_VALUE;
                        if (!aborted) {
                            wait = Math.min(wait, graceAt - now);
                        }
                        if (!resultFixed) {
                            wait = Math.min(wait, deadlineAt - now);
                        }
                        interrupted |= await(wait);
                        continue;
                    }
                }
            }
            if (toAbort != null) {
                toAbort.forEach(NioStrandConnection::shutdownAbort);
            }
            if (fix != null) {
                resultFixed = true;
                publish(fix);
            }
            if (done) {
                dispatch(() -> terminated.complete(null));
                dispatch(this::completeStopped);
                break;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("WaitNotInLoop")
    private boolean await(long nanos) {
        try {
            if (nanos == Long.MAX_VALUE) {
                lock.wait();
            } else {
                lock.wait(nanos / 1_000_000L, (int) (nanos % 1_000_000L));
            }
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    private void publish(ShutdownResult fixed) {
        result.complete(fixed);
        dispatch(this::completeStopped);
    }

    private void completeStopped() {
        ShutdownResult fixed = result.resultNow();
        if (fixed.kind() == ShutdownKind.COMPLETED) {
            stopped.complete(null);
        } else {
            stopped.completeExceptionally(new ShutdownDeadlineExceededException(fixed));
        }
    }

    private void dispatch(Runnable action) {
        StageCompletion.complete(executor, action, deadlineNanos);
    }

    private boolean isTerminated() {
        if (!connections.isEmpty()) {
            return false;
        }
        for (Listener listener : listeners) {
            if (!listener.terminated().isDone()) {
                return false;
            }
        }
        for (Resource task : TASKS) {
            if (owner.current(task) > 0) {
                return false;
            }
        }
        return unreleased == null || unreleased.size() == 0;
    }

    private Map<Resource, Long> remaining() {
        Map<Resource, Long> remaining = new EnumMap<>(Resource.class);
        put(remaining, Resource.TRANSPORTS, connections.size());
        long openListeners = 0;
        for (Listener listener : listeners) {
            if (!listener.terminated().isDone()) {
                openListeners++;
            }
        }
        put(remaining, Resource.LISTENERS, openListeners);
        for (Resource task : TASKS) {
            put(remaining, task, owner.current(task));
        }
        put(remaining, Resource.UNRELEASED_HANDLERS, unreleased == null ? 0 : unreleased.size());
        return remaining;
    }

    private static void put(Map<Resource, Long> into, Resource resource, long count) {
        if (count > 0) {
            into.put(resource, count);
        }
    }
}
