package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.github.ppzxc.strand.api.ShutdownDeadlineExceededException;
import io.github.ppzxc.strand.api.ShutdownKind;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/**
 * 소유 객체(클라이언트·서버) 하나의 전체 종료. 새 접수 차단 → 시도 정리 → 리스닝 중지 → 연결 CLOSE → shutdownGrace가 끝나면 남은 연결 abort → 내부 실행
 * 자원 종료 순서로 진행한다. 두 기한은 첫 {@link #shutdown()} 호출 시각부터 센다.
 *
 * <p>종료 결과({@link #result()})는 모든 내부 자원이 정리되거나 shutdownDeadline이 되면 한 번 확정하고 이후 바꾸지 않는다. 실제 정리 완료({@link
 * #terminated()})는 기한과 무관하게 연결·리스닝·잔존 작업·미정리 결과가 모두 풀린 뒤 따로 완료한다. 앱이 준 EventLoopGroup과 실행기는 종료하지 않는다.
 *
 * <p>앱이 관찰하는 Future는 앱용 실행기를 거치지 않고 감독 스레드가 곧바로 완료한다. 그래서 앱 실행기가 작업을 거절하거나 받고도 실행하지 않아도 공개 완료는 막히지 않는다.
 * 기다림은 연결 종료·리스닝 정리·예약 반환·미정리 결과 인수 같은 실제 사건과 두 기한 시각만으로 깨어난다. 기한 시각은 소유 객체가 쓰는 시계({@code nanoClock})로 계산한다.
 */
final class OwnerShutdown {
    /** 전체 종료가 끝나길 기다리는 잔존 작업의 종류. 예약이 돌아오면 풀린 것이다. */
    private static final Resource[] TASKS = {
        Resource.HANDLER_TASKS, Resource.FACTORY_TASKS, Resource.AUTHENTICATION_TASKS, Resource.RESOLVE_TASKS
    };

    private final OwnerBudgets owner;
    private final @Nullable UnreleasedHandlers unreleased;
    private final long graceNanos;
    private final long deadlineNanos;
    private final LongSupplier clock;

    private final Object lock = new Object();
    private final Set<NettyStrandConnection> connections = new LinkedHashSet<>();
    private final Set<Listener> listeners = new LinkedHashSet<>();
    private boolean shuttingDown;
    private long startedAt;

    private final ResultCompletion<ShutdownResult> result;
    private final Promise<Void> stopped;
    private final Promise<Void> terminated;

    OwnerShutdown(CommonSettings.Common common, @Nullable UnreleasedHandlers unreleased) {
        this.owner = common.owner;
        this.unreleased = unreleased;
        this.graceNanos = common.shutdownGrace.toNanos();
        this.deadlineNanos = common.shutdownDeadline.toNanos();
        this.clock = common.nanoClock;
        this.result = new ResultCompletion<>(
                common.executor,
                new ObservationBudget(common.owner.usage(Resource.OBSERVATIONS).limit()),
                common.owner.observationBudget());
        this.stopped = common.group.next().newPromise();
        this.terminated = common.group.next().newPromise();
        stopped.setUncancellable();
        terminated.setUncancellable();
        common.owner.onRelease(this::poke);
        if (unreleased != null) {
            unreleased.onRemove(this::poke);
        }
    }

    /** 새 연결 시도를 받을 수 있는지(종료를 시작하지 않았는지). 확인만 하며 예약하지 않는다. */
    boolean accepting() {
        synchronized (lock) {
            return !shuttingDown;
        }
    }

    /**
     * 연결(시도 포함)을 종료 대상으로 기록한다.
     *
     * @return 종료를 시작하지 않아 기록했으면 true
     */
    boolean register(NettyStrandConnection connection) {
        synchronized (lock) {
            if (shuttingDown) {
                return false;
            }
            connections.add(connection);
            return true;
        }
    }

    /** 연결이 모든 정리를 마쳤다. 기록에서 뺀다. */
    void unregister(NettyStrandConnection connection) {
        synchronized (lock) {
            connections.remove(connection);
            lock.notifyAll();
        }
    }

    /**
     * 리스닝을 종료 대상으로 기록한다. 바인딩이 끝나기 전의 리스닝도 기록하므로 전체 종료가 진행 중인 바인딩을 놓치지 않는다.
     *
     * @return 종료를 시작하지 않아 기록했으면 true
     */
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

    /**
     * 전체 종료를 시작한다. 처음 호출만 순서를 시작하고, 반복 호출은 같은 결과를 돌려주며 기한을 늦추지 않는다.
     *
     * @return 종료 결과. 성공은 {@code COMPLETED}, 기한 초과는 {@link ShutdownDeadlineExceededException}
     */
    Future<Void> shutdown() {
        List<NettyStrandConnection> toClose = new ArrayList<>();
        List<Listener> toStop = new ArrayList<>();
        boolean first;
        synchronized (lock) {
            first = !shuttingDown;
            if (first) {
                shuttingDown = true;
                // 기한의 시작은 처음 호출이 잠금을 얻은 순간이다. 동시 첫 호출이 서로 다른 시각을 기준으로 삼지 않게 잠금 안에서 읽는다.
                startedAt = clock.getAsLong();
                toClose.addAll(connections);
                toStop.addAll(listeners);
            }
            lock.notifyAll();
        }
        if (first) {
            toStop.forEach(Listener::close);
            toClose.forEach(NettyStrandConnection::shutdownClose);
            Thread thread = new Thread(this::supervise, "strand-netty-shutdown");
            thread.setDaemon(true);
            thread.start();
        }
        return stopped;
    }

    Result<ShutdownResult> result() {
        return result;
    }

    Future<Void> terminated() {
        return terminated;
    }

    private void supervise() {
        long graceAt = Saturating.add(startedAt, graceNanos);
        long deadlineAt = Saturating.add(startedAt, deadlineNanos);
        boolean aborted = false;
        boolean resultFixed = false;
        boolean interrupted = false;
        for (; ; ) {
            List<NettyStrandConnection> toAbort = null;
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
                toAbort.forEach(NettyStrandConnection::shutdownAbort);
            }
            if (fix != null) {
                resultFixed = true;
                // 결과를 먼저 보존하고, 보존한 결과대로 종료 완료를 알린다.
                result.complete(fix);
                completeStopped();
            }
            if (done) {
                // 종료 결과 완료를 먼저 알린 뒤 실제 정리 완료를 알린다.
                terminated.trySuccess(null);
                break;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** 잠금을 쥔 채 부른다. 사건이 알리거나 {@code nanos}가 지나면 돌아온다. 중단됐으면 true. */
    @SuppressWarnings("WaitNotInLoop") // 호출하는 감독 루프가 기다린 뒤 조건을 다시 평가한다.
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

    /** 보존된 종료 결과대로 앱이 관찰하는 종료 완료를 알린다. */
    private void completeStopped() {
        ShutdownResult fixed = result.resultNow();
        if (fixed.kind() == ShutdownKind.COMPLETED) {
            stopped.trySuccess(null);
        } else {
            stopped.tryFailure(new ShutdownDeadlineExceededException(fixed));
        }
    }

    /** 잠금을 쥔 채 부른다. */
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

    /** 잠금을 쥔 채 부른다. 아직 풀리지 않은 것만 담는다. */
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
