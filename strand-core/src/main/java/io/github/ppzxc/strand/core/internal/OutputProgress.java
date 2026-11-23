package io.github.ppzxc.strand.core.internal;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * 출력 진행. 지금 쓰는 출력과 그 식별자, 쓴 바이트 수, 쓰기 대기열 노드에 걸린 확인 대기, 끊긴 뒤의 폐기를 소유한다. 쓰기 대기열의 순서는
 * {@link Outbox}가, 확인 대기의 기한은 {@link LivenessTimers}가 정하고 이 모듈은 쓰기 진행에 맞춰 부르기만 한다. 내 요청 장부에는 쓰기를 시작한
 * 때({@link OwnRequestLedger#onWriteStarted})와 마지막 바이트를 넘긴 때({@link OwnRequestLedger#onWriteCompleted}) 두 사건만 알린다. 시계는
 * 읽지 않고 호출자가 넘긴 시각을 쓴다. 연결 하나의 것이므로 한 스레드에서만 쓴다.
 */
final class OutputProgress {

    private final Outbox outbox;
    private final LivenessTimers timers;
    private final OwnRequestLedger ledger;

    private Outbox.@Nullable Node selected;
    private @Nullable EndpointOutput current;
    private long nextOutputId;
    private int written;
    private long waitId = LivenessTimers.NO_WAIT;
    private boolean disconnected;

    OutputProgress(Outbox outbox, LivenessTimers timers, OwnRequestLedger ledger) {
        this.outbox = outbox;
        this.timers = timers;
        this.ledger = ledger;
    }

    /** 현재 쓸 출력. 철회 또는 우선순위 변경 뒤에는 새 식별자가 발급된다. 끊긴 뒤에는 null이다. */
    @Nullable
    EndpointOutput next() {
        if (disconnected) {
            return null;
        }
        Outbox.Node head = outbox.first();
        if (written == 0 && selected != head) {
            selected = head;
            current = head == null ? null : new EndpointOutput(++nextOutputId, head.message);
        }
        return current;
    }

    /**
     * 선택한 출력에 전송된 바이트 수를 보고한다.
     *
     * @throws IllegalArgumentException 출력이 없거나 식별자가 다르거나 개수가 범위를 벗어날 때
     */
    void progress(long id, int count, long now) {
        EndpointOutput output = next();
        if (output == null || output.id() != id || count < 0 || count > output.length() - written) {
            throw new IllegalArgumentException("유효하지 않은 출력 진행");
        }
        if (count == 0) {
            return;
        }
        if (written == 0) {
            start();
        }
        timers.outputProgressed(now);
        written += count;
        if (written == output.length()) {
            complete(now);
        }
    }

    /** 쓰기를 시작해 남은 바이트. 쓰기를 시작하지 않았으면 0이다. */
    long writingRemaining() {
        return current != null && written > 0 ? current.length() - written : 0;
    }

    /** 내보낼 출력이 남았는가. 끊긴 뒤에는 없다. */
    boolean isPending() {
        return !disconnected && (written > 0 || outbox.size() > 0);
    }

    /** 전송 계층이 끊겼다. 쓰기 대기열과 진행 중인 출력을 폐기하고 이후 출력을 내지 않는다. */
    void disconnect() {
        disconnected = true;
        selected = null;
        current = null;
        written = 0;
        waitId = LivenessTimers.NO_WAIT;
        outbox.discard();
    }

    /** 선택한 출력의 쓰기를 시작한 것으로 한다. 이제 철회할 수 없다. */
    private void start() {
        Outbox.Node node = Objects.requireNonNull(selected);
        waitId = node.waitId;
        ledger.onWriteStarted(node);
        outbox.removeForWriting(node);
    }

    /** 선택한 출력이 전체 전달된 것으로 한다. */
    private void complete(long now) {
        if (waitId != LivenessTimers.NO_WAIT) {
            timers.started(waitId, now);
            waitId = LivenessTimers.NO_WAIT;
        }
        ledger.onWriteCompleted(Objects.requireNonNull(selected));
        written = 0;
        selected = null;
        current = null;
    }
}
