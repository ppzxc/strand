package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * 확인 대기(HELLO, AUTH, PING)의 생존 확인 타이머. 확인 대기 목록(보낸 순서)과 대기별 시작 시각·기한을 소유하고, 시계는 읽지 않는다. 호출자가
 * 가상 시각을 넘긴다(규격 3.3절, 3.4절).
 *
 * <p>대기는 {@link #await}로 등록하고, 프레임을 전송 계층에 넘긴 때에 {@link #started}로 시작한다. 시작 전의 대기는 기한이 없다. 쓰기 대기열 노드와
 * 대기는 {@code await}가 돌려준 식별자로 잇는다. 연결 하나의 타이머이므로 한 스레드에서만 쓴다.
 */
final class LivenessTimers {

    /** 대기 식별자가 아님을 뜻하는 값. {@link #await}는 이 값을 돌려주지 않는다. */
    static final long NO_WAIT = 0;

    /** 무엇의 ACK를 기다리는가. */
    enum Kind {
        HELLO,
        AUTH,
        PING
    }

    private static final class Wait {
        final long id;
        final Kind kind;
        final byte @Nullable [] payload;
        boolean started;
        long deadline;

        Wait(long id, Kind kind, byte @Nullable [] payload) {
            this.id = id;
            this.kind = kind;
            this.payload = payload;
        }
    }

    private final long ackTimeoutMs;
    /** 보낸 순서대로다. */
    private final List<Wait> waits = new ArrayList<>();

    private long lastId = NO_WAIT;

    LivenessTimers(long ackTimeoutMs) {
        this.ackTimeoutMs = ackTimeoutMs;
    }

    /**
     * 확인 대기를 등록한다. 기한은 아직 세지 않는다.
     *
     * @param payload PING이면 보낸 8바이트, 아니면 null
     * @return 대기 식별자
     */
    long await(Kind kind, byte @Nullable [] payload) {
        long id = ++lastId;
        waits.add(new Wait(id, kind, payload));
        return id;
    }

    /** 대기에 매인 프레임을 전송 계층에 넘겼다. 이때부터 {@code now + ackTimeoutMs}가 기한이다. 이미 끝났거나 시작한 대기는 그대로다. */
    void started(long id, long now) {
        // 대기는 등록 순서이고 식별자는 등록할 때마다 커지므로 식별자로 이분 탐색한다.
        int low = 0;
        int high = waits.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Wait wait = waits.get(mid);
            if (wait.id < id) {
                low = mid + 1;
            } else if (wait.id > id) {
                high = mid - 1;
            } else {
                if (!wait.started) {
                    wait.started = true;
                    wait.deadline = now + ackTimeoutMs;
                }
                return;
            }
        }
    }

    /**
     * ACK를 받았다. 종류와 payload가 같은 대기 가운데 가장 먼저 등록한 하나가 끝난다. 프레임을 넘기기 전에 도착한 ACK도 그 대기를 끝낸다.
     * 맞는 대기가 없으면 아무 일도 없다.
     */
    void ackReceived(Kind kind, byte @Nullable [] payload) {
        for (Iterator<Wait> it = waits.iterator(); it.hasNext(); ) {
            Wait wait = it.next();
            if (wait.kind == kind && Arrays.equals(wait.payload, payload)) {
                it.remove();
                return;
            }
        }
    }

    /** ACK를 기다리는 PING이 있는가. */
    boolean hasPing() {
        for (Wait wait : waits) {
            if (wait.kind == Kind.PING) {
                return true;
            }
        }
        return false;
    }

    /** 시작한 대기 가운데 기한이 지난 것이 있는가(UNRESPONSIVE). 기한 정각은 지나지 않은 것이다. */
    boolean ackExpired(long now) {
        for (Wait wait : waits) {
            if (wait.started && now > wait.deadline) {
                return true;
            }
        }
        return false;
    }

    /** 연결이 끝났다. 모든 대기를 지운다. */
    void clear() {
        waits.clear();
    }
}
