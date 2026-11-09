package io.github.ppzxc.strand.core.internal;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * 생존 확인 타이머. 확인 대기(HELLO, AUTH, PING) 목록(보낸 순서)과 대기별 시작 시각·기한, 서버의 AUTH 대기 기한(규격 3.3절), 마지막 수신
 * 시각과 유휴 PING 횟수(규격 3.4.1절)를 소유하고, 시계는 읽지 않는다. 호출자가 가상 시각을 넘긴다. 판단만 돌려주고 프레임 생성과 CLOSE 결정은
 * 호출자의 몫이다.
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

    /** {@link #expiry}의 판단: 연결을 닫아야 하는 기한 경과. */
    enum Expiry {
        NONE,
        /** AUTH 대기 시간 초과. */
        UNAUTHENTICATED,
        /** 확인 대기 시간 초과. */
        UNRESPONSIVE
    }

    private static final int PING_PAYLOAD_SIZE = 8;

    private static final class Wait {
        final Kind kind;
        final byte @Nullable [] payload;
        boolean started;
        long deadline;

        Wait(Kind kind, byte @Nullable [] payload) {
            this.kind = kind;
            this.payload = payload;
        }
    }

    private final long ackTimeoutMs;
    private final long authTimeoutMs;
    /** 0 이하면 유휴 PING을 보내지 않는다. */
    private final long idleIntervalMs;
    /** 식별자로 찾는다. 순회는 등록한 순서대로다(가장 먼저 등록한 대기가 먼저 나온다). */
    private final Map<Long, Wait> waits = new LinkedHashMap<>();

    /** 등록된 PING 대기 수. {@link #hasPing}을 상수 시간에 답하려고 센다. */
    private int pingCount;

    private long lastId = NO_WAIT;
    /** 서버의 AUTH 대기가 끝나는 시각. 대기 중이 아니면 null이다. */
    private @Nullable Long authDeadline;

    private boolean authGatePassed;
    private boolean ended;
    private long lastReceivedAt;
    private long idlePingCount;

    LivenessTimers(long ackTimeoutMs, long authTimeoutMs, long idleIntervalMs) {
        this.ackTimeoutMs = ackTimeoutMs;
        this.authTimeoutMs = authTimeoutMs;
        this.idleIntervalMs = idleIntervalMs;
    }

    /** 서버가 AUTH를 기다리기 시작했다. 기한은 {@code now + authTimeoutMs}다. */
    void authWaitStarted(long now) {
        authDeadline = now + authTimeoutMs;
    }

    /** AUTH를 받아 AUTH 대기가 끝났다. */
    void authWaitEnded() {
        authDeadline = null;
    }

    /** AUTH 관문을 통과했다. 이때부터 유휴 PING을 보낼 수 있다. */
    void authGatePassed() {
        authGatePassed = true;
    }

    /** 프레임을 받았다. 유휴 간격은 이 시각부터 센다. */
    void frameReceived(long now) {
        lastReceivedAt = now;
    }

    /**
     * 확인 대기를 등록한다. 기한은 아직 세지 않는다.
     *
     * @param payload PING이면 보낸 8바이트, 아니면 null
     * @return 대기 식별자
     */
    long await(Kind kind, byte @Nullable [] payload) {
        long id = ++lastId;
        waits.put(id, new Wait(kind, payload));
        if (kind == Kind.PING) {
            pingCount++;
        }
        return id;
    }

    /** 대기에 매인 프레임을 전송 계층에 넘겼다. 이때부터 {@code now + ackTimeoutMs}가 기한이다. 이미 끝났거나 시작한 대기는 그대로다. */
    void started(long id, long now) {
        Wait wait = waits.get(id);
        if (wait != null && !wait.started) {
            wait.started = true;
            wait.deadline = now + ackTimeoutMs;
        }
    }

    /**
     * ACK를 받았다. 종류와 payload가 같은 대기 가운데 가장 먼저 등록한 하나가 끝난다. 프레임을 넘기기 전에 도착한 ACK도 그 대기를 끝낸다.
     * 맞는 대기가 없으면 아무 일도 없다.
     */
    void ackReceived(Kind kind, byte @Nullable [] payload) {
        for (Iterator<Wait> it = waits.values().iterator(); it.hasNext(); ) {
            Wait wait = it.next();
            if (wait.kind == kind && Arrays.equals(wait.payload, payload)) {
                it.remove();
                if (kind == Kind.PING) {
                    pingCount--;
                }
                return;
            }
        }
    }

    /** ACK를 기다리는 PING이 있는가. */
    boolean hasPing() {
        return pingCount > 0;
    }

    /**
     * 연결을 닫아야 하는 기한이 지났는가. AUTH 대기 기한을 먼저 보고 확인 대기 기한을 본다. 기한 정각은 지나지 않은 것이다. 연결이 끝난 뒤에는
     * 항상 {@link Expiry#NONE}이다.
     */
    Expiry expiry(long now) {
        if (ended) {
            return Expiry.NONE;
        }
        if (authDeadline != null && now > authDeadline) {
            return Expiry.UNAUTHENTICATED;
        }
        for (Wait wait : waits.values()) {
            if (wait.started && now > wait.deadline) {
                return Expiry.UNRESPONSIVE;
            }
        }
        return Expiry.NONE;
    }

    /**
     * 지금 보낼 유휴 PING의 payload(횟수 카운터, 빅엔디언 8바이트). 보낼 때가 아니면 null이다. 간격이 설정되지 않았거나, AUTH 관문 통과 전이거나,
     * 마지막 수신 뒤 간격이 차지 않았거나, 기다리는 PING이 있거나, 연결이 끝났으면 보내지 않는다. payload를 돌려주면 보내는 것으로 세고 횟수가
     * 오른다.
     */
    byte @Nullable [] idlePing(long now) {
        if (ended || idleIntervalMs <= 0 || !authGatePassed || now - lastReceivedAt < idleIntervalMs || hasPing()) {
            return null;
        }
        byte[] payload = new byte[PING_PAYLOAD_SIZE];
        for (int i = PING_PAYLOAD_SIZE - 1; i >= 0; i--) {
            payload[i] = (byte) (idlePingCount >>> (8 * (PING_PAYLOAD_SIZE - 1 - i)));
        }
        idlePingCount++;
        return payload;
    }

    /** 연결이 끝났다. 모든 대기를 지우고 이후 판단을 하지 않는다. */
    void clear() {
        ended = true;
        authDeadline = null;
        waits.clear();
        pingCount = 0;
    }
}
