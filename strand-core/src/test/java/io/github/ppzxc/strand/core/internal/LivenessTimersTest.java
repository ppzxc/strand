package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.LivenessTimers.Expiry;
import io.github.ppzxc.strand.core.internal.LivenessTimers.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * 생존 확인 타이머의 internal seam 테스트. 가상 시계 값만 넣고 판단만 확인한다(핸드셰이크 바이트 없음): 대기 종류별 기한 경계값, AUTH 대기 기한, 유휴 PING 일정, ACK와 가장
 * 오래된 대기의 짝 맞춤, HELLO와 PING 대기의 독립, 꺼내기 전 프레임의 대기 미시작, 연결 끝 정리(규격 3.3절, 3.4절).
 */
class LivenessTimersTest {

    private static final long ACK_TIMEOUT = 1000;

    private static final long AUTH_TIMEOUT = 5000;
    private static final long IDLE_INTERVAL = 3000;

    private final LivenessTimers timers = new LivenessTimers(ACK_TIMEOUT, AUTH_TIMEOUT, IDLE_INTERVAL);

    private static byte[] ping(int tag) {
        return new byte[] {0, 0, 0, 0, 0, 0, 0, (byte) tag};
    }

    private long await(Kind kind) {
        return timers.await(kind, kind == Kind.PING ? ping(1) : null);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void waitExpiresOnlyAfterTheDeadline(Kind kind) {
        timers.started(await(kind), 100);

        assertThat(timers.expiry(100 + ACK_TIMEOUT - 1)).isEqualTo(Expiry.NONE);
        assertThat(timers.expiry(100 + ACK_TIMEOUT)).isEqualTo(Expiry.NONE);
        assertThat(timers.expiry(100 + ACK_TIMEOUT + 1)).isEqualTo(Expiry.UNRESPONSIVE);
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void waitNotYetHandedOverNeverExpires(Kind kind) {
        await(kind);

        assertThat(timers.expiry(Long.MAX_VALUE)).isEqualTo(Expiry.NONE);
    }

    @Test
    void waitStartedLaterCountsFromItsOwnStart() {
        timers.started(await(Kind.HELLO), 0);
        timers.ackReceived(Kind.HELLO, null);
        timers.started(await(Kind.AUTH), 500);

        assertThat(timers.expiry(500 + ACK_TIMEOUT)).isEqualTo(Expiry.NONE);
        assertThat(timers.expiry(500 + ACK_TIMEOUT + 1)).isEqualTo(Expiry.UNRESPONSIVE);
    }

    @Test
    void ackEndsTheOldestWaitOfTheSameKind() {
        long first = timers.await(Kind.PING, ping(1));
        long second = timers.await(Kind.PING, ping(1));
        timers.started(first, 0);
        timers.started(second, 600);

        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.expiry(1001)).isEqualTo(Expiry.NONE);
        assertThat(timers.expiry(1601)).isEqualTo(Expiry.UNRESPONSIVE);
    }

    @Test
    void pingAckMustMatchThePayload() {
        timers.started(timers.await(Kind.PING, ping(1)), 0);

        timers.ackReceived(Kind.PING, ping(2));

        assertThat(timers.hasPing()).isTrue();
        assertThat(timers.expiry(1001)).isEqualTo(Expiry.UNRESPONSIVE);

        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.hasPing()).isFalse();
    }

    @Test
    void helloAndPingWaitsAreIndependent() {
        timers.started(await(Kind.HELLO), 0);
        timers.started(await(Kind.PING), 0);

        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.hasPing()).isFalse();
        assertThat(timers.expiry(1001)).isEqualTo(Expiry.UNRESPONSIVE);

        timers.ackReceived(Kind.HELLO, null);

        assertThat(timers.expiry(1001)).isEqualTo(Expiry.NONE);
    }

    @Test
    void ackOfAnotherKindEndsNothing() {
        timers.started(await(Kind.AUTH), 0);

        timers.ackReceived(Kind.HELLO, null);
        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.expiry(1001)).isEqualTo(Expiry.UNRESPONSIVE);
    }

    @Test
    void ackArrivingBeforeTheFrameIsHandedOverEndsTheWait() {
        long id = await(Kind.HELLO);

        timers.ackReceived(Kind.HELLO, null);
        timers.started(id, 0);

        assertThat(timers.expiry(Long.MAX_VALUE)).isEqualTo(Expiry.NONE);
    }

    @Test
    void startingAWaitTwiceKeepsTheFirstDeadline() {
        long id = await(Kind.HELLO);
        timers.started(id, 0);
        timers.started(id, 900);

        assertThat(timers.expiry(1001)).isEqualTo(Expiry.UNRESPONSIVE);
    }

    @Test
    void hasPingIgnoresOtherKinds() {
        await(Kind.HELLO);
        await(Kind.AUTH);

        assertThat(timers.hasPing()).isFalse();

        await(Kind.PING);

        assertThat(timers.hasPing()).isTrue();
    }

    @Test
    void hasPingStaysTrueUntilEveryAwaitedPingIsAcked() {
        timers.await(Kind.PING, ping(1));
        timers.await(Kind.PING, ping(2));
        timers.await(Kind.PING, ping(1));

        timers.ackReceived(Kind.PING, ping(1));
        timers.ackReceived(Kind.PING, ping(2));
        assertThat(timers.hasPing()).isTrue();

        timers.ackReceived(Kind.PING, ping(2));
        assertThat(timers.hasPing()).isTrue();

        timers.ackReceived(Kind.PING, ping(1));
        assertThat(timers.hasPing()).isFalse();
    }

    @Test
    void startingAnUnknownWaitChangesNothing() {
        long id = await(Kind.HELLO);
        timers.ackReceived(Kind.HELLO, null);

        timers.started(id, 0);
        timers.started(id + 100, 0);

        assertThat(timers.expiry(Long.MAX_VALUE)).isEqualTo(Expiry.NONE);
    }

    @Test
    void clearDropsEveryWait() {
        long id = await(Kind.PING);
        timers.started(id, 0);
        timers.started(await(Kind.HELLO), 0);

        timers.clear();

        assertThat(timers.hasPing()).isFalse();
        assertThat(timers.expiry(Long.MAX_VALUE)).isEqualTo(Expiry.NONE);
        timers.started(id, 0);
        assertThat(timers.expiry(Long.MAX_VALUE)).isEqualTo(Expiry.NONE);
    }

    @Test
    void everyWaitStartsAfterOthersWereEndedInAnyOrder() {
        long[] ids = new long[9];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = timers.await(Kind.PING, ping(i));
        }
        timers.ackReceived(Kind.PING, ping(0));
        timers.ackReceived(Kind.PING, ping(4));
        timers.ackReceived(Kind.PING, ping(8));
        for (int i = ids.length - 1; i >= 0; i--) {
            timers.started(ids[i], 10L * i);
        }

        // 남은 대기 가운데 가장 이른 기한은 ping(1)의 10 + 1000이다.
        assertThat(timers.expiry(1010)).isEqualTo(Expiry.NONE);
        assertThat(timers.expiry(1011)).isEqualTo(Expiry.UNRESPONSIVE);
        for (int i : new int[] {1, 2, 3, 5, 6, 7}) {
            timers.ackReceived(Kind.PING, ping(i));
        }
        assertThat(timers.hasPing()).isFalse();
    }

    @Test
    void authWaitExpiresOnlyAfterTheDeadline() {
        timers.authWaitStarted(200);

        assertThat(timers.expiry(200 + AUTH_TIMEOUT - 1)).isEqualTo(Expiry.NONE);
        assertThat(timers.expiry(200 + AUTH_TIMEOUT)).isEqualTo(Expiry.NONE);
        assertThat(timers.expiry(200 + AUTH_TIMEOUT + 1)).isEqualTo(Expiry.UNAUTHENTICATED);
    }

    @Test
    void authWaitEndedNoLongerExpires() {
        timers.authWaitStarted(0);
        timers.authWaitEnded();

        assertThat(timers.expiry(Long.MAX_VALUE)).isEqualTo(Expiry.NONE);
    }

    @Test
    void authDeadlineIsJudgedBeforeAckDeadline() {
        timers.started(await(Kind.HELLO), 0);
        timers.authWaitStarted(0);

        assertThat(timers.expiry(AUTH_TIMEOUT + 1)).isEqualTo(Expiry.UNAUTHENTICATED);
    }

    @Test
    void idlePingIsDueOnlyAfterTheIdleInterval() {
        timers.authGatePassed();
        timers.frameReceived(100);

        assertThat(timers.idlePing(100 + IDLE_INTERVAL - 1)).isNull();
        assertThat(timers.idlePing(100 + IDLE_INTERVAL)).isNotNull();
    }

    @Test
    void idlePingIsSuppressedWithoutAnIdleInterval() {
        LivenessTimers off = new LivenessTimers(ACK_TIMEOUT, AUTH_TIMEOUT, 0);
        off.authGatePassed();

        assertThat(off.idlePing(Long.MAX_VALUE)).isNull();
    }

    @Test
    void idlePingIsSuppressedWhileAPingIsAwaited() {
        timers.authGatePassed();
        await(Kind.PING);

        assertThat(timers.idlePing(IDLE_INTERVAL)).isNull();

        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.idlePing(IDLE_INTERVAL)).isNotNull();
    }

    @Test
    void idlePingIsSuppressedBeforeTheAuthGate() {
        assertThat(timers.idlePing(Long.MAX_VALUE)).isNull();

        timers.authGatePassed();

        assertThat(timers.idlePing(Long.MAX_VALUE)).isNotNull();
    }

    @Test
    void consecutiveIdlePingsCarryAnIncreasingCounter() {
        timers.authGatePassed();

        assertThat(timers.idlePing(IDLE_INTERVAL)).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 0});
        assertThat(timers.idlePing(IDLE_INTERVAL)).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 1});
        assertThat(timers.idlePing(IDLE_INTERVAL)).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 2});
    }

    @Test
    void receivingAFrameDelaysTheNextIdlePing() {
        timers.authGatePassed();
        timers.frameReceived(IDLE_INTERVAL - 1);

        assertThat(timers.idlePing(IDLE_INTERVAL)).isNull();
        assertThat(timers.idlePing(2 * IDLE_INTERVAL - 1)).isNotNull();
    }

    @Test
    void noExpiryIsJudgedAfterTheConnectionEnded() {
        timers.authWaitStarted(0);
        timers.started(await(Kind.HELLO), 0);

        timers.clear();

        assertThat(timers.expiry(Long.MAX_VALUE)).isEqualTo(Expiry.NONE);
        assertThat(timers.hasPing()).isFalse();
    }

    @Test
    void noIdlePingIsJudgedAfterTheConnectionEnded() {
        timers.authGatePassed();

        timers.clear();

        assertThat(timers.idlePing(Long.MAX_VALUE)).isNull();
    }
}
