package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.LivenessTimers.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * 생존 확인 타이머의 internal seam 테스트. 가상 시계 값만 넣고 판단만 확인한다(핸드셰이크 바이트 없음): 대기 종류별 기한 경계값, ACK와 가장
 * 오래된 대기의 짝 맞춤, HELLO와 PING 대기의 독립, 꺼내기 전 프레임의 대기 미시작, 연결 끝 정리(규격 3.3절, 3.4절).
 */
class LivenessTimersTest {

    private static final long ACK_TIMEOUT = 1000;

    private final LivenessTimers timers = new LivenessTimers(ACK_TIMEOUT);

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

        assertThat(timers.ackExpired(100 + ACK_TIMEOUT - 1)).isFalse();
        assertThat(timers.ackExpired(100 + ACK_TIMEOUT)).isFalse();
        assertThat(timers.ackExpired(100 + ACK_TIMEOUT + 1)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void waitNotYetHandedOverNeverExpires(Kind kind) {
        await(kind);

        assertThat(timers.ackExpired(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void waitStartedLaterCountsFromItsOwnStart() {
        timers.started(await(Kind.HELLO), 0);
        timers.ackReceived(Kind.HELLO, null);
        timers.started(await(Kind.AUTH), 500);

        assertThat(timers.ackExpired(500 + ACK_TIMEOUT)).isFalse();
        assertThat(timers.ackExpired(500 + ACK_TIMEOUT + 1)).isTrue();
    }

    @Test
    void ackEndsTheOldestWaitOfTheSameKind() {
        long first = timers.await(Kind.PING, ping(1));
        long second = timers.await(Kind.PING, ping(1));
        timers.started(first, 0);
        timers.started(second, 600);

        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.ackExpired(1001)).isFalse();
        assertThat(timers.ackExpired(1601)).isTrue();
    }

    @Test
    void pingAckMustMatchThePayload() {
        timers.started(timers.await(Kind.PING, ping(1)), 0);

        timers.ackReceived(Kind.PING, ping(2));

        assertThat(timers.hasPing()).isTrue();
        assertThat(timers.ackExpired(1001)).isTrue();

        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.hasPing()).isFalse();
    }

    @Test
    void helloAndPingWaitsAreIndependent() {
        timers.started(await(Kind.HELLO), 0);
        timers.started(await(Kind.PING), 0);

        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.hasPing()).isFalse();
        assertThat(timers.ackExpired(1001)).isTrue();

        timers.ackReceived(Kind.HELLO, null);

        assertThat(timers.ackExpired(1001)).isFalse();
    }

    @Test
    void ackOfAnotherKindEndsNothing() {
        timers.started(await(Kind.AUTH), 0);

        timers.ackReceived(Kind.HELLO, null);
        timers.ackReceived(Kind.PING, ping(1));

        assertThat(timers.ackExpired(1001)).isTrue();
    }

    @Test
    void ackArrivingBeforeTheFrameIsHandedOverEndsTheWait() {
        long id = await(Kind.HELLO);

        timers.ackReceived(Kind.HELLO, null);
        timers.started(id, 0);

        assertThat(timers.ackExpired(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void startingAWaitTwiceKeepsTheFirstDeadline() {
        long id = await(Kind.HELLO);
        timers.started(id, 0);
        timers.started(id, 900);

        assertThat(timers.ackExpired(1001)).isTrue();
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
    void clearDropsEveryWait() {
        long id = await(Kind.PING);
        timers.started(id, 0);
        timers.started(await(Kind.HELLO), 0);

        timers.clear();

        assertThat(timers.hasPing()).isFalse();
        assertThat(timers.ackExpired(Long.MAX_VALUE)).isFalse();
        timers.started(id, 0);
        assertThat(timers.ackExpired(Long.MAX_VALUE)).isFalse();
    }
}
