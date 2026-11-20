package io.github.ppzxc.strand.integration.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** 실제 Netty NIO 채널·loopback 소켓에서 core 출력을 TCP 실제 쓰기량으로 보고하는 경계를 검증한다(#315). */
class CoreTcpDeliveryTest {
    private static byte[] pingPayload() {
        return new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    }

    @Test
    void zeroWriteIsNotStartAndWithdrawalLeavesNothingOnTheWire() throws Exception {
        try (TcpFixture fixture = new TcpFixture()) {
            int filler = fixture.saturate();
            AtomicLong ticket = new AtomicLong();
            fixture.driver.command(
                    () -> ticket.set(fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, new byte[100])));
            // 드라이버는 쓰기가 막히면 1ms마다 다시 시도하므로 0바이트 쓰기의 횟수는 시간에 따라 늘어난다. 횟수가 아니라 0만 보고됐음을 확인한다.
            assertThat(fixture.onLoop(fixture.driver::reported)).isNotEmpty().containsOnly(0);
            assertThat(fixture.onLoop(fixture.driver::zeroWrites)).isPositive();

            fixture.driver.command(() -> fixture.endpoint.cancelRequest(ticket.get()));
            List<Event> events = fixture.onLoop(fixture.endpoint::drainEvents);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).certainty()).hasValue(Certainty.NOT_PROCESSED);

            fixture.driver.command(() -> fixture.endpoint.ping(pingPayload()));
            assertThat(fixture.read(filler)).containsOnly((byte) 0);
            byte[] ping = fixture.read(28);
            assertThat(OpenEndpoint.typeOf(ping)).isEqualTo(2);
            fixture.closeAndSettle();
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
        }
    }

    @Test
    void partialWriteKeepsTheFrameAheadOfTheResetAndResultIsUnknown() throws Exception {
        try (TcpFixture fixture = new TcpFixture()) {
            byte[] payload = new byte[512 * 1024];
            Arrays.fill(payload, (byte) 7);
            AtomicLong ticket = new AtomicLong();
            fixture.driver.command(() -> ticket.set(fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, payload)));
            List<Integer> reported = fixture.onLoop(fixture.driver::reported);
            int frameLength = 4 + 16 + payload.length;
            assertThat(reported.get(0)).isPositive().isLessThan(frameLength);
            assertThat(reported.get(reported.size() - 1)).isZero();

            fixture.driver.command(() -> fixture.endpoint.cancelRequest(ticket.get()));
            List<Event> events = fixture.onLoop(fixture.endpoint::drainEvents);
            assertThat(events.get(0).certainty()).hasValue(Certainty.UNKNOWN);

            byte[] wire = fixture.read(frameLength + 24);
            List<byte[]> frames = OpenEndpoint.frames(wire);
            assertThat(frames).hasSize(2);
            assertThat(OpenEndpoint.typeOf(frames.get(0))).isEqualTo(OpenEndpoint.REQUEST_TYPE);
            assertThat(frames.get(0)).hasSize(frameLength);
            assertThat(OpenEndpoint.typeOf(frames.get(1))).isEqualTo(OpenEndpoint.FRAME_TYPE_RESET);
            fixture.closeAndSettle();
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
            assertThat(fixture.onLoop(fixture.driver::allocatedCount)).isEqualTo(2);
        }
    }

    @Test
    void fullWriteReportsEveryByteAndLaterCancellationStillSendsReset() throws Exception {
        try (TcpFixture fixture = new TcpFixture()) {
            AtomicLong ticket = new AtomicLong();
            fixture.driver.command(() -> ticket.set(fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, new byte[10])));
            assertThat(fixture.onLoop(fixture.driver::reported)).containsExactly(4 + 16 + 10);
            fixture.driver.command(() -> fixture.endpoint.cancelRequest(ticket.get()));
            List<Event> events = fixture.onLoop(fixture.endpoint::drainEvents);
            assertThat(events.get(0).certainty()).hasValue(Certainty.UNKNOWN);
            List<byte[]> frames = OpenEndpoint.frames(fixture.read(30 + 24));
            assertThat(frames).hasSize(2);
            assertThat(OpenEndpoint.typeOf(frames.get(1))).isEqualTo(OpenEndpoint.FRAME_TYPE_RESET);
            fixture.closeAndSettle();
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
        }
    }

    @Test
    void closeAfterPartialWriteIsNotProcessedAndReleasesEveryBuffer() throws Exception {
        try (TcpFixture fixture = new TcpFixture()) {
            byte[] payload = new byte[512 * 1024];
            fixture.driver.command(() -> fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, payload));
            assertThat(fixture.onLoop(fixture.driver::reported).get(0)).isPositive();
            fixture.closeAndSettle();
            List<Event> events = fixture.onLoop(fixture.endpoint::drainEvents);
            assertThat(events).first().satisfies(e -> assertThat(e.certainty()).hasValue(Certainty.NOT_PROCESSED));
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
        }
    }
}
