package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

/** TCP 통로의 쓰기 이어쓰기 판정과 기다림 조합을 소켓 한 쌍으로 결정적으로 확인한다. */
class TcpPassageBranchTest {

    @Test
    void writingAtOrPastTheEndOfAMessageWritesNothing() throws Exception {
        OutboundMessage message = TestMessages.largeRequests().get(0);
        TcpLoopback pair = TcpLoopback.open();
        TcpPassage passage = TcpPassage.of(pair.client);
        try {
            assertThat(passage.write(message, message.length())).isZero();
        } finally {
            passage.close();
            pair.accepted.close();
        }
    }

    @Test
    void aWriteAtAnOffsetThatIsNotWhereThePartialWriteStoppedStartsFromAFreshCopy() throws Exception {
        List<OutboundMessage> messages = TestMessages.largeRequests();
        OutboundMessage message = messages.get(0);
        TcpLoopback pair = TcpLoopback.open(4096);
        TcpPassage passage = TcpPassage.of(pair.client);
        try {
            // 상대가 읽지 않으므로 소켓 버퍼만큼만 쓰고 멈춘다. 메시지는 버퍼보다 훨씬 크다.
            int first = passage.write(message, 0);
            assertThat(first).isLessThan(message.length());

            // 이어 쓰는 위치(first)가 아닌 0에서 다시 쓰면 남은 복사본을 버리고 새로 시작한다.
            int second = passage.write(message, 0);

            assertThat(second).isBetween(0, message.length() - 1);
        } finally {
            passage.close();
            pair.accepted.close();
        }
    }

    @Test
    void awaitingWithoutReadInterestReturnsNoInboundWhetherOrNotWriteInterestIsSet() throws Exception {
        TcpLoopback pair = TcpLoopback.open();
        TcpPassage passage = TcpPassage.of(pair.client);
        try {
            assertThat(passage.await(1, false, false).kind()).isEqualTo(Inbound.Kind.NONE);
            assertThat(passage.await(1, false, true).kind()).isEqualTo(Inbound.Kind.NONE);
            assertThat(passage.await(1, true, true).kind()).isEqualTo(Inbound.Kind.NONE);
        } finally {
            passage.close();
            pair.accepted.close();
        }
    }
}
