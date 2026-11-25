package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

class TcpPassageBranchTest {

    @Test
    void writingAtOrPastTheEndOfAMessageWritesNothing() throws Exception {
        OutboundMessage message = TestMessages.largeRequests().get(0);
        TcpLoopback pair = TcpLoopback.open();
        TcpPassage passage = TcpPassage.of(pair.client);
        try {
            assertThat(TestMessages.write(passage, message, message.length())).isZero();
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

            int first = TestMessages.write(passage, message, 0);
            assertThat(first).isLessThan(message.length());

            int second = TestMessages.write(passage, message, 0);

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
