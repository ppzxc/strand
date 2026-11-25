package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class InMemoryPassageContractTest {

    private static final long BLOCKED_AFTER_BYTES = 4096;

    @Nested
    class RealTime extends PassageContract {
        private InMemoryPassage peer;

        @Override
        protected Passage create() {
            InMemoryPassage.Pair pair = InMemoryPassage.streams();
            peer = pair.second();
            return pair.first();
        }

        @Override
        protected Passage peer() {
            return peer;
        }

        @Override
        protected Passage createBlockable() {
            InMemoryPassage.Pair pair = InMemoryPassage.streams();
            peer = pair.second();
            pair.first().writeCapacity(BLOCKED_AFTER_BYTES);
            return pair.first();
        }

        @Override
        protected byte[] unblockWrites(Passage blocked) {
            ((InMemoryPassage) blocked).addWriteCapacity(BLOCKED_AFTER_BYTES);
            return new byte[0];
        }
    }

    @Nested
    class ManualTime extends PassageContract {
        private InMemoryPassage peer;

        @Override
        protected Passage create() {
            InMemoryPassage.Pair pair = InMemoryPassage.streams();
            peer = pair.second();
            return pair.first().manualTime();
        }

        @Override
        protected Passage peer() {
            return peer;
        }

        @Override
        protected Passage createBlockable() {
            InMemoryPassage.Pair pair = InMemoryPassage.streams();
            peer = pair.second();
            pair.first().writeCapacity(BLOCKED_AFTER_BYTES);
            return pair.first().manualTime();
        }

        @Override
        protected byte[] unblockWrites(Passage blocked) {
            ((InMemoryPassage) blocked).addWriteCapacity(BLOCKED_AFTER_BYTES);
            return new byte[0];
        }

        @Override
        protected boolean timeoutElapsesInRealTime() {
            return false;
        }
    }

    @Test
    void deliveriesHeldBackAreNotReadUntilReleasedTogether() throws IOException {
        InMemoryPassage.Pair pair = InMemoryPassage.streams();
        InMemoryPassage reader = pair.first().manualTime();
        reader.holdDeliveries();

        pair.second().shutdownOutput();

        assertThat(reader.await(0).kind()).isEqualTo(Inbound.Kind.NONE);
        reader.releaseDeliveries();
        assertThat(reader.await(0).kind()).isEqualTo(Inbound.Kind.END);
    }
}
