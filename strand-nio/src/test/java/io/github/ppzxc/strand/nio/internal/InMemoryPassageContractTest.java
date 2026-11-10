package io.github.ppzxc.strand.nio.internal;

import org.junit.jupiter.api.Nested;

/** {@link InMemoryPassage}가 {@link PassageContract}를 지킨다. 실제 시간 모드와 수동 시간 모드 둘 다 시험한다. */
class InMemoryPassageContractTest {

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
        protected boolean timeoutElapsesInRealTime() {
            return false;
        }
    }
}
