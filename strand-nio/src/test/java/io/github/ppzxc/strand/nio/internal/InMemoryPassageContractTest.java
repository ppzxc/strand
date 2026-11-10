package io.github.ppzxc.strand.nio.internal;

import org.junit.jupiter.api.Nested;

/** {@link InMemoryPassage}가 {@link PassageContract}를 지킨다. 실제 시간 모드와 수동 시간 모드 둘 다 시험한다. */
class InMemoryPassageContractTest {

    @Nested
    class RealTime extends PassageContract {
        @Override
        protected Passage create() {
            return InMemoryPassage.streams().first();
        }
    }

    @Nested
    class ManualTime extends PassageContract {
        @Override
        protected Passage create() {
            return InMemoryPassage.streams().first().manualTime();
        }

        @Override
        protected boolean timeoutElapsesInRealTime() {
            return false;
        }
    }
}
