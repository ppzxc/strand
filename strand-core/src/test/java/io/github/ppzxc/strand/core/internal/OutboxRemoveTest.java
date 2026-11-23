package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** {@link Outbox#remove}의 internal seam 테스트: 확인 대기가 붙은 노드는 거두지 않는다. */
class OutboxRemoveTest {

    private static final byte[] CONTROL_FRAME = Frames.control(FrameType.PING, 0, new byte[8]);

    private final Outbox outbox = new Outbox();

    @Test
    void aNodeWithoutAWaitCanBeRemoved() {
        Outbox.Node node = outbox.addControl(OutboundMessage.of(CONTROL_FRAME));

        outbox.remove(node);

        assertThat(outbox.isEmpty()).isTrue();
    }

    @Test
    void aNodeCarryingAWaitCannotBeRemoved() {
        Outbox.Node node = outbox.addControl(OutboundMessage.of(CONTROL_FRAME));
        node.waitId = 1;

        assertThatThrownBy(() -> outbox.remove(node)).isInstanceOf(IllegalStateException.class);

        // 던진 뒤에도 노드는 그대로 남아 있다.
        assertThat(outbox.first()).isSameAs(node);
        assertThat(outbox.size()).isEqualTo(1);
    }
}
