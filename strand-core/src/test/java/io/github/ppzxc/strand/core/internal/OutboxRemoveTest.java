package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@link Outbox#remove}의 internal seam 테스트: 확인 대기가 붙은 노드는 거두지 않는다. 대기는 {@link Outbox#drain}만 시작시킨다. */
class OutboxRemoveTest {

    private static final byte[] CONTROL_FRAME = Frames.control(FrameType.PING, 0, new byte[8]);

    private final Outbox outbox = new Outbox();

    @Test
    void aNodeWithoutAWaitCanBeRemoved() {
        Outbox.Node node = outbox.addControl(OutboundMessage.binary(CONTROL_FRAME));

        outbox.remove(node);

        assertThat(outbox.isEmpty()).isTrue();
    }

    @Test
    void aNodeCarryingAWaitCannotBeRemoved() {
        Outbox.Node node = outbox.addControl(OutboundMessage.binary(CONTROL_FRAME));
        node.waitId = 1;

        assertThatThrownBy(() -> outbox.remove(node)).isInstanceOf(IllegalStateException.class);

        // 던진 뒤에도 노드는 그대로 남아 있어, 꺼내면 대기 식별자를 알려 준다.
        List<Long> started = new ArrayList<>();
        assertThat(outbox.drain(started::add)).hasSize(1);
        assertThat(started).containsExactly(1L);
    }
}
