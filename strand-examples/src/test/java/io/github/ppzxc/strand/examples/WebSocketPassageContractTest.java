package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.PassageContract;

/**
 * {@link WebSocketPassage}가 {@link PassageContract}를 지킨다. 두 통로를 {@link FakeWebSocket}으로 이어 소켓 없이 시험한다. JDK에는 WebSocket
 * 서버가 없어 실제 소켓으로 이은 짝은 만들 수 없다.
 */
class WebSocketPassageContractTest extends PassageContract {

    private WebSocketPassage peer;

    @Override
    protected Passage create() {
        WebSocketPassage passage = new WebSocketPassage();
        peer = new WebSocketPassage();
        FakeWebSocket.link(passage, peer);
        return passage;
    }

    @Override
    protected Passage peer() {
        return peer;
    }
}
