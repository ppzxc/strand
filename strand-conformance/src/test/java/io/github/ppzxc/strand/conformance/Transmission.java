package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.util.List;
import java.util.Map;

/** 한 번의 실행에서 엔드포인트와 상대 사이를 오가는 입력과 출력이 지나는 길. 경로마다 구현이 다르고, 러너는 이 인터페이스만 본다. */
interface Transmission {

    /** 수립 스크립트의 입력 하나(프레임 바이트)를 이 경로의 방식으로 엔드포인트에 넣는다. */
    void deliver(EndpointDriver driver, byte[] bytes);

    /** 벡터의 단계 하나를 엔드포인트에 적용한다. */
    void apply(EndpointDriver driver, JsonObject step, Map<String, Long> requestTickets);

    /** 엔드포인트가 보낸 메시지가 이 경로를 지나 상대에게 닿은 바이트로, 메시지 단위로 돌려준다. */
    List<byte[]> transmit(List<OutboundMessage> sent);

    /** 단계 뒤 이벤트로부터 이 경로만 관측하는 출력(예: WebSocket Close)을 `send` 표기로 돌려준다. */
    List<String> observeAfter(List<Event> events);
}
