package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/** 통로 시험이 쓰는 메시지 재료. 엔드포인트에서 얻어 오므로 실제 프레임이다. */
public final class TestMessages {

    private static final int APPLICATION_TYPE = 256;
    private static final int LARGE_BODY_BYTES = 60_000;
    private static final int LARGE_MESSAGES = 20;

    private TestMessages() {}

    /**
     * 엔드포인트 둘을 직접 이어 핸드셰이크를 마치고, 클라이언트가 보내는 큰 요청 메시지들을 얻는다. 통로는 쓰지 않는다.
     *
     * @return 큰 요청 메시지들
     */
    public static List<OutboundMessage> largeRequests() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        List<OutboundMessage> requests = new ArrayList<>();
        for (int i = 0; i < LARGE_MESSAGES; i++) {
            client.request(APPLICATION_TYPE, new byte[LARGE_BODY_BYTES]);
        }
        boolean moved = true;
        while (moved) {
            moved = false;
            for (OutboundMessage message : client.drainMessages()) {
                moved = true;
                if (message.length() >= LARGE_BODY_BYTES) {
                    requests.add(message);
                }
                server.receive(message.bytes());
            }
            for (OutboundMessage message : server.drainMessages()) {
                moved = true;
                client.receive(message.bytes());
            }
        }
        assertThat(requests).hasSize(LARGE_MESSAGES);
        return requests;
    }

    /**
     * 메시지들의 바이트 길이 합.
     *
     * @param messages 메시지들
     * @return 합
     */
    public static int totalBytes(List<OutboundMessage> messages) {
        int total = 0;
        for (OutboundMessage message : messages) {
            total += message.length();
        }
        return total;
    }

    /**
     * 메시지들을 순서대로 이어 붙인 바이트.
     *
     * @param messages 메시지들
     * @return 이어 붙인 바이트
     */
    public static byte[] concatenated(List<OutboundMessage> messages) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (OutboundMessage message : messages) {
            byte[] bytes = message.bytes();
            out.write(bytes, 0, bytes.length);
        }
        return out.toByteArray();
    }
}
