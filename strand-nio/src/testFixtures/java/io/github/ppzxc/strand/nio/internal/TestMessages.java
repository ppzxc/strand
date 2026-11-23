package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/** 통로 시험이 쓰는 메시지 재료. 엔드포인트에서 얻어 오므로 실제 프레임이다. */
public final class TestMessages {

    private static final int APPLICATION_TYPE = 256;
    private static final int LARGE_BODY_BYTES = 60_000;
    private static final int LARGE_MESSAGES = 20;

    private static final CollectionSpace SPACE = CollectionSpace.of(1 << 16, 1 << 16);
    private static final int EVENT_BATCH = 1024;

    private TestMessages() {}

    /**
     * 엔드포인트가 지금 내보낼 출력을 진행 보고 방식으로 모두 꺼낸다. 출력마다 전체 길이를 쓴 것으로 보고한다.
     *
     * @param endpoint 엔드포인트
     * @return 쓴 순서대로의 출력 메시지
     */
    public static List<OutboundMessage> takeOutputs(Endpoint endpoint) {
        List<OutboundMessage> taken = new ArrayList<>();
        for (EndpointOutput output = endpoint.nextOutput(); output != null; output = endpoint.nextOutput()) {
            taken.add(output.message());
            endpoint.outputProgress(output.id(), output.length());
        }
        return taken;
    }

    /**
     * 받은 바이트를 한도 있는 입력으로 엔드포인트에 모두 넣는다. 수거 공간이 차면 사건을 수거하고 이어 넣으며, 수거한 사건은 버린다.
     *
     * @param endpoint 엔드포인트
     * @param bytes 받은 바이트
     */
    public static void deliver(Endpoint endpoint, byte[] bytes) {
        int position = 0;
        while (position < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(bytes, position, bytes.length - position, 64, SPACE);
            position += progress.consumed();
            if (progress.collectionRequired()) {
                drainEvents(endpoint);
            } else if (progress.consumed() == 0) {
                return;
            }
        }
    }

    /**
     * 쌓인 사건을 한도 있게 모두 수거한다.
     *
     * @param endpoint 엔드포인트
     * @return 수거한 사건
     */
    public static List<Event> drainEvents(Endpoint endpoint) {
        List<Event> all = new ArrayList<>();
        for (List<Event> batch = endpoint.drainEvents(EVENT_BATCH);
                !batch.isEmpty();
                batch = endpoint.drainEvents(EVENT_BATCH)) {
            all.addAll(batch);
        }
        return all;
    }

    /**
     * 엔드포인트가 만든 첫 메시지(연결 서문). 바이너리 메시지를 직접 만들 수 없어 엔드포인트에서 얻는다.
     *
     * @return 첫 메시지
     */
    public static OutboundMessage firstMessage() {
        return Endpoint.client(EndpointConfig.defaults()).nextOutput().message();
    }

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
            for (OutboundMessage message : takeOutputs(client)) {
                moved = true;
                if (message.length() >= LARGE_BODY_BYTES) {
                    requests.add(message);
                }
                deliver(server, message.bytes());
            }
            for (OutboundMessage message : takeOutputs(server)) {
                moved = true;
                deliver(client, message.bytes());
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
