package io.github.ppzxc.strand.integration.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.nio.internal.TestMessages;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/** 검증용 core 엔드포인트와 와이어 프레임 해석. 상대(클라이언트) 엔드포인트와 메모리로 핸드셰이크를 마친 열린 서버를 만든다. */
final class OpenEndpoint {
    /** 규격 2.1절: 프레임은 4바이트 길이(뒤따르는 바이트 수) 뒤에 20바이트 헤더 나머지와 본문이 온다. 응용 요청 type. */
    static final int REQUEST_TYPE = 257;

    static final int FRAME_TYPE_RESET = 4;

    private OpenEndpoint() {}

    /** 상대가 max_frame_size 1 MiB를 알린, 핸드셰이크를 마친 서버 엔드포인트. 이후 출력은 진행 보고 방식으로만 꺼낸다. */
    static Endpoint server() {
        EndpointConfig config = EndpointConfig.defaults().maxFrameSize(1_048_576);
        Endpoint client = Endpoint.client(config);
        Endpoint server = Endpoint.server(config);
        for (int round = 0; round < 8; round++) {
            boolean moved = relay(client, server) | relay(server, client);
            if (!moved) break;
        }
        TestMessages.drainEvents(server);
        TestMessages.drainEvents(client);
        return server;
    }

    private static boolean relay(Endpoint from, Endpoint to) {
        boolean moved = false;
        for (OutboundMessage message : TestMessages.takeOutputs(from)) {
            TestMessages.deliver(to, message.bytes());
            moved = true;
        }
        return moved;
    }

    /** 바이트열을 프레임 단위로 자른다. 끝에 모자란 조각이 있으면 마지막 원소로 남긴다. */
    static List<byte[]> frames(byte[] wire) {
        List<byte[]> frames = new ArrayList<>();
        int at = 0;
        while (at < wire.length) {
            if (wire.length - at < 4) {
                frames.add(java.util.Arrays.copyOfRange(wire, at, wire.length));
                break;
            }
            long length = ((wire[at] & 0xffL) << 24)
                    | ((wire[at + 1] & 0xffL) << 16)
                    | ((wire[at + 2] & 0xffL) << 8)
                    | (wire[at + 3] & 0xffL);
            int end = (int) Math.min(wire.length, at + 4 + length);
            frames.add(java.util.Arrays.copyOfRange(wire, at, end));
            at = end;
        }
        return frames;
    }

    static boolean isComplete(byte[] frame) {
        if (frame.length < 4) return false;
        long length = ((frame[0] & 0xffL) << 24)
                | ((frame[1] & 0xffL) << 16)
                | ((frame[2] & 0xffL) << 8)
                | (frame[3] & 0xffL);
        return frame.length == 4 + length;
    }

    static int typeOf(byte[] frame) {
        return ((frame[4] & 0xff) << 8) | (frame[5] & 0xff);
    }

    static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) out.write(part, 0, part.length);
        return out.toByteArray();
    }
}
