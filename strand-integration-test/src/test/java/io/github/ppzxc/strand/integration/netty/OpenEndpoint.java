package io.github.ppzxc.strand.integration.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.nio.internal.TestMessages;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

final class OpenEndpoint {

    static final int REQUEST_TYPE = 257;

    static final int FRAME_TYPE_RESET = 4;

    private OpenEndpoint() {}

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
