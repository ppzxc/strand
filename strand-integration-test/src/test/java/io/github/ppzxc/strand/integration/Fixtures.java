package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class Fixtures {

    static final long TIMEOUT_MS = 5000L;

    static final int APPLICATION_TYPE = 256;

    static final int PING_TYPE = 2;

    static final int CLOSE_TYPE = 3;

    static final int STATUS_OK = 0;

    static final int STATUS_UNAVAILABLE = 14;

    static final int CLOSE_FRAME_BYTES = 32;

    private Fixtures() {}

    static byte[] payload(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static <T> T await(Result<T> result) throws Exception {
        return result.toCompletionStage(Peer.HANDLERS).toCompletableFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    static Map<Long, Integer> requestTypeByStream(List<ControlledPassage.Frame> frames, boolean sentByClient) {
        Map<Long, Integer> typeByStream = new HashMap<>();
        for (ControlledPassage.Frame frame : frames) {
            boolean oddStream = (frame.streamId() & 1) == 1;
            if (frame.type() >= APPLICATION_TYPE && oddStream == sentByClient) {
                typeByStream.put(frame.streamId(), frame.type());
            }
        }
        return typeByStream;
    }
}
