package io.github.ppzxc.strand.api;

import java.util.EnumMap;
import java.util.Map;

abstract class LimitBuilder<B extends LimitBuilder<B>> {
    final Map<Resource, Long> values = new EnumMap<>(Resource.class);

    abstract B self();

    private void put(Resource resource, long value) {
        if (value <= 0) throw new IllegalArgumentException(resource + " must be positive");
        values.put(resource, value);
    }

    public B outgoingRequests(long count, long bytes) {
        put(Resource.OUTGOING_REQUESTS, count);
        put(Resource.OUTGOING_BYTES, bytes);
        return self();
    }

    public B incomingRequests(long count, long bytes) {
        put(Resource.INCOMING_REQUESTS, count);
        put(Resource.INCOMING_BYTES, bytes);
        return self();
    }

    public B responseBytes(long bytes) {
        put(Resource.RESPONSE_BYTES, bytes);
        return self();
    }

    public B handlerTasks(long count) {
        put(Resource.HANDLER_TASKS, count);
        return self();
    }

    public B observations(long count) {
        put(Resource.OBSERVATIONS, count);
        return self();
    }

    public B preparationBytes(long bytes) {
        put(Resource.PREPARATION_BYTES, bytes);
        return self();
    }

    public B progressBytes(long frame, long control, long result, long plain, long cipher, long tlsControl) {
        if (frame < 20 || control < 32)
            throw new IllegalArgumentException("progress space cannot hold a minimal Strand frame");
        put(Resource.FRAME_ASSEMBLY_BYTES, frame);
        put(Resource.CONTROL_OUTPUT_BYTES, control);
        put(Resource.RESULT_EVENT_BYTES, result);
        put(Resource.TLS_PLAINTEXT_BYTES, plain);
        put(Resource.TLS_CIPHERTEXT_BYTES, cipher);
        put(Resource.TLS_CONTROL_BYTES, tlsControl);
        return self();
    }

    final Map<Resource, Long> finish(int expected) {
        if (values.size() != expected) throw new IllegalStateException("all limits must be configured");
        return new EnumMap<>(values);
    }

    final void set(Resource resource, long value) {
        put(resource, value);
    }
}
