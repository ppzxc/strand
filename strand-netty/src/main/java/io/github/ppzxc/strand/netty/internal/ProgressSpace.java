package io.github.ppzxc.strand.netty.internal;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.Endpoint;
import org.jspecify.annotations.Nullable;

public final class ProgressSpace {
    static final long CONTROL_OUTPUT_UNIT = Endpoint.FRAME_HEADER_SIZE + Integer.BYTES + Long.BYTES;

    private final long frameAssembly;
    private final long controlOutput;
    private final long resultEvent;
    private final long tlsPlaintext;
    private final long tlsCiphertext;
    private final long tlsControl;

    private ProgressSpace(
            long frameAssembly,
            long controlOutput,
            long resultEvent,
            long tlsPlaintext,
            long tlsCiphertext,
            long tlsControl) {
        this.frameAssembly = frameAssembly;
        this.controlOutput = controlOutput;
        this.resultEvent = resultEvent;
        this.tlsPlaintext = tlsPlaintext;
        this.tlsCiphertext = tlsCiphertext;
        this.tlsControl = tlsControl;
    }

    public static ProgressSpace derive(
            long outgoingRequests, long incomingRequests, long maxFrameSize, @Nullable TlsSizes tls) {
        long control =
                Math.multiplyExact(Endpoint.outputsNeeded(outgoingRequests, 0, incomingRequests), CONTROL_OUTPUT_UNIT);
        long result = Math.addExact(maxFrameSize, Math.multiplyExact(Math.addExact(outgoingRequests, 1), (long)
                Endpoint.FRAME_HEADER_SIZE));
        if (tls == null) {
            return new ProgressSpace(maxFrameSize, control, result, 0, 0, 0);
        }
        return new ProgressSpace(
                maxFrameSize,
                control,
                result,
                tls.applicationBufferSize(),
                3L * tls.packetBufferSize(),
                tls.packetBufferSize());
    }

    public long frameAssembly() {
        return frameAssembly;
    }

    public long controlOutput() {
        return controlOutput;
    }

    public long resultEvent() {
        return resultEvent;
    }

    public long tlsPlaintext() {
        return tlsPlaintext;
    }

    public long tlsCiphertext() {
        return tlsCiphertext;
    }

    public long tlsControl() {
        return tlsControl;
    }

    public long[] minimums() {
        return new long[] {frameAssembly, controlOutput, resultEvent, tlsPlaintext, tlsCiphertext, tlsControl};
    }

    public MinimumProgressSpace toApi() {
        return MinimumProgressSpace.of(
                frameAssembly, controlOutput, resultEvent, tlsPlaintext, tlsCiphertext, tlsControl);
    }

    public void require(ResourceLimits limits) {
        long[] minimum = minimums();
        limits.requireProgress(minimum[0], minimum[1], minimum[2], minimum[3], minimum[4], minimum[5]);
    }

    public static int eventSlots(long resultEventBytes, long maxFrameSize) {
        long slots = 1 + Math.max(0, resultEventBytes - maxFrameSize) / Endpoint.FRAME_HEADER_SIZE;
        return (int) Math.min(Integer.MAX_VALUE, slots);
    }

    public static int outputSlots(long controlOutputBytes) {
        return (int) Math.min(Integer.MAX_VALUE, controlOutputBytes / CONTROL_OUTPUT_UNIT);
    }
}
