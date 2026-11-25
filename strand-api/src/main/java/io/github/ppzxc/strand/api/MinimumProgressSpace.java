package io.github.ppzxc.strand.api;

public final class MinimumProgressSpace {
    private final long frame;
    private final long control;
    private final long result;
    private final long plaintext;
    private final long ciphertext;
    private final long tlsControl;

    private MinimumProgressSpace(
            long frame, long control, long result, long plaintext, long ciphertext, long tlsControl) {
        this.frame = frame;
        this.control = control;
        this.result = result;
        this.plaintext = plaintext;
        this.ciphertext = ciphertext;
        this.tlsControl = tlsControl;
    }

    public static MinimumProgressSpace of(
            long frame, long control, long result, long plaintext, long ciphertext, long tlsControl) {
        if (frame < 0 || control < 0 || result < 0 || plaintext < 0 || ciphertext < 0 || tlsControl < 0) {
            throw new IllegalArgumentException("minimum progress space cannot be negative");
        }
        return new MinimumProgressSpace(frame, control, result, plaintext, ciphertext, tlsControl);
    }

    public long get(Resource resource) {
        switch (resource) {
            case FRAME_ASSEMBLY_BYTES:
                return frame;
            case CONTROL_OUTPUT_BYTES:
                return control;
            case RESULT_EVENT_BYTES:
                return result;
            case TLS_PLAINTEXT_BYTES:
                return plaintext;
            case TLS_CIPHERTEXT_BYTES:
                return ciphertext;
            case TLS_CONTROL_BYTES:
                return tlsControl;
            default:
                throw new IllegalArgumentException("not a progress resource: " + resource);
        }
    }
}
