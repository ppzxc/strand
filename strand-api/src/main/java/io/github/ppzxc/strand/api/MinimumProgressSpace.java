package io.github.ppzxc.strand.api;

/**
 * The minimum progress space one connection needs so that cancellation, result preservation and shutdown keep
 * working while resources are saturated. Both the per-connection and the per-owner limits must be at least this
 * value for every resource kind. A transport module computes it; an application reads it before choosing limits.
 *
 * <p>Plain (non-TLS) connections report 0 for the TLS kinds.
 */
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

    /** Creates a value. Transport modules call this; every amount is in bytes and must not be negative. */
    public static MinimumProgressSpace of(
            long frame, long control, long result, long plaintext, long ciphertext, long tlsControl) {
        if (frame < 0 || control < 0 || result < 0 || plaintext < 0 || ciphertext < 0 || tlsControl < 0) {
            throw new IllegalArgumentException("minimum progress space cannot be negative");
        }
        return new MinimumProgressSpace(frame, control, result, plaintext, ciphertext, tlsControl);
    }

    /**
     * The per-connection minimum, in bytes, of one progress resource.
     *
     * @param resource one of {@code FRAME_ASSEMBLY_BYTES}, {@code CONTROL_OUTPUT_BYTES}, {@code RESULT_EVENT_BYTES},
     *     {@code TLS_PLAINTEXT_BYTES}, {@code TLS_CIPHERTEXT_BYTES}, {@code TLS_CONTROL_BYTES}
     * @throws IllegalArgumentException for any other resource kind
     */
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
