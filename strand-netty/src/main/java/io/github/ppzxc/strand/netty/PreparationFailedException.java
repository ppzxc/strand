package io.github.ppzxc.strand.netty;

public final class PreparationFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient PreparationFailure failure;

    PreparationFailedException(PreparationFailure failure) {
        super("연결 준비 실패: " + failure.stage(), failure.cause());
        this.failure = failure;
    }

    public PreparationFailure failure() {
        return failure;
    }
}
