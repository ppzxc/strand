package io.github.ppzxc.strand.netty;

public final class PreparationDeadlineExceededException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    PreparationDeadlineExceededException() {
        super("준비 기한이 지났다");
    }
}
