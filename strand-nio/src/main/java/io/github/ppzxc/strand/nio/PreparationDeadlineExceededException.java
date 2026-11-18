package io.github.ppzxc.strand.nio;

/** 준비 기한(클라이언트 호출 또는 서버 TCP 수락부터 핸들러 등록까지)이 지났다. */
public final class PreparationDeadlineExceededException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    PreparationDeadlineExceededException() {
        super("준비 기한이 지났다");
    }
}
