package io.github.ppzxc.strand.netty;

/** 클라이언트 {@code connect}가 준비 단계에서 실패했을 때 Future를 실패시키는 예외. 단계와 원인은 {@link #failure()}로 조회한다. */
public final class PreparationFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient PreparationFailure failure;

    PreparationFailedException(PreparationFailure failure) {
        super("연결 준비 실패: " + failure.stage(), failure.cause());
        this.failure = failure;
    }

    /**
     * 실패의 단계와 원인.
     *
     * @return 준비 실패 진단
     */
    public PreparationFailure failure() {
        return failure;
    }
}
