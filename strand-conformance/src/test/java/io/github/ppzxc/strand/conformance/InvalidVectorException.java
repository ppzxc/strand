package io.github.ppzxc.strand.conformance;

/** 벡터 파일이 형식을 어겼다. */
final class InvalidVectorException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    InvalidVectorException(String message) {
        super(message);
    }
}
