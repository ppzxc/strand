package io.github.ppzxc.strand.conformance;

/** 적합성 벡터 하나. 로더가 형식을 검증한 뒤에만 만들어진다. */
final class Vector {
    enum Result {
        VALID,
        INVALID
    }

    private final String id;
    private final String description;
    private final Result result;
    private final byte[] bytes;

    Vector(String id, String description, Result result, byte[] bytes) {
        this.id = id;
        this.description = description;
        this.result = result;
        this.bytes = bytes.clone();
    }

    String id() {
        return id;
    }

    String description() {
        return description;
    }

    Result result() {
        return result;
    }

    byte[] bytes() {
        return bytes.clone();
    }
}
