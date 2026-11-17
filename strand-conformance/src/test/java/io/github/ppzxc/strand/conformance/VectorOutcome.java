package io.github.ppzxc.strand.conformance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 벡터의 지원 사전검사와 실행 결과를 분리해 보관한다. */
final class VectorOutcome {

    enum Status {
        PASS,
        FAIL,
        UNSUPPORTED
    }

    private final String vectorId;
    private final Status status;
    private final List<String> details;

    private VectorOutcome(String vectorId, Status status, List<String> details) {
        this.vectorId = vectorId;
        this.status = status;
        this.details = Collections.unmodifiableList(new ArrayList<>(details));
    }

    static VectorOutcome pass(String vectorId) {
        return new VectorOutcome(vectorId, Status.PASS, Collections.emptyList());
    }

    static VectorOutcome fail(String vectorId, String reason) {
        return new VectorOutcome(vectorId, Status.FAIL, Collections.singletonList(String.valueOf(reason)));
    }

    static VectorOutcome unsupported(String vectorId, List<String> features) {
        return new VectorOutcome(vectorId, Status.UNSUPPORTED, features);
    }

    String vectorId() {
        return vectorId;
    }

    Status status() {
        return status;
    }

    List<String> details() {
        return details;
    }
}
