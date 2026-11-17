package io.github.ppzxc.strand.conformance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 실행 경로별 결과와 벡터별 통합 결과를 집계한다. */
final class ConformanceReport {

    private final Map<String, VectorOutcome> byVector;

    private ConformanceReport(Map<String, VectorOutcome> byVector) {
        this.byVector = Collections.unmodifiableMap(new LinkedHashMap<>(byVector));
    }

    static ConformanceReport of(List<VectorOutcome> outcomes) {
        Map<String, VectorOutcome> indexed = new LinkedHashMap<>();
        for (VectorOutcome outcome : outcomes) {
            if (indexed.put(outcome.vectorId(), outcome) != null) {
                throw new IllegalArgumentException("중복 벡터 결과: " + outcome.vectorId());
            }
        }
        return new ConformanceReport(indexed);
    }

    int count(VectorOutcome.Status status) {
        int count = 0;
        for (VectorOutcome outcome : byVector.values()) {
            if (outcome.status() == status) count++;
        }
        return count;
    }

    List<VectorOutcome> outcomes() {
        return Collections.unmodifiableList(new ArrayList<>(byVector.values()));
    }

    boolean passes() {
        return count(VectorOutcome.Status.FAIL) == 0 && count(VectorOutcome.Status.UNSUPPORTED) == 0;
    }

    String summary() {
        List<String> details = new ArrayList<>();
        for (VectorOutcome outcome : byVector.values()) {
            if (outcome.status() != VectorOutcome.Status.PASS) {
                details.add(outcome.vectorId() + " " + outcome.status() + " " + outcome.details());
            }
        }
        return "pass=" + count(VectorOutcome.Status.PASS) + ", fail=" + count(VectorOutcome.Status.FAIL)
                + ", unsupported=" + count(VectorOutcome.Status.UNSUPPORTED) + "; " + details;
    }
}
