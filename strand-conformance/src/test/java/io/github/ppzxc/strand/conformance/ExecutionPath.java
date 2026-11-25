package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

interface ExecutionPath {

    String name();

    List<String> unsupportedFeatures(ConformanceVector vector);

    VectorOutcome evaluate(ConformanceVector vector, String vectorId);

    default void run(ConformanceVector vector) {
        VectorOutcome outcome = evaluate(vector, vector.id());
        assertThat(outcome.status())
                .as("%s 경로 벡터 %s: %s", name(), vector.id(), outcome.details())
                .isEqualTo(VectorOutcome.Status.PASS);
    }
}
