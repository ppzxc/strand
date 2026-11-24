package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

/**
 * 벡터를 실행하는 경로 하나(벡터 형식 문서의 실행 경로). 경로마다 지원하는 벡터와 실행 방법이 다르므로, 호출하는 쪽은 경로를 {@link ExecutionPaths}에서 한 번 고르고
 * 이 인터페이스만 본다. 경로 이름은 보고서에 찍히는 값이다.
 */
interface ExecutionPath {

    /** 보고서와 경로 선택에 쓰는 이름. */
    String name();

    /** 이 경로가 실행하지 못하는 부분. 한 단계도 실행하기 전에 판정하며, 비어 있으면 지원한다. */
    List<String> unsupportedFeatures(ConformanceVector vector);

    /** 벡터를 실행한다. 미지원이면 아무것도 실행하지 않고 미지원으로 보고하며, 어긋남은 실패로 보고한다. */
    VectorOutcome evaluate(ConformanceVector vector, String vectorId);

    /** 벡터를 실행해 통과하지 않으면 {@link AssertionError}를 던진다. */
    default void run(ConformanceVector vector) {
        VectorOutcome outcome = evaluate(vector, vector.id());
        assertThat(outcome.status())
                .as("%s 경로 벡터 %s: %s", name(), vector.id(), outcome.details())
                .isEqualTo(VectorOutcome.Status.PASS);
    }
}
