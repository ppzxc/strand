package io.github.ppzxc.strand.conformance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** 실행 경로 목록. 경로를 고르는 곳은 여기 하나다. */
final class ExecutionPaths {

    /** core 엔드포인트를 직접 구동한다. */
    static final ExecutionPath CORE =
            new Provided(ConformanceReport.CORE_PATH, VectorRunner::unsupportedFeatures, VectorRunner::evaluate);

    /** 제품 NIO 연결 엔진으로 실행한다. 전송 계층 쓰기 경계가 필요한 벡터만 지원한다. */
    static final ExecutionPath NIO =
            new Provided(ConformanceReport.NIO_PATH, NioVectorRunner::unsupportedFeatures, NioVectorRunner::evaluate);

    /** 제품 netty WebSocket 통로를 거쳐 실행한다. */
    static final ExecutionPath NETTY = new Provided(
            ConformanceReport.NETTY_PATH,
            vector -> VectorRunner.unsupportedFeatures(vector, NettyMessageWireHarness.MEDIUM),
            (vector, vectorId) -> VectorRunner.evaluate(vector, vectorId, NettyMessageWireHarness.MEDIUM));

    private static final List<ExecutionPath> ALL = Collections.unmodifiableList(Arrays.asList(CORE, NIO, NETTY));

    private ExecutionPaths() {}

    /** 보고서 순서(core, nio, netty)의 모든 경로. */
    static List<ExecutionPath> all() {
        return ALL;
    }

    /**
     * 이름으로 경로를 찾는다.
     *
     * @throws IllegalArgumentException 그런 이름의 경로가 없을 때. 다른 경로로 대신하지 않는다
     */
    static ExecutionPath named(String name) {
        for (ExecutionPath path : ALL) {
            if (path.name().equals(name)) {
                return path;
            }
        }
        throw new IllegalArgumentException("알 수 없는 실행 경로: " + name);
    }

    private static final class Provided implements ExecutionPath {
        private final String name;
        private final java.util.function.Function<ConformanceVector, List<String>> unsupported;
        private final java.util.function.BiFunction<ConformanceVector, String, VectorOutcome> evaluator;

        private Provided(
                String name,
                java.util.function.Function<ConformanceVector, List<String>> unsupported,
                java.util.function.BiFunction<ConformanceVector, String, VectorOutcome> evaluator) {
            this.name = name;
            this.unsupported = unsupported;
            this.evaluator = evaluator;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<String> unsupportedFeatures(ConformanceVector vector) {
            return unsupported.apply(vector);
        }

        @Override
        public VectorOutcome evaluate(ConformanceVector vector, String vectorId) {
            return evaluator.apply(vector, vectorId);
        }
    }
}
