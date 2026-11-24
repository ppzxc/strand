package io.github.ppzxc.strand.conformance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** 실행 경로 목록. 경로를 고르는 곳은 여기 하나다. */
final class ExecutionPaths {

    /** core 엔드포인트를 직접 구동한다. */
    static final ExecutionPath CORE = new MediumPath(ConformanceReport.CORE_PATH, VectorRunner.DIRECT);

    /** 제품 NIO 연결 엔진으로 실행한다. 전송 계층 쓰기 경계가 필요한 벡터만 지원한다. */
    static final ExecutionPath NIO = new NioPath();

    /** 제품 netty WebSocket 통로를 거쳐 실행한다. */
    static final ExecutionPath NETTY = new MediumPath(ConformanceReport.NETTY_PATH, NettyMessageWireHarness.MEDIUM);

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

    /** 엔드포인트를 {@link VectorRunner}로 구동하는 경로. 경로마다 다른 부분은 {@link VectorRunner.Medium}이 정한다. */
    private static final class MediumPath implements ExecutionPath {
        private final String name;
        private final VectorRunner.Medium medium;

        private MediumPath(String name, VectorRunner.Medium medium) {
            this.name = name;
            this.medium = medium;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public List<String> unsupportedFeatures(ConformanceVector vector) {
            return VectorRunner.unsupportedFeatures(vector, medium);
        }

        @Override
        public VectorOutcome evaluate(ConformanceVector vector, String vectorId) {
            return VectorRunner.evaluate(vector, vectorId, medium);
        }
    }

    /** 엔드포인트가 아니라 제품 NIO 연결 엔진을 구동하는 경로. */
    private static final class NioPath implements ExecutionPath {
        @Override
        public String name() {
            return ConformanceReport.NIO_PATH;
        }

        @Override
        public List<String> unsupportedFeatures(ConformanceVector vector) {
            return NioVectorRunner.unsupportedFeatures(vector);
        }

        @Override
        public VectorOutcome evaluate(ConformanceVector vector, String vectorId) {
            return NioVectorRunner.evaluate(vector, vectorId);
        }
    }
}
