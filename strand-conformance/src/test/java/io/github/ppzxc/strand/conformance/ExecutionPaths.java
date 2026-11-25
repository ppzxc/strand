package io.github.ppzxc.strand.conformance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

final class ExecutionPaths {

    static final ExecutionPath CORE = new MediumPath(ConformanceReport.CORE_PATH, VectorRunner.DIRECT);

    static final ExecutionPath NIO = new NioPath();

    static final ExecutionPath NETTY = new MediumPath(ConformanceReport.NETTY_PATH, NettyMessageWireHarness.MEDIUM);

    private static final List<ExecutionPath> ALL = Collections.unmodifiableList(Arrays.asList(CORE, NIO, NETTY));

    private ExecutionPaths() {}

    static List<ExecutionPath> all() {
        return ALL;
    }

    static ExecutionPath named(String name) {
        for (ExecutionPath path : ALL) {
            if (path.name().equals(name)) {
                return path;
            }
        }
        throw new IllegalArgumentException("알 수 없는 실행 경로: " + name);
    }

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
