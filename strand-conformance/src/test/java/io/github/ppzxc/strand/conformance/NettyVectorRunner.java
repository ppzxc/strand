package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

/**
 * 제품 netty WebSocket 통로를 거쳐 벡터를 실행하는 세 번째 실행 경로. {@code transport: websocket} 벡터와 이 통로로 관측할 수 있는 것(WebSocket Close 포함)을 지원하고,
 * 나머지(바이트 스트림 벡터)는 경로별 미지원으로 보고한다. 통로는 제품 코덱을 {@link NettyPipeline}으로 {@code EmbeddedChannel}에 얹은 것이다.
 */
final class NettyVectorRunner {

    private NettyVectorRunner() {}

    static List<String> unsupportedFeatures(ConformanceVector vector) {
        return VectorRunner.unsupportedFeaturesOnNetty(vector);
    }

    static VectorOutcome evaluate(ConformanceVector vector, String vectorId) {
        return VectorRunner.evaluateOnNetty(vector, vectorId);
    }

    static void run(ConformanceVector vector) {
        VectorOutcome outcome = evaluate(vector, vector.id());
        assertThat(outcome.status())
                .as("netty 경로 벡터 %s: %s", vector.id(), outcome.details())
                .isEqualTo(VectorOutcome.Status.PASS);
    }
}
