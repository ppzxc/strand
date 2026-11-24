package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 실행 경로 목록: 경로는 한 곳에서 이름으로 고르고, 보고서에 찍히는 이름은 벡터 형식 문서의 값 그대로다. */
class ExecutionPathsTest {

    @Test
    void pathsAreCoreNioNettyInReportOrder() {
        List<String> names = new ArrayList<>();
        for (ExecutionPath path : ExecutionPaths.all()) {
            names.add(path.name());
        }

        assertThat(names)
                .containsExactly(ConformanceReport.CORE_PATH, ConformanceReport.NIO_PATH, ConformanceReport.NETTY_PATH);
    }

    @Test
    void aPathIsFoundByItsName() {
        assertThat(ExecutionPaths.named("netty")).isSameAs(ExecutionPaths.NETTY);
        assertThat(ExecutionPaths.named("nio")).isSameAs(ExecutionPaths.NIO);
        assertThat(ExecutionPaths.named("core")).isSameAs(ExecutionPaths.CORE);
    }

    @Test
    void anUnknownNameIsRejectedInsteadOfFallingBackToAnotherPath() {
        assertThatThrownBy(() -> ExecutionPaths.named("integrated")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void everyPathReportsAnUnsupportedVectorWithoutRunningIt() {
        ConformanceVector future = new ConformanceVector(
                "v",
                "d",
                Arrays.asList("req-ping-ack-reply"),
                JsonParser.parseString("{\"role\":\"server\",\"established\":true,\"future_setup\":1}")
                        .getAsJsonObject(),
                new ArrayList<>());

        for (ExecutionPath path : ExecutionPaths.all()) {
            assertThat(path.evaluate(future, "v").status()).as(path.name()).isEqualTo(VectorOutcome.Status.UNSUPPORTED);
        }
    }
}
