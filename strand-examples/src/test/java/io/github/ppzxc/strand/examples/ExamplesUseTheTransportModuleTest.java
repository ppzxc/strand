package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * 예제는 전송 모듈 사용법만 보여 준다. 손으로 쓴 드라이브 루프(읽은 것을 넣고, 쌓인 것을 꺼내 쓰고, 시계를 진행)는 연결 드라이버가 하는 일이므로
 * 예제 소스에 {@code Endpoint}를 직접 부르는 호출이 없어야 한다.
 */
class ExamplesUseTheTransportModuleTest {

    private static final Path SOURCES = Paths.get("src", "main", "java", "io", "github", "ppzxc", "strand", "examples");

    /** {@code Endpoint}의 드라이브용 동작. 요청과 응답은 연결 드라이버가 낸다. */
    private static final List<String> DRIVE_CALLS = Arrays.asList(
            "Endpoint.client(",
            "Endpoint.server(",
            ".receive(",
            ".receiveMessage(",
            ".advance(",
            ".drainMessages(",
            ".drainEvents(",
            ".nextDeadline(",
            ".disconnect(");

    private static List<Path> exampleSources() throws IOException {
        try (Stream<Path> files = Files.list(SOURCES)) {
            return files.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList());
        }
    }

    @Test
    void theFourExamplesAndTheirSupportExist() throws IOException {
        assertThat(exampleSources())
                .extracting(path -> path.getFileName().toString())
                .contains(
                        "BlockingTcpExample.java", "NioTcpExample.java", "ServerExample.java", "WebSocketExample.java");
    }

    @Test
    void noExampleReferencesTheEndpointTypeItself() throws IOException {
        // 변수로 받은 Endpoint의 ping, close, request 호출도 막으려고 타입 참조 자체를 금지한다. EndpointConfig는 설정이라 쓴다.
        Pattern endpointType = Pattern.compile("\\bEndpoint\\b");
        for (Path source : exampleSources()) {
            String text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
            assertThat(endpointType.matcher(text).find())
                    .as(source.getFileName() + "가 Endpoint 타입을 참조한다")
                    .isFalse();
        }
    }

    @Test
    void noExampleCallsTheEndpointDirectly() throws IOException {
        for (Path source : exampleSources()) {
            String text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
            for (String call : DRIVE_CALLS) {
                assertThat(text).as(source.getFileName() + "의 " + call).doesNotContain(call);
            }
        }
    }
}
