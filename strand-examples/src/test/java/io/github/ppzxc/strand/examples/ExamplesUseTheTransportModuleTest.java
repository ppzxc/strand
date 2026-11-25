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

class ExamplesUseTheTransportModuleTest {

    private static final Path SOURCES = Paths.get("src", "main", "java", "io", "github", "ppzxc", "strand", "examples");

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
    void theTcpExamplesAndTheAppApiExamplesExist() throws IOException {
        assertThat(exampleSources())
                .extracting(path -> path.getFileName().toString())
                .contains(
                        "TcpClientExample.java",
                        "ServerExample.java",
                        "NioAppExample.java",
                        "NettyAppExample.java",
                        "NettyWebSocketExample.java");
    }

    @Test
    void onlyTheNettyModuleHasAWebSocketExample() throws IOException {

        assertThat(exampleSources())
                .extracting(path -> path.getFileName().toString())
                .filteredOn(name -> name.contains("WebSocket"))
                .containsExactly("NettyWebSocketExample.java");
    }

    @Test
    void noExampleImportsAnInternalPackage() throws IOException {
        Pattern internalImport = Pattern.compile("(?m)^import\\s+(static\\s+)?[\\w.]*\\.internal\\.");
        for (Path source : exampleSources()) {
            String text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
            assertThat(internalImport.matcher(text).find())
                    .as(source.getFileName() + "가 internal 패키지를 import한다")
                    .isFalse();
        }
    }

    @Test
    void noExampleReferencesTheEndpointTypeItself() throws IOException {

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
