package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.RequestResult;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TcpExamplesTest {

    private ServerExample.Server server;

    @BeforeEach
    void startServer() throws Exception {
        server = ServerExample.start(0);
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    private static void assertEchoed(RequestResult result) {
        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(result.status()).isEqualTo(ExampleSupport.STATUS_OK);
        assertThat(new String(result.payload(), StandardCharsets.UTF_8)).isEqualTo("echo:hello");
    }

    @Test
    void theTcpClientAndTheServerExchangeOneRequest() throws Exception {
        assertEchoed(TcpClientExample.run("localhost", server.port()));
    }
}
