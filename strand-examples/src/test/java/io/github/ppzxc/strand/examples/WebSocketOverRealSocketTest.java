package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 실제 JDK WebSocket 클라이언트가 시험 안의 로컬 서버({@link LocalWebSocketServer})와 소켓으로 통신한다. {@link FakeWebSocket}이 가리는 JDK의
 * 비동기 콜백, 수요({@code request}) 흐름, 하위 프로토콜 검사, 연결 실패를 확인한다. 포트는 0이고 기다림은 상한 시간뿐이다.
 */
class WebSocketOverRealSocketTest {

    private static final long LIMIT_MS = 5000;

    private final ExecutorService handlers = ExampleSupport.newHandlerExecutor("test-handler");
    private final List<AutoCloseable> toClose = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : toClose) {
            closeable.close();
        }
        handlers.shutdownNow();
    }

    private LocalWebSocketServer serve(boolean chooseSubprotocol, LocalWebSocketServer.SessionHandler handler)
            throws IOException {
        LocalWebSocketServer server = LocalWebSocketServer.start(chooseSubprotocol, handler);
        toClose.add(server);
        return server;
    }

    private WebSocketPassage connect(LocalWebSocketServer server) throws IOException {
        WebSocketPassage passage = WebSocketPassage.connect(server.uri());
        toClose.add(passage::close);
        return passage;
    }

    /** 상한 시간 안에서 NONE이 아닌 것이 올 때까지 읽는다. */
    private static Inbound awaitSomething(WebSocketPassage passage) throws IOException {
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LIMIT_MS);
        while (System.nanoTime() < limit) {
            Inbound inbound = passage.await(LIMIT_MS);
            if (inbound.kind() != Inbound.Kind.NONE) {
                return inbound;
            }
        }
        throw new AssertionError("상한 시간 안에 아무것도 오지 않았다");
    }

    @Test
    void theWebSocketExampleExchangesOneRequestWithAStrandServerOverARealSocket() throws Exception {
        LocalWebSocketServer server = serve(true, session -> {
            ConnectionDriver driver = ConnectionDriver.server(
                            ExampleSupport.withAuthenticator(
                                    EndpointConfig.defaults().webSocket()),
                            new LocalWebSocketServer.SessionPassage(session))
                    .handler(handlers, request -> {
                        String body = new String(request.body(), StandardCharsets.UTF_8);
                        request.respond(ExampleSupport.STATUS_OK, ("echo:" + body).getBytes(StandardCharsets.UTF_8));
                    })
                    .start();
            toClose.add(driver);
        });

        RequestResult result = WebSocketExample.run(server.uri());

        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(result.status()).isEqualTo(ExampleSupport.STATUS_OK);
        assertThat(new String(result.body(), StandardCharsets.UTF_8)).isEqualTo("echo:hello");
    }

    @Test
    void connectingFailsWhenTheServerDoesNotChooseTheStrandSubprotocol() throws Exception {
        LocalWebSocketServer server = serve(false, session -> {});

        assertThatThrownBy(() -> WebSocketPassage.connect(server.uri()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("strand");
    }

    @Test
    void connectingToAClosedPortIsAnIoException() throws Exception {
        URI unused;
        try (ServerSocket reserved = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            unused = URI.create("ws://127.0.0.1:" + reserved.getLocalPort() + "/");
        }

        assertThatThrownBy(() -> WebSocketPassage.connect(unused)).isInstanceOf(IOException.class);
    }

    @Test
    void fragmentsTheServerSendsArriveAsOneBinaryMessage() throws Exception {
        LocalWebSocketServer server = serve(true, session -> {
            session.send(2, false, new byte[] {1, 2});
            session.send(0, false, new byte[] {3});
            session.send(0, true, new byte[] {4, 5});
        });
        WebSocketPassage passage = connect(server);

        Inbound inbound = awaitSomething(passage);

        assertThat(inbound.kind()).isEqualTo(Inbound.Kind.MESSAGE);
        assertThat(inbound.data()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void severalMessagesAreAllDeliveredInOrderOneRequestAtATime() throws Exception {
        LocalWebSocketServer server = serve(true, session -> {
            for (int i = 0; i < 5; i++) {
                session.send(2, true, new byte[] {(byte) i});
            }
        });
        WebSocketPassage passage = connect(server);

        for (int i = 0; i < 5; i++) {
            assertThat(awaitSomething(passage).data()).containsExactly(i);
        }
    }

    @Test
    void aCloseTheServerSendsEndsTheReading() throws Exception {
        LocalWebSocketServer server = serve(true, session -> session.send(8, true, new byte[] {0x03, (byte) 0xE8}));
        WebSocketPassage passage = connect(server);

        assertThat(awaitSomething(passage).kind()).isEqualTo(Inbound.Kind.END);
    }

    @Test
    void aMessageWeWriteReachesTheServerAsOneMaskedBinaryFrame() throws Exception {
        CompletableFuture<LocalWebSocketServer.Frame> received = new CompletableFuture<>();
        LocalWebSocketServer server = serve(true, session -> received.complete(session.readFrame()));
        WebSocketPassage passage = connect(server);
        OutboundMessage message = Endpoint.client(EndpointConfig.defaults().webSocket())
                .drainMessages()
                .get(0);

        passage.write(message);

        LocalWebSocketServer.Frame frame = received.get(LIMIT_MS, TimeUnit.MILLISECONDS);
        assertThat(frame.opcode).isEqualTo(2);
        assertThat(frame.fin).isTrue();
        assertThat(frame.payload).isEqualTo(message.bytes());
    }
}
