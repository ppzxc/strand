package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 관찰 지점(받은 바이트 수, 보낸 바이트 수, 보낸 프레임 수)을 전송 모듈의 interface로 확인한다. 기대값은 통로가 실제로 쓴 메시지({@link
 * InMemoryPassage#written()})에서 독립적으로 얻는다. 수동 시간 통로와 {@link InMemoryPassage#awaitQuiescence}로 양쪽이 더 할 일이 없을 때 읽는다.
 */
class ConnectionObservationTest {

    private static final int APPLICATION_TYPE = 256;
    /** 클라이언트의 첫 메시지는 8바이트 연결 서문이다(규격 3.1절, 통로 종류와 무관). 프레임이 아니고 서버는 서문을 보내지 않는다. */
    private static final int CLIENT_PREFACE_MESSAGES = 1;

    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private ConnectionDriver start(ConnectionDriver.Builder builder) {
        ConnectionDriver driver = builder.clock(() -> 0L).start();
        started.add(driver);
        return driver;
    }

    private static long totalBytes(List<byte[]> messages) {
        long total = 0;
        for (byte[] message : messages) {
            total += message.length;
        }
        return total;
    }

    @Test
    void aClientAloneHasSentItsPrefaceAndHelloAndReceivedNothing() {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        ConnectionDriver client = start(
                ConnectionDriver.client(EndpointConfig.defaults(), link.first().manualTime()));

        InMemoryPassage.awaitQuiescence(link.first());

        List<byte[]> written = link.first().written();
        assertThat(client.writtenBytes()).isEqualTo(totalBytes(written));
        assertThat(client.writtenFrames()).isEqualTo(written.size() - CLIENT_PREFACE_MESSAGES);
        assertThat(client.writtenFrames()).isPositive();
        assertThat(client.receivedBytes()).isZero();
    }

    @Test
    void bytesAndFramesFollowWhatBothSidesActuallyExchanged() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        ConnectionDriver server = start(ConnectionDriver.server(
                        EndpointConfig.defaults(), link.second().manualTime())
                .handler(
                        handlers,
                        request -> request.respond(0, "re".getBytes(StandardCharsets.UTF_8))
                                .join()));
        ConnectionDriver client = start(
                ConnectionDriver.client(EndpointConfig.defaults(), link.first().manualTime()));
        CompletableFuture<RequestResult> result = client.request(
                        APPLICATION_TYPE, "hello".getBytes(StandardCharsets.UTF_8))
                .result();
        result.get(5, TimeUnit.SECONDS);

        InMemoryPassage.awaitQuiescence(link.first(), link.second());

        List<byte[]> clientWrote = link.first().written();
        List<byte[]> serverWrote = link.second().written();
        assertThat(client.writtenBytes()).isEqualTo(totalBytes(clientWrote));
        assertThat(server.writtenBytes()).isEqualTo(totalBytes(serverWrote));
        assertThat(client.writtenFrames()).isEqualTo(clientWrote.size() - CLIENT_PREFACE_MESSAGES);
        assertThat(server.writtenFrames()).isEqualTo(serverWrote.size());
        // 한쪽이 보낸 바이트는 상대가 받아 처리한 바이트와 같다.
        assertThat(server.receivedBytes()).isEqualTo(client.writtenBytes());
        assertThat(client.receivedBytes()).isEqualTo(server.writtenBytes());
    }

    @Test
    void onAMessagePassageEveryWrittenMessageButThePrefaceIsAFrame() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        EndpointConfig config = EndpointConfig.defaults().webSocket();
        ConnectionDriver server = start(ConnectionDriver.server(
                        config, link.second().manualTime())
                .handler(handlers, request -> request.respond(0, new byte[0]).join()));
        ConnectionDriver client =
                start(ConnectionDriver.client(config, link.first().manualTime()));
        client.request(APPLICATION_TYPE, new byte[3]).result().get(5, TimeUnit.SECONDS);

        InMemoryPassage.awaitQuiescence(link.first(), link.second());

        assertThat(client.writtenFrames()).isEqualTo(link.first().written().size() - CLIENT_PREFACE_MESSAGES);
        assertThat(server.writtenFrames()).isEqualTo(link.second().written().size());
        assertThat(server.receivedBytes()).isEqualTo(client.writtenBytes());
        assertThat(client.receivedBytes()).isEqualTo(server.writtenBytes());
    }
}
