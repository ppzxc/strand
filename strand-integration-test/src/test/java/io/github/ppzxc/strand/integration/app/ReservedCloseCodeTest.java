package io.github.ppzxc.strand.integration.app;

import static io.github.ppzxc.strand.integration.app.Rig.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.ErrorCode;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.nio.internal.TestMessages;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 상대가 예약 {@code error_code}(8 이상)로 CLOSE를 보내면 실제 nio 연결의 종료 알림 원인이 {@code INTERNAL_ERROR}다(규격 2.5.2절). 상대는 핸드셰이크를
 * 마친 core 클라이언트 엔드포인트가 실제 소켓으로 말하고, 예약 코드는 core가 보내지 못하게 막으므로 CLOSE 바이트의 {@code error_code}만 바꿔 보낸다.
 */
class ReservedCloseCodeTest {
    private static final int RESERVED_ERROR_CODE = 99;
    /** CLOSE 프레임에서 error_code가 시작하는 위치: 길이 4바이트와 헤더 나머지 16바이트 뒤(규격 2.1절, 3.5절). */
    private static final int ERROR_CODE_OFFSET = 20;

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
    }

    @Test
    void reservedErrorCodeInReceivedCloseReachesTheNoticeAsInternalError() throws Exception {
        Rig rig = new NioRig(false, new Spec());
        closeables.add(rig);
        AtomicReference<App.Conn> accepted = new AtomicReference<>();
        App.Server server = rig.server(new Rig.ServerSetup().factory(connection -> {
            accepted.set(connection);
            return new App.Prepared(request -> {}, () -> {});
        }));
        App.Lsn listener = Rig.get(server.listen());
        closeables.add(listener::close);
        Socket socket = new Socket(listener.address().getAddress(), listener.address().getPort());
        closeables.add(socket);
        socket.setSoTimeout(200);
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        handshake(client, socket);
        awaitTrue(() -> accepted.get() != null);

        client.close(ErrorCode.INTERNAL_ERROR);
        byte[] close = TestMessages.takeOutputs(client).get(0).bytes();
        close[ERROR_CODE_OFFSET + 3] = RESERVED_ERROR_CODE;
        socket.getOutputStream().write(close);
        socket.getOutputStream().flush();

        CloseNotice notice = rig.result(accepted.get().closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(ErrorCode.INTERNAL_ERROR);
        assertThat(notice.sender()).contains(CloseNotice.Sender.PEER);
    }

    /** 클라이언트 엔드포인트와 소켓 사이로 바이트를 옮겨 AUTH(ACK)를 받을 때까지 핸드셰이크한다. */
    private static void handshake(Endpoint client, Socket socket) throws IOException {
        byte[] buffer = new byte[8192];
        for (int round = 0; round < 100; round++) {
            for (OutboundMessage message : TestMessages.takeOutputs(client)) {
                socket.getOutputStream().write(message.bytes());
            }
            socket.getOutputStream().flush();
            try {
                int read = socket.getInputStream().read(buffer);
                if (read < 0) {
                    throw new AssertionError("핸드셰이크 중 서버가 연결을 끊었다");
                }
                TestMessages.deliver(client, Arrays.copyOf(buffer, read));
            } catch (SocketTimeoutException idle) {
                // 읽을 것이 없다. 아래에서 확인한다.
            }
            for (Event event : TestMessages.drainEvents(client)) {
                if (event.kind() == Event.Kind.AUTHENTICATED) {
                    return;
                }
            }
        }
        throw new AssertionError("핸드셰이크가 끝나지 않았다");
    }
}
