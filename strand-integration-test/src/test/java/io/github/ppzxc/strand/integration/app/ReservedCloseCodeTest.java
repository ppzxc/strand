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

class ReservedCloseCodeTest {
    private static final int RESERVED_ERROR_CODE = 99;

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
        Socket socket =
                new Socket(listener.address().getAddress(), listener.address().getPort());
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
