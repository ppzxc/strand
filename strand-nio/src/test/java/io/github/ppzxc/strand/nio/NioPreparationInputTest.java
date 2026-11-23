package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Resource;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 팩터리가 끝나지 않은 준비 중의 입력을 와이어 수준 상대로 확인한다(규격 3.3~3.5절). AUTH 관문을 통과한 뒤의 PING은 확인(ACK)되고, 요청은
 * 활성화까지 보관되며, 두 번째 AUTH는 연결 오류이고, CLOSE는 준비를 실패시킨다(#319).
 */
class NioPreparationInputTest {
    private static final int HELLO = 0x0001;
    private static final int PING = 0x0002;
    private static final int CLOSE = 0x0003;
    private static final int RESET = 0x0004;
    private static final int AUTH = 0x0005;
    private static final int ACK = 0x01;
    private static final int PROTOCOL_ERROR = 1;

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private static void awaitTrue(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("조건이 시간 안에 참이 되지 않았다");
            }
            Thread.sleep(10);
        }
    }

    private static byte[] frame(int type, int flags, long streamId, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int length = 16 + payload.length;
        out.write(length >>> 24);
        out.write(length >>> 16);
        out.write(length >>> 8);
        out.write(length);
        out.write(type >>> 8);
        out.write(type);
        out.write(flags);
        out.write(0);
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) (streamId >>> shift));
        }
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    /** 서문과 HELLO를 교환하고 AUTH를 보내 서버의 AUTH(ACK)까지 받는 원시 클라이언트. */
    private static final class RawPeer implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream in;
        private final OutputStream out;

        RawPeer(InetSocketAddress address) throws IOException {
            socket = new Socket(address.getAddress(), address.getPort());
            socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
            in = new DataInputStream(socket.getInputStream());
            out = socket.getOutputStream();
        }

        void write(byte[] bytes) throws IOException {
            out.write(bytes);
            out.flush();
        }

        void handshakeAndAuthenticate() throws IOException {
            write(new byte[] {(byte) 0xC0, (byte) 0xDE, 0x53, 0x54, 0x52, 0x30, 0x00, 0x00});
            write(frame(HELLO, 0, 0, new byte[0]));
            readUntil(HELLO, 0);
            write(frame(HELLO, ACK, 0, new byte[0]));
            byte[] mechanism = "TOKEN".getBytes(StandardCharsets.US_ASCII);
            byte[] creds = "secret".getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            payload.write(mechanism.length);
            payload.write(mechanism, 0, mechanism.length);
            payload.write(creds, 0, creds.length);
            write(frame(AUTH, 0, 0, payload.toByteArray()));
            readUntil(AUTH, ACK);
        }

        /** 원하는 type·flags의 프레임이 올 때까지 읽는다. 연결이 끝나면 null. */
        byte[] readUntil(int type, int flags) throws IOException {
            while (true) {
                byte[] frame = readFrame();
                if (frame == null) {
                    return null;
                }
                int gotType = ((frame[4] & 0xFF) << 8) | (frame[5] & 0xFF);
                if (gotType == type && (frame[6] & 0xFF) == flags) {
                    return frame;
                }
            }
        }

        byte[] readFrame() throws IOException {
            byte[] head = new byte[4];
            try {
                in.readFully(head);
            } catch (java.io.EOFException e) {
                return null;
            }
            int length =
                    ((head[0] & 0xFF) << 24) | ((head[1] & 0xFF) << 16) | ((head[2] & 0xFF) << 8) | (head[3] & 0xFF);
            byte[] frame = new byte[4 + length];
            System.arraycopy(head, 0, frame, 0, 4);
            in.readFully(frame, 4, length);
            return frame;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private final class BlockedFactoryServer {
        final AppFixture fixture;
        final CountDownLatch factoryEntered = new CountDownLatch(1);
        final CountDownLatch releaseFactory = new CountDownLatch(1);
        final CountDownLatch handled = new CountDownLatch(1);
        final BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        final NioStrandServer server;
        final InetSocketAddress address;

        BlockedFactoryServer() throws Exception {
            fixture = AppFixture.plain();
            closeables.add(fixture);
            server = fixture.serverBuilder()
                    .authenticate(c -> AuthenticationResult.accepted("p"))
                    .handlerFactory(connection -> {
                        factoryEntered.countDown();
                        releaseFactory.await();
                        return PreparedHandler.of(request -> handled.countDown());
                    })
                    .onConnectionFailure(failures::add)
                    .build();
            Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
            closeables.add(listener::close);
            address = AppFixture.loopback(listener);
        }
    }

    @Test
    void aPingDuringPreparationIsAcknowledgedAndARequestBeforeTheHandlerIsHeldUntilItIsActivated() throws Exception {
        BlockedFactoryServer blocked = new BlockedFactoryServer();
        try (RawPeer peer = new RawPeer(blocked.address)) {
            peer.handshakeAndAuthenticate();
            assertThat(blocked.factoryEntered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            byte[] pingPayload = {1, 2, 3, 4, 5, 6, 7, 8};
            peer.write(frame(PING, 0, 0, pingPayload));
            byte[] ack = peer.readUntil(PING, ACK);
            peer.write(frame(0x0100, 0, 1, new byte[0]));

            assertThat(ack).isNotNull();
            assertThat(java.util.Arrays.copyOfRange(ack, 20, 28)).containsExactly(pingPayload);
            // 준비 전에 온 요청은 거절하지 않고 활성화까지 보관한다. PING과 요청은 준비를 끝내지 않는다.
            assertThat(blocked.handled.await(300, TimeUnit.MILLISECONDS)).isFalse();
            assertThat(blocked.failures.poll(300, TimeUnit.MILLISECONDS)).isNull();
            assertThat(blocked.server
                            .resourceUsage(Resource.PREPARING_CONNECTIONS)
                            .current())
                    .isEqualTo(1);
            blocked.releaseFactory.countDown();
            assertThat(blocked.handled.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
        }
    }

    @Test
    void aSecondAuthDuringPreparationIsAConnectionErrorThatFailsThePreparation() throws Exception {
        BlockedFactoryServer blocked = new BlockedFactoryServer();
        try (RawPeer peer = new RawPeer(blocked.address)) {
            peer.handshakeAndAuthenticate();
            assertThat(blocked.factoryEntered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            peer.write(frame(AUTH, 0, 0, new byte[] {1, 'X'}));
            byte[] close = peer.readUntil(CLOSE, 0);

            assertThat(close).isNotNull();
            assertThat(close[23] & 0xFF).isEqualTo(PROTOCOL_ERROR);
            PreparationFailure failure = blocked.failures.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(failure).isNotNull();
            assertThat(failure.stage()).isEqualTo(PreparationStage.HANDLER_FACTORY);
            awaitTrue(() ->
                    blocked.server.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
            // 팩터리 작업은 반환하지 않았으므로 슬롯을 유지한다.
            assertThat(blocked.server.resourceUsage(Resource.FACTORY_TASKS).current())
                    .isEqualTo(1);
            blocked.releaseFactory.countDown();
            awaitTrue(() -> blocked.server.resourceUsage(Resource.FACTORY_TASKS).current() == 0);
        }
    }

    @Test
    void aCloseDuringPreparationFailsThePreparationAtTheFactoryStage() throws Exception {
        BlockedFactoryServer blocked = new BlockedFactoryServer();
        try (RawPeer peer = new RawPeer(blocked.address)) {
            peer.handshakeAndAuthenticate();
            assertThat(blocked.factoryEntered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            payload.write(new byte[12]);
            peer.write(frame(CLOSE, 0, 0, payload.toByteArray()));

            PreparationFailure failure = blocked.failures.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(failure).isNotNull();
            assertThat(failure.stage()).isEqualTo(PreparationStage.HANDLER_FACTORY);
            assertThat(failure.cause()).isInstanceOf(ConnectionEndedException.class);
            awaitTrue(() ->
                    blocked.server.resourceUsage(Resource.PREPARING_CONNECTIONS).current() == 0);
            blocked.releaseFactory.countDown();
        }
    }
}
