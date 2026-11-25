package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.RequestResult;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;

class AuthIntegrationTest {

    private static final int HEADER_BYTES = 20;
    private static final int PREFACE_BYTES = 8;
    private static final int HELLO = 1;
    private static final int AUTH = 5;
    private static final int ACK = 1;

    private static final int SILENCE_MS = 300;

    private static final byte[] PREFACE = {(byte) 0xc0, (byte) 0xde, 0x53, 0x54, 0x52, 0x30, 0, 0};

    @TransportTest
    @DisplayName("클라이언트의 요청은 AUTH(ACK)를 받은 뒤에야 서버에 도달한다 (규격 3.3)")
    void clientRequestReachesTheServerOnlyAfterAuthAck(Transport transport) throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Peer> connecting = Peer.connectLater(transport, listener.getLocalPort(), null);
            try (Socket raw = listener.accept()) {
                DataInputStream in = new DataInputStream(raw.getInputStream());
                OutputStream out = raw.getOutputStream();

                assertThat(readBytes(in, PREFACE_BYTES)).isEqualTo(PREFACE);
                assertThat(typeOf(readFrame(in))).isEqualTo(HELLO);
                out.write(frame(HELLO, 0, new byte[0]));
                out.write(frame(HELLO, ACK, new byte[0]));
                byte[] helloAck = readFrame(in);
                byte[] auth = readFrame(in);
                assertThat(typeOf(helloAck)).isEqualTo(HELLO);
                assertThat(flagsOf(helloAck)).isEqualTo(ACK);
                assertThat(typeOf(auth)).isEqualTo(AUTH);
                assertThat(new String(auth, HEADER_BYTES + 1, 9, StandardCharsets.US_ASCII))
                        .isEqualTo("ANONYMOUS");

                raw.setSoTimeout(SILENCE_MS);
                assertThatThrownBy(() -> in.readByte()).isInstanceOf(SocketTimeoutException.class);
                assertThat(connecting).isNotDone();

                out.write(frame(AUTH, ACK, new byte[0]));
                try (Peer client = connecting.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
                    raw.setSoTimeout((int) Fixtures.TIMEOUT_MS);
                    byte[] request = readFrame(in);
                    assertThat(typeOf(request)).isEqualTo(APPLICATION_TYPE);
                    assertThat(ByteBuffer.wrap(request, 8, 8).getLong()).isEqualTo(1L);
                    assertThat(client.authenticated()).isEmpty();
                }
            }
        }
    }

    @TransportTest
    @DisplayName("서버가 먼저 내는 요청은 AUTH를 통과시킨 뒤에야 클라이언트에 도달한다 (규격 3.3)")
    void serverInitiatedRequestReachesTheClientOnlyAfterAuth(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport);
                Socket raw = new Socket(InetAddress.getLoopbackAddress(), server.port())) {
            DataInputStream in = new DataInputStream(raw.getInputStream());
            OutputStream out = raw.getOutputStream();
            out.write(PREFACE);
            out.write(frame(HELLO, 0, new byte[0]));

            byte[] serverHello = readFrame(in);
            byte[] helloAck = readFrame(in);
            assertThat(typeOf(serverHello)).isEqualTo(HELLO);
            assertThat(typeOf(helloAck)).isEqualTo(HELLO);
            assertThat(flagsOf(helloAck)).isEqualTo(ACK);
            out.write(frame(HELLO, ACK, new byte[0]));

            raw.setSoTimeout(SILENCE_MS);
            assertThatThrownBy(() -> in.readByte()).isInstanceOf(SocketTimeoutException.class);
            assertThat(server.pollPeer()).isNull();

            out.write(frame(AUTH, 0, anonymousAuthPayload()));
            Peer serverSide = server.awaitPeer();
            serverSide.request(APPLICATION_TYPE, Fixtures.payload("from-server"));
            raw.setSoTimeout((int) Fixtures.TIMEOUT_MS);
            byte[] authAck = readFrame(in);
            byte[] request = readFrame(in);
            assertThat(typeOf(authAck)).isEqualTo(AUTH);
            assertThat(flagsOf(authAck)).isEqualTo(ACK);
            assertThat(typeOf(request)).isEqualTo(APPLICATION_TYPE);
            assertThat(ByteBuffer.wrap(request, 8, 8).getLong()).isEqualTo(2L);
            assertThat(serverSide.closedNotices()).isEmpty();
        }
    }

    @TransportTest
    @DisplayName("ANONYMOUS로 수립한 연결에서 클라이언트가 AUTH 통과를 알게 되고 요청이 오간다 (규격 3.3.1)")
    void anonymousEstablishmentCompletesBetweenRealEndpoints(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport, type -> Fixtures.STATUS_OK);
                Peer client = Peer.connect(transport, server.port(), null)) {
            client.authenticated();

            RequestResult result = client.request(APPLICATION_TYPE, Fixtures.payload("hello"))
                    .result()
                    .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertThat(result.status()).isEqualTo(Fixtures.STATUS_OK);
        }
    }

    private static byte[] anonymousAuthPayload() {
        byte[] name = "ANONYMOUS".getBytes(StandardCharsets.US_ASCII);
        byte[] payload = new byte[1 + name.length];
        payload[0] = (byte) name.length;
        System.arraycopy(name, 0, payload, 1, name.length);
        return payload;
    }

    private static byte[] frame(int type, int flags, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES + payload.length);
        buffer.putInt(HEADER_BYTES - 4 + payload.length);
        buffer.putShort((short) type);
        buffer.put((byte) flags);
        buffer.put((byte) 0);
        buffer.putLong(0L);
        buffer.putShort((short) 0);
        buffer.putShort((short) 0);
        buffer.put(payload);
        return buffer.array();
    }

    private static int typeOf(byte[] frame) {
        return ((frame[4] & 0xFF) << 8) | (frame[5] & 0xFF);
    }

    private static int flagsOf(byte[] frame) {
        return frame[6] & 0xFF;
    }

    private static byte[] readBytes(DataInputStream in, int count) throws IOException {
        byte[] bytes = new byte[count];
        in.readFully(bytes);
        return bytes;
    }

    private static byte[] readFrame(DataInputStream in) throws IOException {
        byte[] length = readBytes(in, 4);
        byte[] rest = readBytes(in, ByteBuffer.wrap(length).getInt());
        byte[] frame = new byte[4 + rest.length];
        System.arraycopy(length, 0, frame, 0, 4);
        System.arraycopy(rest, 0, frame, 4, rest.length);
        return frame;
    }
}
