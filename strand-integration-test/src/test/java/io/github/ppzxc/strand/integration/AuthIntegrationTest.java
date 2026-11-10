package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 규격 3.3절, 3.3.1절의 AUTH 관문을 실제 TCP 위에서 확인한다. 프레임의 바이트 형식은 컨포먼스 벡터가 맡는다. 여기서는 한쪽을 날것의 소켓으로 두고
 * {@code AUTH(ACK)}(서버 쪽이면 {@code AUTH})를 보내는 시점을 정해, 그 시점 전에는 애플리케이션 프레임이 상대에게 닿지 않고 그 뒤에야 닿는지만 본다.
 * 통합 테스트는 {@code ANONYMOUS}로 수립한다.
 */
class AuthIntegrationTest {

    private static final int HEADER_BYTES = 20;
    private static final int PREFACE_BYTES = 8;
    private static final int HELLO = 1;
    private static final int AUTH = 5;
    private static final int ACK = 1;
    /** 이 시간 안에 아무것도 오지 않으면 닿지 않은 것으로 본다. */
    private static final int SILENCE_MS = 300;

    private static final byte[] PREFACE = {(byte) 0xc0, (byte) 0xde, 0x53, 0x54, 0x52, 0x30, 0, 0};

    /**
     * 클라이언트는 요청을 AUTH 전에 냈어도 {@code AUTH(ACK)}를 받기 전에는 보내지 않는다. 날것의 서버가 클라이언트의 HELLO·ACK·AUTH까지 읽은 뒤
     * {@value #SILENCE_MS}ms 동안 아무것도 오지 않는 것을 확인하고, {@code AUTH(ACK)}를 보내면 그제서야 요청이 온다.
     */
    @Test
    @DisplayName("클라이언트의 요청은 AUTH(ACK)를 받은 뒤에야 서버에 도달한다 (규격 3.3)")
    void clientRequestReachesTheServerOnlyAfterAuthAck() throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            try (Peer client = Peer.connect(listener.getLocalPort(), null);
                    Socket raw = listener.accept()) {
                client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
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

                out.write(frame(AUTH, ACK, new byte[0]));
                raw.setSoTimeout((int) Fixtures.TIMEOUT_MS);
                byte[] request = readFrame(in);
                assertThat(typeOf(request)).isEqualTo(APPLICATION_TYPE);
                assertThat(ByteBuffer.wrap(request, 8, 8).getLong()).isEqualTo(1L);
                assertThat(client.awaitAuthenticated()).isEmpty();
            }
        }
    }

    /**
     * 서버는 자기가 먼저 내는 요청을 {@code AUTH(ACK)}를 보낸 뒤에야 보낸다. 날것의 클라이언트가 HELLO 교환을 마치고 AUTH를 보내기 전에는 서버가 요청을 냈어도
     * {@value #SILENCE_MS}ms 동안 아무것도 오지 않고, {@code ANONYMOUS} AUTH를 보내면 {@code AUTH(ACK)}에 이어 요청이 온다.
     */
    @Test
    @DisplayName("서버가 먼저 내는 요청은 AUTH를 통과시킨 뒤에야 클라이언트에 도달한다 (규격 3.3)")
    void serverInitiatedRequestReachesTheClientOnlyAfterAuth() throws Exception {
        try (TcpServer server = TcpServer.start();
                Socket raw = new Socket(InetAddress.getLoopbackAddress(), server.port())) {
            DataInputStream in = new DataInputStream(raw.getInputStream());
            OutputStream out = raw.getOutputStream();
            out.write(PREFACE);
            out.write(frame(HELLO, 0, new byte[0]));
            Peer serverSide = server.awaitPeer();
            serverSide.request(APPLICATION_TYPE, Fixtures.payload("from-server"));

            byte[] serverHello = readFrame(in);
            byte[] helloAck = readFrame(in);
            assertThat(typeOf(serverHello)).isEqualTo(HELLO);
            assertThat(typeOf(helloAck)).isEqualTo(HELLO);
            assertThat(flagsOf(helloAck)).isEqualTo(ACK);
            out.write(frame(HELLO, ACK, new byte[0]));

            raw.setSoTimeout(SILENCE_MS);
            assertThatThrownBy(() -> in.readByte()).isInstanceOf(SocketTimeoutException.class);

            out.write(frame(AUTH, 0, anonymousAuthPayload()));
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

    /** 실제 클라이언트와 서버가 AUTH까지 마쳐 수립하고, 클라이언트가 {@code AUTH(ACK)}를 받은 것을 알린다. */
    @Test
    @DisplayName("ANONYMOUS로 수립한 연결에서 클라이언트가 AUTH 통과를 알게 되고 요청이 오간다 (규격 3.3.1)")
    void anonymousEstablishmentCompletesBetweenRealEndpoints() throws Exception {
        try (TcpServer server = TcpServer.start(type -> Fixtures.STATUS_OK);
                Peer client = Peer.connect(server.port(), null)) {
            client.awaitAuthenticated();

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

    /** 길이 필드로 프레임 하나를 읽는다. */
    private static byte[] readFrame(DataInputStream in) throws IOException {
        byte[] length = readBytes(in, 4);
        byte[] rest = readBytes(in, ByteBuffer.wrap(length).getInt());
        byte[] frame = new byte[4 + rest.length];
        System.arraycopy(length, 0, frame, 0, 4);
        System.arraycopy(rest, 0, frame, 4, rest.length);
        return frame;
    }
}
