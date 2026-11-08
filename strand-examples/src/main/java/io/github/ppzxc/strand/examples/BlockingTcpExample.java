package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * 블로킹 {@link Socket} 위에서 클라이언트 {@link Endpoint}를 돌린다. 연결 서문과 HELLO를 교환하고, 요청 하나와 PING 하나를 보내
 * 결과를 받은 뒤 CLOSE로 끝낸다. 사용법: {@code BlockingTcpExample [host] [port]} (상대는 {@link ServerExample})
 */
public final class BlockingTcpExample {

    private static final int READ_TIMEOUT_MS = 5000;

    private BlockingTcpExample() {}

    public static void main(String[] args) throws IOException {
        String host = ExampleSupport.arg(args, 0, "localhost");
        int port = Integer.parseInt(ExampleSupport.arg(args, 1, "9000"));
        System.out.println("strand core " + StrandVersion.get());
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(READ_TIMEOUT_MS);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            Endpoint endpoint = Endpoint.client(ExampleSupport.withAuth(EndpointConfig.defaults()));
            // 서버 HELLO를 ACK하기 전의 요청은 core가 미뤘다가 보낸다.
            endpoint.request(ExampleSupport.DEMO_REQUEST_TYPE, "hello".getBytes(StandardCharsets.UTF_8));
            endpoint.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
            flush(endpoint, out);

            byte[] buffer = new byte[4096];
            boolean resultReceived = false;
            boolean closed = false;
            while (!closed) {
                int n = in.read(buffer);
                if (n < 0) {
                    endpoint.disconnect();
                } else {
                    byte[] chunk = new byte[n];
                    System.arraycopy(buffer, 0, chunk, 0, n);
                    endpoint.receive(chunk);
                }
                for (Event event : endpoint.drainEvents()) {
                    System.out.println("[클라이언트] " + ExampleSupport.describe(event));
                    if (event.kind() == Event.Kind.RESULT) {
                        ExampleSupport.handleResult(event);
                        resultReceived = true;
                    }
                    closed |= event.kind() == Event.Kind.CLOSED;
                }
                if (resultReceived && !closed) {
                    endpoint.close(ExampleSupport.NO_ERROR);
                }
                flush(endpoint, out);
            }
        }
    }

    /** core가 쌓은 메시지를 순서대로 소켓에 쓴다. 바이트 스트림이므로 이어 붙이면 된다. */
    private static void flush(Endpoint endpoint, OutputStream out) throws IOException {
        for (OutboundMessage message : endpoint.drainMessages()) {
            if (message.kind() == OutboundMessage.Kind.BINARY) {
                out.write(message.bytes());
            }
        }
        out.flush();
    }
}
