package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * 서버 역할 {@link Endpoint}를 블로킹 TCP 위에 올린다. 받은 요청마다 {@code OK}로 응답하고, 연결마다 스레드 하나를 쓴다. 클라이언트 예제
 * 두 개({@link BlockingTcpExample}, {@link NioTcpExample})의 상대로 쓴다. 사용법: {@code ServerExample [port]}
 */
public final class ServerExample {

    private ServerExample() {}

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(ExampleSupport.arg(args, 0, "9000"));
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("대기 중 " + server.getLocalPort());
            while (true) {
                Socket socket = server.accept();
                Thread thread = new Thread(() -> serve(socket), "strand-server-" + socket.getPort());
                thread.setDaemon(true);
                thread.start();
            }
        }
    }

    private static void serve(Socket socket) {
        Endpoint endpoint = Endpoint.server(EndpointConfig.defaults());
        try (Socket s = socket) {
            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();
            byte[] buffer = new byte[4096];
            boolean closed = false;
            while (!closed) {
                int n = in.read(buffer);
                if (n < 0) {
                    // CLOSE 없이 끊겼다.
                    endpoint.disconnect();
                } else {
                    byte[] chunk = new byte[n];
                    System.arraycopy(buffer, 0, chunk, 0, n);
                    endpoint.receive(chunk);
                }
                for (Event event : endpoint.drainEvents()) {
                    System.out.println("[서버] " + ExampleSupport.describe(event));
                    if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                        endpoint.respond(
                                event.streamId(), ExampleSupport.STATUS_OK, "ok".getBytes(StandardCharsets.UTF_8));
                    } else if (event.kind() == Event.Kind.CLOSED) {
                        closed = true;
                    }
                }
                // 응답과 ACK를 모아 보낸다. 닫는 중에도 마지막 CLOSE·ACK를 내보낸다.
                for (OutboundMessage message : endpoint.drainMessages()) {
                    if (message.kind() == OutboundMessage.Kind.BINARY) {
                        out.write(message.bytes());
                    }
                }
                out.flush();
                if (n < 0) {
                    closed = true;
                }
            }
        } catch (IOException e) {
            endpoint.disconnect();
            System.out.println("[서버] 연결 오류: " + e.getMessage());
        }
    }
}
