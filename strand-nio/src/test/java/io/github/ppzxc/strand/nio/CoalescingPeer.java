package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.github.ppzxc.strand.nio.internal.TestMessages;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 실제 TCP 위의 가짜 서버. 인증 응답과 서버 요청 프레임을 한 번의 쓰기(한 TCP 세그먼트)로 보낸다. 클라이언트는 둘을 같은 읽기에서 받으므로, 인증 응답이 낳는
 * 준비 성공 알림 거절 뒤에 같은 처리 단계에서 요청 수신 사건이 이어진다. 공개 API만으로는 이 겹침을 결정적으로 만들 수 없다.
 */
final class CoalescingPeer implements AutoCloseable {

    private final ServerSocket listener;
    private final Thread thread;
    private volatile Socket socket;

    CoalescingPeer(int requestType) throws IOException {
        listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        thread = new Thread(() -> serve(requestType), "coalescing-peer");
        thread.setDaemon(true);
        thread.start();
    }

    InetSocketAddress address() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort());
    }

    private void serve(int requestType) {
        try (Socket accepted = listener.accept()) {
            socket = accepted;
            Endpoint endpoint = Endpoint.server(EndpointConfig.defaults());
            // 인증 관문 전에 낸 요청은 미뤄졌다가 인증 응답 바로 뒤에 쌓인다.
            endpoint.request(requestType, new byte[0]);
            InputStream in = accepted.getInputStream();
            OutputStream out = accepted.getOutputStream();
            byte[] buffer = new byte[4096];
            int count;
            while ((count = in.read(buffer)) >= 0) {
                byte[] received = new byte[count];
                System.arraycopy(buffer, 0, received, 0, count);
                TestMessages.deliver(endpoint, received);
                TestMessages.drainEvents(endpoint);
                ByteArrayOutputStream burst = new ByteArrayOutputStream();
                for (EndpointOutput output = endpoint.nextOutput(); output != null; output = endpoint.nextOutput()) {
                    burst.write(output.bytes());
                    endpoint.outputProgress(output.id(), output.length());
                }
                if (burst.size() > 0) {
                    out.write(burst.toByteArray());
                    out.flush();
                }
            }
        } catch (IOException ignored) {
            // 시험이 끝나 소켓이 닫혔다.
        }
    }

    @Override
    public void close() throws IOException {
        listener.close();
        Socket current = socket;
        if (current != null) {
            current.close();
        }
    }
}
