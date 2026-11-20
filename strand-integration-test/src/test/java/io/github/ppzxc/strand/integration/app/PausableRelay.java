package io.github.ppzxc.strand.integration.app;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 암호문 바이트를 그대로 전달하다가 클라이언트 쪽 읽기를 멈출 수 있는 TCP 중계. 상대가 읽기를 멈춘 상황을 만든다. TLS를 풀지 않으므로 양 끝은 실제 TLS를 한다.
 * 수신 버퍼를 작게 잡아 멈춘 동안 커널이 삼키는 양을 줄인다.
 */
final class PausableRelay implements AutoCloseable {
    private final ServerSocket server;
    private final InetSocketAddress target;
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private volatile boolean paused;

    PausableRelay(InetSocketAddress target) throws IOException {
        this.target = target;
        this.server = new ServerSocket();
        server.setReceiveBufferSize(4096);
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        Thread acceptor = new Thread(this::acceptLoop, "relay-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    InetSocketAddress address() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort());
    }

    /** 클라이언트에서 서버로 가는 바이트 읽기를 멈춘다. */
    void pauseClientToServer() {
        paused = true;
    }

    private void acceptLoop() {
        try {
            while (!server.isClosed()) {
                Socket client = server.accept();
                Socket upstream = new Socket(target.getAddress(), target.getPort());
                sockets.add(client);
                sockets.add(upstream);
                pump(client, upstream, true);
                pump(upstream, client, false);
            }
        } catch (IOException closed) {
            // 닫힘
        }
    }

    private void pump(Socket from, Socket to, boolean gated) {
        Thread thread = new Thread(
                () -> {
                    byte[] buffer = new byte[2048];
                    try {
                        InputStream in = from.getInputStream();
                        OutputStream out = to.getOutputStream();
                        while (true) {
                            while (gated && paused) {
                                Thread.sleep(5);
                            }
                            int read = in.read(buffer);
                            if (read < 0) {
                                break;
                            }
                            out.write(buffer, 0, read);
                            out.flush();
                        }
                    } catch (IOException | InterruptedException ended) {
                        // 끝
                    } finally {
                        closeQuietly(from);
                        closeQuietly(to);
                    }
                },
                gated ? "relay-up" : "relay-down");
        thread.setDaemon(true);
        thread.start();
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // 무시
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
        for (Socket socket : sockets) {
            closeQuietly(socket);
        }
    }
}
