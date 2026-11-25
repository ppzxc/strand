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

    void pauseClientToServer() {
        paused = true;
    }

    private enum Direction {
        CLIENT_TO_SERVER("relay-up"),
        SERVER_TO_CLIENT("relay-down");

        final String threadName;

        Direction(String threadName) {
            this.threadName = threadName;
        }
    }

    private void acceptLoop() {
        try {
            while (!server.isClosed()) {
                Socket client = server.accept();
                Socket upstream = new Socket(target.getAddress(), target.getPort());
                sockets.add(client);
                sockets.add(upstream);
                pump(Direction.CLIENT_TO_SERVER, client, upstream);
                pump(Direction.SERVER_TO_CLIENT, upstream, client);
            }
        } catch (IOException closed) {

        }
    }

    private void pump(Direction direction, Socket from, Socket to) {
        Thread thread = new Thread(
                () -> {
                    byte[] buffer = new byte[2048];
                    try {
                        InputStream in = from.getInputStream();
                        OutputStream out = to.getOutputStream();
                        while (true) {
                            while (direction == Direction.CLIENT_TO_SERVER && paused) {
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

                    } finally {
                        closeQuietly(from);
                        closeQuietly(to);
                    }
                },
                direction.threadName);
        thread.setDaemon(true);
        thread.start();
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {

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
