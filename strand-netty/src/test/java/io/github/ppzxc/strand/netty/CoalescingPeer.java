package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

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
            endpoint.request(requestType, new byte[0]);
            InputStream in = accepted.getInputStream();
            OutputStream out = accepted.getOutputStream();
            byte[] buffer = new byte[4096];
            int count;
            while ((count = in.read(buffer)) >= 0) {
                byte[] received = new byte[count];
                System.arraycopy(buffer, 0, received, 0, count);
                EndpointFeed.receive(endpoint, received);
                EndpointFeed.drain(endpoint);
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
