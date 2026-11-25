package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import io.github.ppzxc.strand.nio.internal.TlsEngines;
import io.github.ppzxc.strand.nio.internal.TlsPassage;
import io.github.ppzxc.strand.nio.internal.TlsSizes;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.jspecify.annotations.Nullable;

@SuppressWarnings("EmptyCatch")
final class PassageFactory {
    private PassageFactory() {}

    static Passage open(
            SocketChannel channel, @Nullable SSLContext context, @Nullable String clientHost, int clientPort)
            throws IOException {
        TcpPassage tcp = null;
        try {
            if (context == null) {
                tcp = TcpPassage.of(channel);
                return tcp;
            }
            TlsSizes sizes = TlsSizes.of(context);
            tcp = TcpPassage.of(channel, sizes.packetBufferSize());
            SSLEngine engine = clientHost == null
                    ? TlsEngines.server(context)
                    : TlsEngines.client(context, clientHost, clientPort);
            return new TlsPassage(tcp, engine, sizes);
        } catch (IOException | RuntimeException e) {
            if (tcp != null) {
                tcp.close();
            } else {
                closeQuietly(channel);
            }
            throw e;
        }
    }

    static void closeQuietly(SocketChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }
}
