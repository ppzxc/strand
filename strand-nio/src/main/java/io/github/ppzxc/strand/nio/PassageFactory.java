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

/** 맺어진 소켓을 채널 보안 설정에 맞는 통로로 만든다. 실패하면 소켓을 닫는다. */
final class PassageFactory {
    private PassageFactory() {}

    /**
     * @param channel 맺어진 소켓. 통로가 소유한다
     * @param context TLS 컨텍스트. TLS를 쓰지 않으면 null
     * @param clientHost 클라이언트일 때 서버 호스트 이름(인증서 검증 대상). 서버면 null
     * @param clientPort 클라이언트일 때 서버 포트
     */
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
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }
}
