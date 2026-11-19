package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.Nullable;

/** 클라이언트 연결 스레드가 서버 주소로 TCP 연결을 맺고 채널 보안 설정에 맞는 통로를 여는 동작. */
final class TcpConnector implements ConnectionParams.PassageOpener {
    private final ConnectionBudgets budgets;
    private final InetSocketAddress address;
    private final @Nullable SSLContext tlsContext;

    TcpConnector(ConnectionBudgets budgets, InetSocketAddress address, @Nullable SSLContext tlsContext) {
        this.budgets = budgets;
        this.address = address;
        this.tlsContext = tlsContext;
    }

    @Override
    public Passage open(long remainingNanos) throws IOException {
        InetSocketAddress target = resolve(budgets, address);
        long millis = remainingNanos / 1_000_000L + 1;
        if (remainingNanos <= 0) {
            throw new SocketTimeoutException("준비 기한 안에 연결하지 못했다");
        }
        SocketChannel channel = SocketChannel.open();
        try {
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.socket().connect(target, (int) Math.min(millis, Integer.MAX_VALUE));
        } catch (IOException | RuntimeException e) {
            PassageFactory.closeQuietly(channel);
            throw e;
        }
        return PassageFactory.open(channel, tlsContext, target.getHostString(), target.getPort());
    }

    /** 해석하지 않은 주소는 주소 해석 작업 슬롯을 예약해 해석한다. */
    private static InetSocketAddress resolve(ConnectionBudgets budgets, InetSocketAddress address) throws IOException {
        if (!address.isUnresolved()) {
            return address;
        }
        if (!budgets.owner().tryReserve(Resource.RESOLVE_TASKS, 1)) {
            throw new IOException(new ResourceExhaustedException(Resource.RESOLVE_TASKS));
        }
        try {
            InetSocketAddress resolved = new InetSocketAddress(address.getHostString(), address.getPort());
            if (resolved.isUnresolved()) {
                throw new UnknownHostException(address.getHostString());
            }
            return resolved;
        } finally {
            budgets.owner().release(Resource.RESOLVE_TASKS, 1);
        }
    }
}
