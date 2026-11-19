package io.github.ppzxc.strand.netty;

import io.netty.channel.Channel;
import java.net.SocketAddress;

/**
 * 서버의 리스닝. {@link #close}는 새 연결 수락만 멈춘다. 이미 맺어 등록한 준비 중·활성 연결은 유지한다. 앱의 EventLoopGroup은 종료하지 않는다.
 */
public final class Listener {
    private final Channel channel;
    private final SocketAddress localAddress;

    Listener(Channel channel) {
        this.channel = channel;
        this.localAddress = channel.localAddress();
    }

    /**
     * 바인딩된 로컬 주소.
     *
     * @return 로컬 주소
     */
    public SocketAddress localAddress() {
        return localAddress;
    }

    /** 새 연결 수락을 멈추고 리스닝 소켓을 닫는다. 호출 스레드를 막지 않는다. 반복해도 추가 효과가 없다. */
    @SuppressWarnings("FutureReturnValueIgnored") // 닫기 완료의 관찰은 후속 티켓(listener.closed)이 맡는다.
    public void close() {
        channel.close();
    }
}
