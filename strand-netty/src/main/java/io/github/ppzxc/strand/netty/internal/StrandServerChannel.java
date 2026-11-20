package io.github.ppzxc.strand.netty.internal;

import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

/** 수락한 소켓을 {@link StrandChannel}로 만드는 리스닝 채널. */
public final class StrandServerChannel extends NioServerSocketChannel {
    @Override
    protected int doReadMessages(List<Object> buf) throws Exception {
        SocketChannel accepted = javaChannel().accept();
        if (accepted == null) {
            return 0;
        }
        try {
            buf.add(new StrandChannel(this, accepted));
            return 1;
        } catch (RuntimeException e) {
            accepted.close();
            throw e;
        }
    }

    /**
     * 닫은 채널의 소켓을 JDK가 아직 선택기에 붙들고 있는가. JDK는 선택기에 등록돼 있던 채널의 파일 기술자를 선택기가 취소된 키를 처리할 때 놓으므로 닫기와 등록 해제가 끝나도 다음 선택 주기가
     * 돌기 전에는 리스닝 포트가 풀리지 않는다. 선택 키(Netty 내부, 4.2는 등록 해제 뒤 내주지 않는다)가 아니라 JDK 채널의 상태로 본다.
     */
    public boolean socketHeld() {
        return javaChannel().isRegistered();
    }
}
