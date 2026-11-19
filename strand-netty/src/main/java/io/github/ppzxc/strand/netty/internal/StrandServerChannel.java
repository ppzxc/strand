package io.github.ppzxc.strand.netty.internal;

import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
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
     * 닫힌 채널의 취소된 선택 키를 지금 비운다. JDK는 선택기에 등록돼 있던 채널의 파일 기술자를 선택기가 취소된 키를 처리할 때 놓는다. 그래서 채널 닫기와 등록 해제가 끝나도
     * 다음 선택 주기가 돌기 전에는 리스닝 포트가 풀리지 않는다. 이 채널의 EventLoop에서 등록 해제 뒤에 부른다.
     */
    public void releaseClosedSocket() {
        try {
            selectionKey().selector().selectNow();
        } catch (IOException | ClosedSelectorException e) {
            // 선택기가 이미 닫혔다면 선택기가 등록된 채널을 모두 놓았다.
        }
    }
}
