package io.github.ppzxc.strand.netty.internal;

import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.jspecify.annotations.Nullable;

/** 수락한 소켓을 {@link StrandChannel}로 만드는 리스닝 채널. */
public final class StrandServerChannel extends NioServerSocketChannel {
    private volatile @Nullable Selector selector;

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

    /** 선택기에 등록된 동안 그 선택기를 기억한다. 등록 해제 뒤에는 Netty가 선택 키를 내주지 않는다(4.2). 이 채널의 EventLoop에서 등록 사건 안에서 부른다. */
    public void captureSelector() {
        selector = selectionKey().selector();
    }

    /**
     * 닫은 채널의 취소된 선택 키를 지금 비운다. JDK는 선택기에 등록돼 있던 채널의 파일 기술자를 선택기가 취소된 키를 처리할 때 놓는다. 그래서 {@code closeFuture}나 등록
     * 해제 사건이 끝나도 다음 선택 주기가 돌기 전에는 소켓이 실제로 풀리지 않는다. 이 채널의 EventLoop에서 닫은 뒤에 부른다. 선택기에 등록한 적 없으면 할 일이 없다.
     */
    public void releaseClosedSocket() {
        Selector registered = selector;
        if (registered == null) {
            return;
        }
        try {
            registered.selectNow();
        } catch (IOException | ClosedSelectorException e) {
            // 선택기가 이미 닫혔다면 선택기가 등록된 채널을 모두 놓았다.
        }
    }
}
