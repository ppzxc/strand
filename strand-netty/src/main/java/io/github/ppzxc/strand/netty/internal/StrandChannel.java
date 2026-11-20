package io.github.ppzxc.strand.netty.internal;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.jspecify.annotations.Nullable;

/**
 * Strand 연결 하나의 Netty 채널. 지원 조합({@code docs/research/netty-progress-verification.md})은 이 채널과 출력 handler가 없는 pipeline이다.
 * 출력은 {@code write}/{@code flush}와 {@code Future}를 거치지 않고 {@link #writeAttempt}로 소켓에 직접 쓴다. 그래서 실제로 소켓이 받아 준 바이트 수가
 * core 진행 보고의 근거다. 모든 메서드는 이 채널의 이벤트 루프에서만 부른다.
 */
public class StrandChannel extends NioSocketChannel {
    private volatile @Nullable Selector selector;

    /** 클라이언트 쪽: 아직 연결하지 않은 채널. */
    public StrandChannel() {
        super();
    }

    /** 서버 쪽: 수락한 소켓. */
    public StrandChannel(Channel parent, SocketChannel socket) {
        super(parent, socket);
    }

    /**
     * 비차단 소켓 쓰기를 한 번 시도한다.
     *
     * @return 소켓이 받아 준 바이트 수(0이면 받아 주지 않은 것)
     * @throws IOException 소켓 오류
     */
    public int writeAttempt(ByteBuffer bytes) throws IOException {
        return javaChannel().write(bytes);
    }

    /** 보내는 쪽만 닫는다(TCP FIN). 상대가 끊을 때까지 읽는 것은 그대로다. */
    public void shutdownOutputNow() throws IOException {
        javaChannel().shutdownOutput();
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
