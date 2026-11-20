package io.github.ppzxc.strand.netty.internal;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Strand 연결 하나의 Netty 채널. 지원 조합({@code docs/research/netty-progress-verification.md})은 이 채널과 출력 handler가 없는 pipeline이다.
 * 출력은 {@code write}/{@code flush}와 {@code Future}를 거치지 않고 {@link #writeAttempt}로 소켓에 직접 쓴다. 그래서 실제로 소켓이 받아 준 바이트 수가
 * core 진행 보고의 근거다. 모든 메서드는 이 채널의 이벤트 루프에서만 부른다.
 */
public class StrandChannel extends NioSocketChannel {
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

    /**
     * 닫은 채널의 소켓을 JDK가 아직 선택기에 붙들고 있는가. {@code closeFuture}가 끝나도 선택기가 취소된 키를 처리하는 다음 선택 주기가 돌기 전에는 소켓이 실제로 풀리지 않는다.
     * 선택 키(Netty 내부, 4.2는 등록 해제 뒤 내주지 않는다)가 아니라 JDK 채널의 상태로 본다. 선택기에 등록한 적 없으면 false다.
     */
    public boolean socketHeld() {
        return javaChannel().isRegistered();
    }
}
