package io.github.ppzxc.strand.netty.internal;

import java.io.IOException;
import org.jspecify.annotations.Nullable;

/**
 * 메시지 단위 통로(WebSocket)와 연결 엔진 사이의 변환. 엔진은 평문 바이트(TCP 소켓에서 읽은 것 또는 TLS가 푼 것)를 {@link #feed}로 넣고, {@link
 * #pollWire}로 내보낼 평문 바이트를 가져간다. 통로를 여는 절차(업그레이드)와 프레임 변환, 조각 모으기, Close 교환이 이 안에 있다. 이벤트 루프에서만 부른다.
 *
 * <p>이 인터페이스는 Netty의 HTTP·WebSocket 코덱 타입을 노출하지 않는다. 코덱이 없는 클래스패스에서 TCP가 동작하려면 엔진이 이 타입만 알아야 한다.
 */
public interface MessageWire {
    /** 클라이언트: 업그레이드 요청을 {@link #pollWire}로 나갈 바이트에 올린다. 서버는 아무 일도 하지 않는다. */
    void start() throws IOException;

    /**
     * 상대에게서 읽은 평문 바이트를 넣는다. 이어서 {@link #pollMessage}로 입력을, {@link #pollWire}로 응답 바이트를 가져간다.
     *
     * @throws WireUpgradeException 업그레이드가 실패했다. 상대에게 보낼 거절 응답이 있으면 이미 {@link #pollWire}에 올라 있다
     */
    void feed(byte[] bytes) throws IOException;

    /** 업그레이드를 마쳐 메시지를 주고받을 수 있는가. */
    boolean established();

    /** 가져가지 않은 입력이 있는가. */
    boolean hasMessages();

    /** 다음 입력. 없으면 null. */
    @Nullable
    WireMessage pollMessage();

    /** 내보낼 바이트. 한 번에 가져가며 없으면 null이다. 가져간 바이트는 호출자가 보낼 책임이 있다. */
    byte @Nullable [] pollWire();

    /** 내보낼 바이트가 남아 있는가. */
    boolean wirePending();

    /** 바이너리 메시지 하나를 보낸다. 업그레이드를 마친 뒤에만 부른다. */
    void sendMessage(byte[] bytes) throws IOException;

    /**
     * Close 프레임을 보낸다(사유 문자열은 늘 비어 있다). 이미 보냈거나 보낼 수 없으면 아무 일도 하지 않는다.
     *
     * @param status WebSocket 상태 코드
     */
    void sendClose(int status);

    /** 상대의 Close에 이미 답했거나 이쪽 Close를 이미 보냈는가. */
    boolean closeSent();

    /** 내부 버퍼를 모두 놓는다. 여러 번 불러도 된다. */
    void release();
}
