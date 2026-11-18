package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;

/**
 * 연결 드라이버가 바이트를 주고받는 통로(TCP, in-memory, WebSocket 등). 바이트 스트림 통로는 {@link Inbound#bytes}를, 메시지 통로는
 * {@link Inbound#message}를 내며, 드라이버는 그 종류에 따라 {@code Endpoint}의 {@code receive}와 {@code receiveMessage} 가운데 하나를
 * 부른다.
 *
 * <p>{@code await}와 {@link #write}는 전송 스레드 하나만 부른다. {@link #wakeup}과 {@link #close}는 어느 스레드에서든 부른다.
 */
public interface Passage {

    /**
     * 읽을 것이 생기거나 {@link #wakeup}이 불리거나 시간이 다 될 때까지 기다린다. 읽기만 기다리는 {@code await(timeoutMs, true, false)}와 같다.
     *
     * @param timeoutMs 기다리는 상한(밀리초). 0은 기다리지 않고 폴, 음수는 상한 없음, 양수는 그 밀리초까지
     * @return 읽은 것. 읽을 것이 없이 깨어났으면 {@link Inbound#none()}
     * @throws IOException 입출력 오류. 드라이버는 연결이 끊긴 것으로 본다
     */
    default Inbound await(long timeoutMs) throws IOException {
        return await(timeoutMs, true, false);
    }

    /**
     * 기다린다. {@code waitForRead}면 읽을 것이 생길 때, {@code waitForWrite}면 쓰기가 다시 받아 줄 수 있을 때, 그리고 언제나 {@link #wakeup}이
     * 불리거나 시간이 다 될 때 끝난다. 기다리기 전에 불린 {@code wakeup}도 다음 기다림을 바로 끝낸다({@code Selector.wakeup()}과 같다). 쓰기를
     * 기다릴 때 이미 받아 줄 수 있으면 바로 끝난다.
     *
     * <p>시간 제한의 계약이다. {@code timeoutMs}가 0이면 기다리지 않고 지금 읽을 수 있는 것만 돌려준다(논블로킹 폴, 읽을 것이 없으면 바로
     * {@link Inbound#none()}). 음수면 기다리는 대상이 오거나 {@code wakeup}이 올 때까지 상한 없이 기다린다. 양수면 그 밀리초까지만 기다린다. 드라이버는 양수 값을 {@code Integer.MAX_VALUE} 이하로 잘라 부른다.
     * 구현은 {@code PassageContract}(test fixtures)를 상속한 테스트로 이를 확인한다.
     *
     * <p>{@code waitForRead}가 false면 이 호출은 읽지 않는다. 읽을 것이 있어도 깨어나지 않고 입력을 가져가지도 않으며, 늘 {@link Inbound#none()}을
     * 돌려준다(상대의 끊김도 알리지 않는다). 다만 통로가 닫혔거나 쓰기가 실패한 것 같은 입출력 오류는 읽기를 끈 대기에서도 {@link IOException}으로 알릴 수 있다. 읽기를 멈춘 동안 빈 깨어남이 되풀이되지 않게 하려는 것이다. 읽기를 다시 켜면 그때까지 쌓인 입력을
     * 돌려준다. 쓰기 가능으로 깨어났을 때도 읽을 것이 없으면 {@link Inbound#none()}이다.
     *
     * @param timeoutMs 기다리는 상한(밀리초). 0은 기다리지 않고 폴, 음수는 상한 없음, 양수는 그 밀리초까지
     * @param waitForRead 읽을 것이 생기기를 기다리고 읽은 것을 돌려줄지
     * @param waitForWrite 쓰기가 받아 줄 수 있게 되기를 기다릴지
     * @return 읽은 것. 읽을 것이 없이 깨어났으면 {@link Inbound#none()}
     * @throws IOException 입출력 오류. 드라이버는 연결이 끊긴 것으로 본다
     */
    Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException;

    /**
     * 메시지의 {@code offset}부터 지금 받을 수 있는 만큼만 상대에게 보낸다. 기다리지 않는다(비블로킹). 통로가 받아 줄 수 없으면 0을 돌려주고,
     * 받아 줄 수 있게 되기를 {@link #await(long, boolean, boolean)}의 쓰기 대기로 기다린다. 부분 쓰기라서 한 메시지를 여러 번에 나눠 쓸 수
     * 있다. 호출자는 받아 준 만큼 {@code offset}을 올려 이어 쓰고, 메시지 하나를 다 쓰기 전에는 다른 메시지를 쓰지 않는다.
     *
     * <p>내용이 없는 메시지(TCP에 해당하는 것이 없는 WebSocket 닫기 등)는 {@code offset}이 0인 한 번의 호출로 끝나고 0을 돌려준다. 메시지
     * 통로는 메시지를 다 받아야 상대에게 메시지 하나로 보인다.
     *
     * @param message 보낼 메시지
     * @param offset 이미 받아 준 바이트 수(0 이상, 메시지 길이 이하)
     * @return 이번에 받아 준 바이트 수(0 이상, 남은 길이 이하)
     * @throws IOException 입출력 오류. 드라이버는 연결이 끊긴 것으로 본다
     */
    int write(OutboundMessage message, int offset) throws IOException;

    /**
     * 바이트 배열의 {@code offset}부터 지금 받을 수 있는 만큼만 상대에게 보낸다. 기다리지 않는다(비블로킹). 진행 보고 방식의 쓰기다. 돌려준 값이 보고하는
     * 소비량이다. TCP는 실제 소켓 쓰기량이고, TLS는 엔진이 실제로 소비한 평문 바이트 수다. 0은 쓰기를 시작한 것이 아니다. 호출자는 0이면 쓰기
     * 가능을 기다렸다가 다시 부른다.
     *
     * @param bytes 보낼 바이트를 든 배열(통로는 바꾸지 않는다)
     * @param offset 보낼 바이트가 시작하는 위치
     * @param length 보낼 바이트 수(0 이상)
     * @return 이번에 소비한 바이트 수(0 이상, {@code length} 이하)
     * @throws IOException 입출력 오류. 드라이버는 연결이 끊긴 것으로 본다
     * @throws UnsupportedOperationException 이 통로가 진행 보고 방식 쓰기를 지원하지 않을 때
     */
    default int writeBytes(byte[] bytes, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("진행 보고 방식 쓰기를 지원하지 않는 통로");
    }

    /**
     * 소비한 평문이 아직 상대에게 다 나가지 않고 통로 안에 남아 있는가. TLS에서 평문은 소비했지만 암호문이 덜 나간 상태다. 이 상태에서는 보내는 쪽을 닫기
     * 전에 쓰기 가능을 기다려 비워야 한다.
     *
     * @return 남은 출력이 있으면 true
     */
    default boolean pendingOutput() {
        return false;
    }

    /** 기다리고 있거나 곧 기다릴 {@link #await}를 깨운다. 어느 스레드에서든 부를 수 있다. */
    void wakeup();

    /**
     * 보내는 쪽만 닫는다. 상대는 끊김(EOF)을 보지만 이쪽은 상대가 끊을 때까지 {@link #await}로 계속 읽을 수 있다(TCP의 {@code
     * shutdownOutput}과 같다). 드라이버는 연결이 끝난 뒤(CLOSED), 쓸 것을 다 쓰고 나서 한 번 부른다. 기본 구현은 통로를 통째로 닫는다.
     */
    default void shutdownOutput() {
        close();
    }

    /** 통로를 닫는다. 상대는 끊김을 본다. 어느 스레드에서든 부를 수 있고 여러 번 불러도 된다. */
    void close();
}
