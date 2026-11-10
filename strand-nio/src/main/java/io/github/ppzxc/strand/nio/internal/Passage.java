package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;

/**
 * 연결 드라이버가 바이트를 주고받는 통로(TCP, in-memory, WebSocket 등). 바이트 스트림 통로는 {@link Inbound#bytes}를, 메시지 통로는
 * {@link Inbound#message}를 내며, 드라이버는 그 종류에 따라 {@code Endpoint}의 {@code receive}와 {@code receiveMessage} 가운데 하나를
 * 부른다.
 *
 * <p>{@link #await}와 {@link #write}는 전송 스레드 하나만 부른다. {@link #wakeup}과 {@link #close}는 어느 스레드에서든 부른다.
 */
public interface Passage {

    /**
     * 읽을 것이 생기거나 {@link #wakeup}이 불리거나 시간이 다 될 때까지 기다린다. 기다리기 전에 불린 {@code wakeup}도 다음 기다림을 바로 끝낸다
     * ({@code Selector.wakeup()}과 같다).
     *
     * <p>시간 제한의 계약이다. {@code timeoutMs}가 0이면 기다리지 않고 지금 읽을 수 있는 것만 돌려준다(논블로킹 폴, 읽을 것이 없으면 바로
     * {@link Inbound#none()}). 음수면 읽을 것이나 {@code wakeup}이 올 때까지 상한 없이 기다린다. 양수면 그 밀리초까지만 기다린다. 구현은
     * {@code PassageContract}(test fixtures)를 상속한 테스트로 이를 확인한다.
     *
     * @param timeoutMs 기다리는 상한(밀리초). 0은 기다리지 않고 폴, 음수는 상한 없음, 양수는 그 밀리초까지
     * @return 읽은 것. 읽을 것이 없이 깨어났으면 {@link Inbound#none()}
     * @throws IOException 입출력 오류. 드라이버는 연결이 끊긴 것으로 본다
     */
    Inbound await(long timeoutMs) throws IOException;

    /**
     * 메시지 하나를 상대에게 보낸다. 돌아오면 다 쓴 것이다.
     *
     * @param message 보낼 메시지
     * @throws IOException 입출력 오류. 드라이버는 연결이 끊긴 것으로 본다
     */
    void write(OutboundMessage message) throws IOException;

    /** 기다리고 있거나 곧 기다릴 {@link #await}를 깨운다. 어느 스레드에서든 부를 수 있다. */
    void wakeup();

    /** 통로를 닫는다. 상대는 끊김을 본다. 어느 스레드에서든 부를 수 있고 여러 번 불러도 된다. */
    void close();
}
