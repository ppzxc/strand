package io.github.ppzxc.strand.nio.internal;

import java.util.OptionalLong;

/**
 * 연결의 상태 알림을 받는 애플리케이션의 리스너. 핸들러 실행기에서 불리므로 전송 스레드를 막지 않고 블로킹 작업을 해도 된다. 실행기가 스레드를
 * 여럿 쓰면 알림 사이의 순서는 보장되지 않는다.
 */
public interface ConnectionListener {

    /**
     * 클라이언트가 서버의 {@code AUTH(ACK)}를 받아 AUTH 관문을 통과했다(규격 3.3절). 클라이언트 연결에서만 불린다.
     *
     * @param credentialLifetime 서버가 알린 자격 증명의 남은 수명(초, 규격 3.3.1절). 알리지 않았으면 비어 있다
     */
    default void onAuthenticated(OptionalLong credentialLifetime) {}

    /**
     * 연결이 끝났다. 연결마다 한 번만 불린다.
     *
     * @param event 종료 원인
     */
    default void onClosed(ConnectionClosed event) {}
}
