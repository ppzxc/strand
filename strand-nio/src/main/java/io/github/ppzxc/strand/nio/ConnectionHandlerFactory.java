package io.github.ppzxc.strand.nio;

/** 인증된 연결마다 핸들러를 만든다. 앱용 실행기에서 불리므로 블로킹해도 된다. 서버가 연결별 업무 상태를 둘 때 쓴다. */
@FunctionalInterface
public interface ConnectionHandlerFactory {

    /**
     * 인증된 연결의 핸들러를 만든다. 던지면 준비 실패다.
     *
     * @param connection 인증된 연결. {@link NioStrandConnection#authenticatedPeer()}를 조회할 수 있다
     * @return 핸들러와 폐기 동작
     * @throws Exception 만들지 못했을 때
     */
    PreparedHandler create(NioStrandConnection connection) throws Exception;
}
