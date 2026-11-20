package io.github.ppzxc.strand.netty;

/** 상대가 낸 요청을 받는 핸들러. 앱용 실행기에서 같은 연결의 호출끼리 직렬로 불린다. 응답은 어느 스레드에서든 할 수 있고 응답 없이 반환해도 된다. */
@FunctionalInterface
public interface RequestHandler {

    /**
     * 받은 요청을 처리한다. 던지면 요청이 아직 살아 있고 응답 전일 때 빈 {@code INTERNAL} 응답을 시도하며 연결은 유지한다. 예외 메시지는 상대에게
     * 보내지 않는다.
     *
     * @param request 받은 요청
     * @throws Exception 처리하지 못했을 때
     */
    void handle(IncomingRequest request) throws Exception;
}
