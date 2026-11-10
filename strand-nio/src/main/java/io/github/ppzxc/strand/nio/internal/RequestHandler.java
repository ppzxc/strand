package io.github.ppzxc.strand.nio.internal;

/** 상대가 낸 요청을 받는 애플리케이션의 핸들러. 사용자가 준 실행기에서 불린다. */
@FunctionalInterface
public interface RequestHandler {

    /**
     * 받은 요청을 처리한다. 응답은 어느 스레드에서든 {@link IncomingRequest#respond}로 한다.
     *
     * @param request 받은 요청
     */
    void handle(IncomingRequest request);
}
