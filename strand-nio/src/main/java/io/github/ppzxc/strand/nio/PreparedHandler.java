package io.github.ppzxc.strand.nio;

import java.util.Objects;

/**
 * 연결별 팩터리가 돌려주는 핸들러와, 활성화되지 못한 결과를 정리하는 폐기 동작. 폐기 동작은 이 결과가 연결에 활성화되지 못했을 때(연결이 먼저
 * 끝났거나 준비 기한이 지남)만 실행된다. 활성화된 뒤의 앱 자원은 연결의 종료 알림({@link NioStrandConnection#closed()})으로 정리한다.
 */
public final class PreparedHandler {
    private final RequestHandler handler;
    private final Runnable disposal;

    private PreparedHandler(RequestHandler handler, Runnable disposal) {
        this.handler = handler;
        this.disposal = disposal;
    }

    /**
     * 핸들러와 폐기 동작을 묶는다.
     *
     * @param handler 활성화되면 요청을 받을 핸들러
     * @param disposal 활성화되지 못했을 때 앱 자원을 정리하는 동작. 앱용 실행기에서 한 번 실행된다
     * @return 준비된 핸들러
     */
    public static PreparedHandler of(RequestHandler handler, Runnable disposal) {
        return new PreparedHandler(
                Objects.requireNonNull(handler, "handler"), Objects.requireNonNull(disposal, "disposal"));
    }

    /**
     * 정리할 자원이 없는 준비된 핸들러.
     *
     * @param handler 핸들러
     * @return 준비된 핸들러
     */
    public static PreparedHandler of(RequestHandler handler) {
        return of(handler, () -> {});
    }

    RequestHandler handler() {
        return handler;
    }

    void dispose() {
        disposal.run();
    }
}
