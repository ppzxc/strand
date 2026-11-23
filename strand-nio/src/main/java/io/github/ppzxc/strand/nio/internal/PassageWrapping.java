package io.github.ppzxc.strand.nio.internal;

import org.jspecify.annotations.Nullable;

/**
 * 테스트 전용: 제품 엔진이 연 통로를 테스트가 감싸게 하는 지점. 엔진은 통로를 안에서 만들어 테스트가 직접 끼울 수 없으므로, 엔진이 통로를 연 직후 연결을 만든
 * builder에 주입된 감싸개가 있으면 그것이 돌려준 통로 위에서 연결을 돌린다. 쓰기 시점을 늦추거나 쓴 프레임을 기록하는 시험이 쓴다.
 *
 * <p>감싸개는 builder 하나에 주입되어 그 builder로 만든 서버·클라이언트의 연결에만 적용된다. 프로세스 전체에 걸리는 설치는 없으므로 같은 프로세스에서 도는 다른
 * 연결에는 영향이 없다. 이 타입은 {@code internal}에만 있고 공개 API(공개 패키지의 public 타입·메서드)에 나타나지 않는다. TLS 통로는 엔진이 TLS 상태를 직접
 * 조회하므로 감싸지 않는다.
 */
public final class PassageWrapping {

    /** 감싸는 통로가 어느 쪽 연결의 것인가. */
    public enum Side {
        CLIENT,
        SERVER
    }

    /** 엔진이 연 통로를 감싼 통로로 바꾼다. 연결 스레드에서 불린다. */
    @FunctionalInterface
    public interface Wrapper {
        /**
         * 엔진이 대신 돌릴 통로를 돌려준다.
         *
         * @param side 연결의 쪽
         * @param passage 엔진이 연 통로
         * @return 엔진이 대신 돌릴 통로. 받은 통로를 그대로 돌려줘도 된다
         */
        Passage wrap(Side side, Passage passage);
    }

    private PassageWrapping() {}

    /** 엔진이 통로를 연 직후 부른다. 감싸개가 없거나 TLS 통로면 받은 통로 그대로다. */
    public static Passage wrap(@Nullable Wrapper wrapper, Side side, Passage passage) {
        if (wrapper == null || passage instanceof TlsPassage) {
            return passage;
        }
        return wrapper.wrap(side, passage);
    }
}
