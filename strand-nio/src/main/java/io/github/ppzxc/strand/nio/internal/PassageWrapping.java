package io.github.ppzxc.strand.nio.internal;

import org.jspecify.annotations.Nullable;

/**
 * 테스트 전용: 제품 엔진이 연 통로를 테스트가 감싸게 하는 지점. 엔진은 통로를 안에서 만들어 테스트가 직접 끼울 수 없으므로, 엔진이 통로를 연 직후 설치된
 * 감싸개가 있으면 그것이 돌려준 통로 위에서 연결을 돌린다. 쓰기 시점을 늦추거나 쓴 프레임을 기록하는 시험이 쓴다.
 *
 * <p>설치는 프로세스 전체에 걸리므로 설치한 시험이 끝나면 {@link Installation#close()}로 되돌린다. 같은 프로세스에서 연결을 동시에 돌리는 다른 시험이 있으면 그
 * 연결의 통로도 감싼다. 이 타입은 {@code internal}에만 있고 공개 API(공개 패키지의 public 타입·메서드)에 나타나지 않는다. TLS 통로는 엔진이 TLS 상태를 직접
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

    /** 설치를 되돌린다. */
    public interface Installation extends AutoCloseable {
        /** 설치 전의 감싸개로 되돌린다. 여러 번 불러도 된다. */
        @Override
        void close();
    }

    private static volatile @Nullable Wrapper installed;

    private PassageWrapping() {}

    /**
     * 감싸개를 설치한다. 이미 설치된 감싸개는 돌려주는 {@link Installation}을 닫을 때 되살아난다.
     *
     * @param wrapper 감싸개
     * @return 닫으면 설치 전으로 되돌리는 핸들
     */
    public static Installation install(Wrapper wrapper) {
        Wrapper previous = installed;
        installed = wrapper;
        return new Installation() {
            private boolean closed;

            @Override
            public synchronized void close() {
                if (!closed) {
                    closed = true;
                    installed = previous;
                }
            }
        };
    }

    /** 엔진이 통로를 연 직후 부른다. 설치된 감싸개가 없거나 TLS 통로면 받은 통로 그대로다. */
    public static Passage wrap(Side side, Passage passage) {
        Wrapper wrapper = installed;
        if (wrapper == null || passage instanceof TlsPassage) {
            return passage;
        }
        return wrapper.wrap(side, passage);
    }
}
