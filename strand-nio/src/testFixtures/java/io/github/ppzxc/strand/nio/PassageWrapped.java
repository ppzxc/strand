package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.nio.internal.PassageWrapping;

/**
 * 테스트 전용: builder가 만드는 연결의 통로를 감싸는 감싸개를 builder에 주입한다. 빌더의 감싸개 주입 지점은 공개 패키지의 패키지 전용 멤버라 같은 패키지의 이
 * 클래스만 부를 수 있다. 공개 API에는 없다.
 */
public final class PassageWrapped {

    private PassageWrapped() {}

    /**
     * 이 builder로 만든 서버의 연결 통로를 감싼다.
     *
     * @param builder 서버 builder
     * @param wrapper 감싸개
     * @return 같은 builder
     */
    public static NioStrandServer.Builder server(NioStrandServer.Builder builder, PassageWrapping.Wrapper wrapper) {
        return builder.passageWrapper(wrapper);
    }

    /**
     * 이 builder로 만든 클라이언트의 연결 통로를 감싼다.
     *
     * @param builder 클라이언트 builder
     * @param wrapper 감싸개
     * @return 같은 builder
     */
    public static NioStrandClient.Builder client(NioStrandClient.Builder builder, PassageWrapping.Wrapper wrapper) {
        return builder.passageWrapper(wrapper);
    }
}
