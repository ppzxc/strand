package io.github.ppzxc.strand.core;

import org.jspecify.annotations.Nullable;

/**
 * 실행 중인 core의 버전. 공개 전까지 바꾸거나 지울 수 있는 임시 API다.
 */
public final class StrandVersion {

    private StrandVersion() {}

    /**
     * core jar 매니페스트의 {@code Implementation-Version}을 반환한다.
     *
     * @return 버전 문자열. jar가 아니거나 매니페스트에 값이 없으면 {@code null}
     */
    public static @Nullable String get() {
        return StrandVersion.class.getPackage().getImplementationVersion();
    }
}
