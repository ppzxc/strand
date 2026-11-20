package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.TlsSizes;
import java.util.Objects;
import javax.net.ssl.SSLContext;

/**
 * 연결당 최소 진행 공간 조회. 앱은 연결별 한도와 소유 객체 한도를 정하기 전에 이 값을 읽는다. 소유 객체 한도를 이 값으로 나누면 동시에 진행을 보장할 수 있는
 * 연결 수를 계획할 수 있다. 빌더 검증과 같은 계산을 쓰므로, 이 값 그대로 한도를 설정하면 {@code build()}가 한도 부족으로 실패하지 않는다.
 */
public final class NettyMinimumProgress {
    private NettyMinimumProgress() {}

    /**
     * 연결당 최소 진행 공간을 돌려준다. 빌더 상태에 의존하지 않는다. TLS를 쓰면 현재 JDK·provider의 TLS 버퍼 크기를 반영하고, TLS를 쓰지 않으면 TLS
     * 종류({@link Resource#TLS_PLAINTEXT_BYTES}, {@link Resource#TLS_CIPHERTEXT_BYTES}, {@link
     * Resource#TLS_CONTROL_BYTES})는 0이다.
     *
     * @param security 연결에 쓸 채널 보안
     * @param maxFrameSize 받을 수 있는 최대 프레임 크기(16384~1048576)
     * @param outgoingRequests 연결별 OUTGOING 요청 수
     * @param incomingRequests 연결별 INCOMING 요청 수
     * @return 자원 종류별 연결당 최소량(바이트)
     */
    public static MinimumProgressSpace of(
            NettySecurity security, long maxFrameSize, long outgoingRequests, long incomingRequests) {
        Objects.requireNonNull(security, "security");
        SSLContext context = security.tlsContext();
        ProgressSpace space = ProgressSpace.derive(
                outgoingRequests, incomingRequests, maxFrameSize, context == null ? null : TlsSizes.of(context));
        return space.toApi();
    }
}
