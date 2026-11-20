package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * 준비를 마친 Strand 연결. 양방향 요청을 보내고, 연결을 닫고, 종료 알림을 관찰한다. 클라이언트 {@code connect}가 성공했을 때는 보안·HELLO·연결
 * 인증·핸들러 등록이 모두 끝나 있다. 어느 스레드에서든 부를 수 있다.
 */
public final class NettyStrandConnection {
    private final ConnectionEngine engine;
    private final SocketAddress remoteAddress;
    private volatile @Nullable AuthenticatedPeer peer;

    NettyStrandConnection(ConnectionParams params, StrandChannel channel) {
        this.remoteAddress = params.remoteAddress;
        this.engine = new ConnectionEngine(this, params, channel);
    }

    /** 클라이언트: 채널을 등록하고 서버에 연결한다. */
    void startClient(InetSocketAddress target) {
        engine.startClient(target);
    }

    /** 서버: 수락한 채널을 등록한다. */
    void startServer() {
        engine.startServer();
    }

    /**
     * 요청을 낸다. 요청 기한은 없다. 결과는 {@link RequestHandle#result()}로 한 번 확정되어 보존된다. 로컬에서 접수하지 못한 요청도 예외 없이 결과로
     * 끝난다: 종료된 연결은 {@code UNAVAILABLE}, 한도나 협상한 프레임 크기를 넘으면 {@code RESOURCE_EXHAUSTED}이고 모두 처리 안 됨이다.
     *
     * @param request 요청. 호출 중 복사한다
     * @return 요청 핸들
     */
    public RequestHandle request(Request request) {
        return engine.request(Objects.requireNonNull(request, "request"), RequestOptions.noDeadline());
    }

    /**
     * 기한을 두고 요청을 낸다. 기한은 이 호출 시점부터 로컬 대기를 포함해 센다. 0이면 즉시 만료(처리 안 됨)다.
     *
     * @param request 요청. 호출 중 복사한다
     * @param options 요청 옵션
     * @return 요청 핸들
     */
    public RequestHandle request(Request request, RequestOptions options) {
        return engine.request(Objects.requireNonNull(request, "request"), Objects.requireNonNull(options, "options"));
    }

    /** 연결 종료(CLOSE) 절차를 시작한다. 새 요청은 받지 않고 대기 중인 요청이 끝나길 기다린다. 반복해도 추가 효과가 없다. */
    public void close() {
        engine.close();
    }

    /** 즉시 끊는다. 이미 close()를 불렀어도 즉시 끊기로 바뀐다. 반복해도 추가 효과가 없다. */
    public void abort() {
        engine.abort();
    }

    /**
     * 연결 종료를 알리는 보존된 결과. 남은 요청 결과와 종료 원인을 보존하고 실제 통로(소켓·TLS 엔진·내부 버퍼)를 정리한 뒤 완료한다. 핸들러 반환이나
     * 알림 실행은 기다리지 않는다.
     *
     * @return 종료 알림 결과
     */
    public Result<CloseNotice> closed() {
        return engine.closed();
    }

    /**
     * 인증된 상대. 서버 연결에서는 연결 인증을 통과한 뒤(팩터리가 불리기 전)부터, 클라이언트 연결에서는 준비가 끝난 뒤부터 조회된다.
     *
     * @return 인증된 상대
     */
    public AuthenticatedPeer authenticatedPeer() {
        // 연결은 인증이 끝난 뒤에만 앱에 보인다(서버는 팩터리가 불리기 전, 클라이언트는 connect 성공 전에 설정한다).
        return Objects.requireNonNull(peer, "인증되지 않은 연결은 앱에 보이지 않는다");
    }

    /**
     * 상대 주소.
     *
     * @return 상대 주소
     */
    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * 이 연결 범위의 한도 사용량 집계(설정값·현재량·최고량·거절 횟수). 자격 증명은 담지 않는다.
     *
     * @param resource 조회할 종류
     * @return 사용량
     * @throws IllegalArgumentException 연결 범위에 없는 종류일 때
     */
    public ResourceUsage resourceUsage(Resource resource) {
        return engine.usage(resource);
    }

    void shutdownClose() {
        engine.shutdownClose();
    }

    void shutdownAbort() {
        engine.shutdownAbort();
    }

    void authenticated(AuthenticatedPeer value) {
        this.peer = value;
    }
}
