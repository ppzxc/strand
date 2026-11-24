package io.github.ppzxc.strand.netty;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * 연결의 전송 방식(규격 1.2절). {@link NettyStrandServer#listen(java.net.SocketAddress, Transport)}와 {@link
 * NettyStrandClient#connect(java.net.SocketAddress, Transport)}에 넘겨 고른다. 값 객체이며 통로를 끼우는 확장 지점이 아니다.
 *
 * <p>{@link #webSocket(String)}은 Netty의 WebSocket·HTTP 코덱({@code netty-codec-http})이 클래스패스에 있어야 쓸 수 있다. TCP만 쓰면 필요 없다.
 * 보안은 전송과 무관하게 {@code security(...)}로 정한다: {@code tls}면 TLS 위의 WebSocket(wss)이고 ALPN은 쓰지 않는다.
 */
public final class Transport {
    /** 경로를 생략한 WebSocket이 쓰는 경로. */
    public static final String DEFAULT_PATH = "/";

    private static final Transport TCP = new Transport(null);

    private final @Nullable String path;

    private Transport(@Nullable String path) {
        this.path = path;
    }

    /**
     * 바이트 스트림(TCP) 전송.
     *
     * @return TCP 전송
     */
    public static Transport tcp() {
        return TCP;
    }

    /**
     * 경로 {@value #DEFAULT_PATH}의 WebSocket 전송.
     *
     * @return WebSocket 전송
     */
    public static Transport webSocket() {
        return webSocket(DEFAULT_PATH);
    }

    /**
     * 주어진 경로의 WebSocket 전송. 서버는 이 경로와 정확히 일치하는 요청만 업그레이드하고, 클라이언트는 이 경로로 업그레이드를 요청한다.
     *
     * @param path {@code /}로 시작하는 요청 경로. 질의({@code ?})·조각({@code #})·공백·제어 문자를 담을 수 없다
     * @return WebSocket 전송
     * @throws IllegalArgumentException 경로가 형식에 맞지 않을 때
     */
    public static Transport webSocket(String path) {
        Objects.requireNonNull(path, "path");
        if (path.isEmpty() || path.charAt(0) != '/') {
            throw new IllegalArgumentException("WebSocket 경로는 /로 시작해야 한다: " + path);
        }
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c <= ' ' || c == 0x7f || c == '?' || c == '#' || c > 0x7e) {
                throw new IllegalArgumentException("WebSocket 경로에 쓸 수 없는 문자가 있다: " + path);
            }
        }
        return new Transport(path);
    }

    /**
     * WebSocket 전송인가.
     *
     * @return WebSocket이면 true
     */
    public boolean isWebSocket() {
        return path != null;
    }

    /**
     * WebSocket 전송의 경로.
     *
     * @return 경로. TCP 전송이면 {@code null}
     */
    public @Nullable String path() {
        return path;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Transport && Objects.equals(path, ((Transport) other).path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(path);
    }

    @Override
    public String toString() {
        return path == null ? "tcp" : "webSocket(" + path + ")";
    }
}
