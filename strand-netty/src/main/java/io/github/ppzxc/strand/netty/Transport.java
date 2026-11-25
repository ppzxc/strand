package io.github.ppzxc.strand.netty;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

public final class Transport {
    public static final String DEFAULT_PATH = "/";

    private static final Transport TCP = new Transport(null);

    private final @Nullable String path;

    private Transport(@Nullable String path) {
        this.path = path;
    }

    public static Transport tcp() {
        return TCP;
    }

    public static Transport webSocket() {
        return webSocket(DEFAULT_PATH);
    }

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

    public boolean isWebSocket() {
        return path != null;
    }

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
