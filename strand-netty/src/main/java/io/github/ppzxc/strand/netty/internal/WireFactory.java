package io.github.ppzxc.strand.netty.internal;

import java.net.URI;

/**
 * 메시지 통로(WebSocket) 생성의 유일한 입구. 이 클래스는 Netty 코덱 타입을 직접 쓰지 않아 코덱이 없는 클래스패스에서도 읽을 수 있고, 코덱을 쓰는 {@link
 * WebSocketWire}는 통로를 실제로 만드는 {@code server}·{@code client}에서만 읽힌다. TCP만 쓰는 연결은 이 클래스를 만나지 않는다.
 */
public final class WireFactory {
    private static final String[] REQUIRED = {
        "io.netty.handler.codec.http.HttpServerCodec",
        "io.netty.handler.codec.http.websocketx.WebSocketFrame",
        "io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker"
    };

    private WireFactory() {}

    /**
     * WebSocket 전송에 필요한 코덱({@code netty-codec-http})이 클래스패스에 있는지 확인한다.
     *
     * @throws IllegalStateException 코덱이 없을 때
     */
    public static void requireCodec() {
        requireCodec(WireFactory.class.getClassLoader());
    }

    static void requireCodec(ClassLoader loader) {
        for (String name : REQUIRED) {
            try {
                Class.forName(name, false, loader);
            } catch (ClassNotFoundException | LinkageError e) {
                throw new IllegalStateException(
                        "WebSocket 전송에는 netty-codec-http가 필요하다(클래스패스에 " + name
                                + "이(가) 없다). 앱이 netty-codec-http를 Netty와 같은 버전으로 제공해야 한다",
                        e);
            }
        }
    }

    /** 서버 쪽 WebSocket 통로. */
    public static MessageWire server(String path, int maxMessageBytes) {
        return WebSocketWire.server(path, maxMessageBytes);
    }

    /** 클라이언트 쪽 WebSocket 통로. */
    public static MessageWire client(URI uri, int maxMessageBytes) {
        return WebSocketWire.client(uri, maxMessageBytes);
    }
}
