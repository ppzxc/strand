package io.github.ppzxc.strand.netty.internal;

import java.net.URI;

public final class WireFactory {
    private static final String[] REQUIRED = {
        "io.netty.handler.codec.http.HttpServerCodec",
        "io.netty.handler.codec.http.websocketx.WebSocketFrame",
        "io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker"
    };

    private WireFactory() {}

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

    public static MessageWire server(String path, int maxMessageBytes) {
        return WebSocketWire.server(path, maxMessageBytes);
    }

    public static MessageWire client(URI uri, int maxMessageBytes) {
        return WebSocketWire.client(uri, maxMessageBytes);
    }
}
