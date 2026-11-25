package io.github.ppzxc.strand.netty.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

public final class WebSocketWire implements MessageWire {
    public static final String SUBPROTOCOL = "strand";

    private static final int MAX_HTTP_BYTES = 16 * 1024;

    private final int maxMessageBytes;
    private final EmbeddedChannel handshake;
    private final EmbeddedChannel encoder;
    private final ArrayDeque<WireMessage> messages = new ArrayDeque<>();
    private @Nullable WebSocketClientHandshaker clientHandshaker;
    private @Nullable WireUpgradeException upgradeFailure;
    private boolean established;
    private boolean broken;
    private boolean closeSent;
    private boolean released;

    private WebSocketWire(boolean client, int maxMessageBytes, EmbeddedChannel handshake) {
        this.maxMessageBytes = maxMessageBytes;
        this.handshake = handshake;
        this.encoder = new EmbeddedChannel(new WebSocket13FrameEncoder(client));
    }

    public static WebSocketWire server(String path, int maxMessageBytes) {
        WebSocketWire[] self = new WebSocketWire[1];
        EmbeddedChannel channel = new EmbeddedChannel(
                new HttpServerCodec(),
                new HttpObjectAggregator(MAX_HTTP_BYTES),
                new ServerUpgrade(() -> self[0], path, maxMessageBytes));
        self[0] = new WebSocketWire(false, maxMessageBytes, channel);
        return self[0];
    }

    public static WebSocketWire client(URI uri, int maxMessageBytes) {
        WebSocketWire[] self = new WebSocketWire[1];
        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, SUBPROTOCOL, false, null, maxMessageBytes);
        EmbeddedChannel channel = new EmbeddedChannel(
                new HttpClientCodec(),
                new HttpObjectAggregator(MAX_HTTP_BYTES),
                new ClientUpgrade(() -> self[0], handshaker));
        self[0] = new WebSocketWire(true, maxMessageBytes, channel);
        self[0].clientHandshaker = handshaker;
        return self[0];
    }

    public static WebSocketWire established(boolean client, int maxMessageBytes) {
        EmbeddedChannel channel = new EmbeddedChannel(
                frameDecoder(client, maxMessageBytes), new WebSocketFrameAggregator(maxMessageBytes));
        WebSocketWire wire = new WebSocketWire(client, maxMessageBytes, channel);
        wire.established = true;
        return wire;
    }

    private static WebSocket13FrameDecoder frameDecoder(boolean client, int maxMessageBytes) {
        return new WebSocket13FrameDecoder(WebSocketDecoderConfig.newBuilder()
                .expectMaskedFrames(!client)
                .allowMaskMismatch(false)
                .allowExtensions(false)
                .maxFramePayloadLength(maxMessageBytes)
                .closeOnProtocolViolation(false)
                .build());
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void start() throws IOException {
        WebSocketClientHandshaker handshaker = clientHandshaker;
        if (handshaker == null) {
            return;
        }
        try {
            handshaker.handshake(handshake);
        } catch (RuntimeException e) {
            throw new WireUpgradeException("업그레이드 요청을 만들지 못했다", 0, e);
        }
    }

    @Override
    public void feed(byte[] bytes) throws IOException {
        if (broken || released) {
            return;
        }
        boolean wasEstablished = established;
        try {
            handshake.writeInbound(Unpooled.wrappedBuffer(bytes));
        } catch (Exception e) {
            if (!established && upgradeFailure == null) {
                upgradeFailure = new WireUpgradeException("업그레이드 중 입력을 해석하지 못했다", 0, e);
            } else if (established) {
                broken = true;
                messages.add(WireMessage.unsupported());
            }
        }
        if (upgradeFailure != null) {
            broken = true;
            throw upgradeFailure;
        }
        if (established) {
            collectFrames();
        } else if (!wasEstablished && !handshake.isOpen()) {
            broken = true;
            throw new WireUpgradeException("업그레이드를 마치기 전에 통로가 닫혔다", 0);
        }
    }

    @Override
    public boolean established() {
        return established;
    }

    @Override
    public boolean hasMessages() {
        return !messages.isEmpty();
    }

    @Override
    public @Nullable WireMessage pollMessage() {
        return messages.poll();
    }

    @Override
    public byte @Nullable [] pollWire() {
        if (!wirePending()) {
            return null;
        }
        ByteBuf out = Unpooled.buffer();
        try {
            drain(handshake, out, !established || pendingHandshakeResponse);
            pendingHandshakeResponse = false;
            drain(encoder, out, true);
            if (!out.isReadable()) {
                return null;
            }
            return ByteBufUtil.getBytes(out);
        } finally {
            out.release();
        }
    }

    private boolean pendingHandshakeResponse;

    private static void drain(EmbeddedChannel channel, ByteBuf into, boolean keep) {
        Object next = channel.readOutbound();
        while (next != null) {
            if (next instanceof ByteBuf) {
                ByteBuf buffer = (ByteBuf) next;
                if (keep) {
                    into.writeBytes(buffer);
                }
                buffer.release();
            } else {
                ReferenceCountUtil.release(next);
            }
            next = channel.readOutbound();
        }
    }

    @Override
    public boolean wirePending() {
        boolean handshakeBytes = !handshake.outboundMessages().isEmpty() && (!established || pendingHandshakeResponse);
        return handshakeBytes || !encoder.outboundMessages().isEmpty();
    }

    @Override
    public void sendMessage(byte[] bytes) throws IOException {
        if (!established || closeSent || released) {
            throw new IOException("메시지를 보낼 수 없다");
        }
        encoder.writeOutbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes)));
    }

    @Override
    public void sendClose(int status) {
        if (!established || closeSent || released) {
            return;
        }
        closeSent = true;
        encoder.writeOutbound(status < 0 ? new CloseWebSocketFrame() : new CloseWebSocketFrame(status, ""));
    }

    @Override
    public boolean closeSent() {
        return closeSent;
    }

    @Override
    public void release() {
        if (released) {
            return;
        }
        released = true;
        messages.clear();
        handshake.finishAndReleaseAll();
        encoder.finishAndReleaseAll();
    }

    private void collectFrames() {
        Object next = handshake.readInbound();
        while (next != null) {
            try {
                if (next instanceof BinaryWebSocketFrame) {
                    messages.add(WireMessage.binary(ByteBufUtil.getBytes(((BinaryWebSocketFrame) next).content())));
                } else if (next instanceof PingWebSocketFrame) {
                    if (!closeSent) {
                        encoder.writeOutbound(new PongWebSocketFrame(
                                ((PingWebSocketFrame) next).content().retainedSlice()));
                    }
                } else if (next instanceof PongWebSocketFrame) {
                } else if (next instanceof CloseWebSocketFrame) {
                    CloseWebSocketFrame close = (CloseWebSocketFrame) next;
                    sendClose(close.statusCode());
                    messages.add(WireMessage.peerClosed());
                    broken = true;
                } else {
                    messages.add(WireMessage.unsupported());
                    broken = true;
                }
            } finally {
                ReferenceCountUtil.release(next);
            }
            if (broken) {
                break;
            }
            next = handshake.readInbound();
        }
    }

    private void upgraded() {
        established = true;
        pendingHandshakeResponse = true;
    }

    private static final class ServerUpgrade extends ChannelInboundHandlerAdapter {
        private final Supplier<WebSocketWire> wire;
        private final String path;
        private final int maxMessageBytes;

        ServerUpgrade(Supplier<WebSocketWire> wire, String path, int maxMessageBytes) {
            this.wire = wire;
            this.path = path;
            this.maxMessageBytes = maxMessageBytes;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (!(message instanceof FullHttpRequest)) {
                ReferenceCountUtil.release(message);
                return;
            }
            FullHttpRequest request = (FullHttpRequest) message;
            try {
                handle(ctx, request);
            } finally {
                request.release();
            }
        }

        private void handle(ChannelHandlerContext ctx, FullHttpRequest request) {
            WebSocketWire self = wire.get();
            if (request.decoderResult().isFailure()) {
                reject(ctx, self, HttpResponseStatus.BAD_REQUEST, "형식이 어긋난 HTTP 요청");
            } else if (!path.equals(new QueryStringDecoder(request.uri()).rawPath())) {
                reject(ctx, self, HttpResponseStatus.NOT_FOUND, "경로가 다르다: " + request.uri());
            } else if (!isUpgrade(request)) {
                reject(ctx, self, HttpResponseStatus.NOT_FOUND, "WebSocket 업그레이드 요청이 아니다");
            } else if (!offersStrand(request)) {
                reject(ctx, self, HttpResponseStatus.BAD_REQUEST, "하위 프로토콜 " + SUBPROTOCOL + "이(가) 제시되지 않았다");
            } else {
                upgrade(ctx, self, request);
            }
        }

        private static boolean isUpgrade(FullHttpRequest request) {
            return HttpMethod.GET.equals(request.method())
                    && request.headers().containsValue(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE, true)
                    && request.headers().containsValue(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true);
        }

        private static boolean offersStrand(FullHttpRequest request) {
            for (String header : request.headers().getAll(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL)) {
                int from = 0;
                while (from <= header.length()) {
                    int comma = header.indexOf(',', from);
                    int to = comma < 0 ? header.length() : comma;
                    if (SUBPROTOCOL.equals(header.substring(from, to).trim())) {
                        return true;
                    }
                    from = to + 1;
                }
            }
            return false;
        }

        @SuppressWarnings("FutureReturnValueIgnored")
        private void upgrade(ChannelHandlerContext ctx, WebSocketWire self, FullHttpRequest request) {
            String host = request.headers().get(HttpHeaderNames.HOST, "localhost");
            WebSocketServerHandshaker handshaker = new WebSocketServerHandshakerFactory(
                            "ws://" + host + path, SUBPROTOCOL, false, maxMessageBytes)
                    .newHandshaker(request);
            if (handshaker == null) {
                WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
                self.upgradeFailure = new WireUpgradeException("지원하지 않는 WebSocket 버전", 426);
                return;
            }
            try {
                handshaker.handshake(ctx.channel(), request);
            } catch (RuntimeException e) {
                self.upgradeFailure = new WireUpgradeException("업그레이드를 마치지 못했다", 0, e);
                return;
            }
            ctx.pipeline().addLast(new WebSocketFrameAggregator(maxMessageBytes));
            ctx.pipeline().remove(this);
            self.upgraded();
        }

        @SuppressWarnings("FutureReturnValueIgnored")
        private static void reject(
                ChannelHandlerContext ctx, WebSocketWire self, HttpResponseStatus status, String reason) {
            FullHttpResponse response =
                    new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
            response.headers()
                    .set(HttpHeaderNames.CONTENT_LENGTH, 0)
                    .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            ctx.writeAndFlush(response);
            self.upgradeFailure = new WireUpgradeException(reason, status.code());
        }
    }

    private static final class ClientUpgrade extends ChannelInboundHandlerAdapter {
        private final Supplier<WebSocketWire> wire;
        private final WebSocketClientHandshaker handshaker;

        ClientUpgrade(Supplier<WebSocketWire> wire, WebSocketClientHandshaker handshaker) {
            this.wire = wire;
            this.handshaker = handshaker;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (!(message instanceof FullHttpResponse)) {
                ReferenceCountUtil.release(message);
                return;
            }
            FullHttpResponse response = (FullHttpResponse) message;
            WebSocketWire self = wire.get();
            try {
                handshaker.finishHandshake(ctx.channel(), response);
                if (!SUBPROTOCOL.equals(handshaker.actualSubprotocol())) {
                    self.upgradeFailure = new WireUpgradeException(
                            "서버가 하위 프로토콜 " + SUBPROTOCOL + "을(를) 선택하지 않았다",
                            response.status().code());
                    return;
                }
                ctx.pipeline().addLast(new WebSocketFrameAggregator(self.maxMessageBytes));
                ctx.pipeline().remove(this);
                self.upgraded();
            } catch (WebSocketHandshakeException | IllegalStateException e) {
                self.upgradeFailure = new WireUpgradeException(
                        "업그레이드 응답이 거절됐다: " + response.status(),
                        response.status().code(),
                        e);
            } finally {
                response.release();
            }
        }
    }
}
