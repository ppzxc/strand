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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import org.jspecify.annotations.Nullable;

/**
 * Netty의 HTTP·WebSocket 코덱으로 만든 {@link MessageWire}. 연결 채널과 별개의 {@link EmbeddedChannel}에 코덱 파이프라인을 얹어 평문 바이트를
 * 메시지로, 메시지를 평문 바이트로 바꾼다. 소켓·TLS·쓰기 진행 보고는 연결 엔진의 기존 경로를 그대로 쓰므로 이 클래스는 입출력 바이트와 메시지만 오간다.
 *
 * <p>업그레이드 단계(서버: 경로·업그레이드 요청·하위 프로토콜 검사, 클라이언트: 요청·응답 검사)는 HTTP 코덱 파이프라인이 하고, 마친 뒤의 프레임 변환은 Netty의 WebSocket 13
 * 코덱이 한다. 프레임은 조각을 {@code maxMessageBytes}까지 모아 한 메시지로 넘기며 한도를 넘는 프레임·누적은 버퍼링 전에 코덱이 거절한다. 나가는 프레임은 별도 인코더로
 * 만들어 Netty가 프로토콜 위반 때 자체로 쓰는 Close(상태 코드 1009 등)가 연결에 섞이지 않게 한다. Close의 상태 코드와 사유(항상 비움)는 엔진이 정한다.
 */
public final class WebSocketWire implements MessageWire {
    /** 하위 프로토콜 식별자(규격 1.2.1절). 고정이며 설정으로 열지 않는다. */
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

    /**
     * 서버 쪽 통로. 정확히 {@code path}인 업그레이드 요청만 받는다.
     *
     * @param path 요청 경로
     * @param maxMessageBytes 한 메시지의 최대 바이트 수(core의 {@code max_frame_size})
     */
    public static WebSocketWire server(String path, int maxMessageBytes) {
        WebSocketWire[] self = new WebSocketWire[1];
        EmbeddedChannel channel = new EmbeddedChannel(
                new HttpServerCodec(),
                new HttpObjectAggregator(MAX_HTTP_BYTES),
                new ServerUpgrade(() -> self[0], path, maxMessageBytes));
        self[0] = new WebSocketWire(false, maxMessageBytes, channel);
        return self[0];
    }

    /**
     * 클라이언트 쪽 통로. {@link #start}가 업그레이드 요청을 낸다.
     *
     * @param uri {@code ws://host:port/path} 또는 {@code wss://...}. Host 헤더가 여기서 만들어진다
     * @param maxMessageBytes 한 메시지의 최대 바이트 수(core의 {@code max_frame_size})
     */
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

    /**
     * 업그레이드를 이미 마친 통로. HTTP 단계 없이 프레임 변환만 한다. 적합성 검증처럼 핸드셰이크 없이 메시지 통로만 시험할 때 쓴다.
     *
     * @param client 클라이언트 쪽이면 true(나가는 프레임을 마스킹하고 들어오는 프레임은 마스킹을 기대하지 않는다)
     * @param maxMessageBytes 한 메시지의 최대 바이트 수
     */
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
    @SuppressWarnings("FutureReturnValueIgnored") // EmbeddedChannel의 쓰기는 호출 안에서 끝난다. 실패는 예외나 upgradeFailure로 드러난다.
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
        ByteBuf out = Unpooled.buffer();
        try {
            // 업그레이드를 마친 뒤 handshake 채널이 스스로 쓰는 것(Netty가 프로토콜 위반에 내는 Close 등)은 연결에 보내지 않는다.
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

    /** 업그레이드를 마친 순간에 handshake 채널에 쌓인 응답(서버의 101)이 아직 나가지 않았는가. */
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
                io.netty.util.ReferenceCountUtil.release(next);
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
        // 사유 문자열은 늘 비운다. 상태 코드가 없는 Close는 상대가 코드를 보내지 않았을 때의 답이다.
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

    /** 코덱이 푼 프레임을 메시지로 옮긴다. 조각은 파이프라인의 집계기가 이미 한 프레임으로 모았다. */
    private void collectFrames() {
        Object next = handshake.readInbound();
        while (next != null) {
            try {
                if (next instanceof BinaryWebSocketFrame) {
                    messages.add(WireMessage.binary(ByteBufUtil.getBytes(((BinaryWebSocketFrame) next).content())));
                } else if (next instanceof PingWebSocketFrame) {
                    // 이 계층의 Ping에 답은 하되 생존 확인(규격 3.4절)에는 쓰지 않는다. 엔진은 이 프레임을 보지 못한다.
                    if (!closeSent) {
                        encoder.writeOutbound(new PongWebSocketFrame(
                                ((PingWebSocketFrame) next).content().retainedSlice()));
                    }
                } else if (next instanceof PongWebSocketFrame) {
                    // 생존 확인으로 쓰지 않는다.
                } else if (next instanceof CloseWebSocketFrame) {
                    CloseWebSocketFrame close = (CloseWebSocketFrame) next;
                    // 상대가 먼저 닫으면 상대의 코드를 그대로 돌려준다. 앱에는 노출하지 않는다.
                    sendClose(close.statusCode());
                    messages.add(WireMessage.peerClosed());
                    broken = true;
                } else {
                    // 텍스트 메시지 등 바이트로 넘길 수 없는 입력.
                    messages.add(WireMessage.unsupported());
                    broken = true;
                }
            } finally {
                io.netty.util.ReferenceCountUtil.release(next);
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

    // ---------------------------------------------------------------- 업그레이드 핸들러

    private static final class ServerUpgrade extends ChannelInboundHandlerAdapter {
        private final java.util.function.Supplier<WebSocketWire> wire;
        private final String path;
        private final int maxMessageBytes;

        ServerUpgrade(java.util.function.Supplier<WebSocketWire> wire, String path, int maxMessageBytes) {
            this.wire = wire;
            this.path = path;
            this.maxMessageBytes = maxMessageBytes;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (!(message instanceof FullHttpRequest)) {
                io.netty.util.ReferenceCountUtil.release(message);
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

        @SuppressWarnings("FutureReturnValueIgnored") // EmbeddedChannel의 쓰기는 호출 안에서 끝난다.
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

        @SuppressWarnings("FutureReturnValueIgnored") // EmbeddedChannel의 쓰기는 호출 안에서 끝난다.
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
        private final java.util.function.Supplier<WebSocketWire> wire;
        private final WebSocketClientHandshaker handshaker;

        ClientUpgrade(java.util.function.Supplier<WebSocketWire> wire, WebSocketClientHandshaker handshaker) {
            this.wire = wire;
            this.handshaker = handshaker;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (!(message instanceof FullHttpResponse)) {
                io.netty.util.ReferenceCountUtil.release(message);
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
