package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.netty.internal.MessageWire;
import io.github.ppzxc.strand.netty.internal.WebSocketStatus;
import io.github.ppzxc.strand.netty.internal.WebSocketWire;
import io.github.ppzxc.strand.netty.internal.WireMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocket13FrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 적합성 {@code netty} 경로의 제품 통로. 제품 WebSocket 통로({@link WebSocketWire}: Netty의 WebSocket 13 코덱과 조각 집계기)를 {@link EmbeddedChannel}에 얹고,
 * 러너의 {@code recv_message}를 {@code BinaryWebSocketFrame}/{@code TextWebSocketFrame}으로 인코딩해 넣고, 엔드포인트가 낸 메시지를 제품 통로로 보내 나온 프레임
 * ({@code CloseWebSocketFrame} 포함)을 풀어 비교에 쓴다. 업그레이드는 마친 상태에서 시작한다(벡터는 업그레이드 뒤의 메시지 통로를 다룬다).
 *
 * <p>종료 원인에 대응하는 Close 상태 코드는 제품 규칙({@link WebSocketStatus})이다. 벡터 형식 2는 1000만 관측하므로 그 밖의 상태는 비교에서 뺀다.
 */
final class NettyMessageWireHarness implements Transmission {
    private static final int RECEIVE_LIMIT = 1 << 26;

    /** netty 경로: {@code transport: websocket} 벡터만 실행하고 WebSocket Close까지 관측한다. */
    static final VectorRunner.Medium MEDIUM = new VectorRunner.Medium() {
        @Override
        public List<String> unsupportedSetup(JsonObject setup) {
            boolean webSocket = setup.has("transport")
                    && VectorSchema.WEBSOCKET.equals(setup.get("transport").getAsString());
            return webSocket ? Collections.<String>emptyList() : Collections.singletonList("setup.transport");
        }

        @Override
        public boolean carriesFrame(JsonObject frame) {
            return true;
        }

        @Override
        public Transmission open(boolean client, boolean webSocket, int maxFrameSize) {
            return new NettyMessageWireHarness(client, maxFrameSize);
        }
    };

    private final MessageWire wire;
    /** 상대가 엔드포인트에 보내는 프레임을 만든다: 상대가 클라이언트(엔드포인트가 서버)일 때만 마스킹한다. */
    private final EmbeddedChannel peerEncoder;
    /** 엔드포인트가 보낸 프레임을 푼다: 엔드포인트가 클라이언트이면 마스킹을 기대한다. */
    private final EmbeddedChannel peerDecoder;

    private final List<Integer> closeStatuses = new ArrayList<>();

    NettyMessageWireHarness(boolean endpointIsClient, int maxMessageBytes) {
        this.wire = WebSocketWire.established(endpointIsClient, maxMessageBytes);
        this.peerEncoder = new EmbeddedChannel(new WebSocket13FrameEncoder(!endpointIsClient));
        this.peerDecoder = new EmbeddedChannel(new WebSocket13FrameDecoder(WebSocketDecoderConfig.newBuilder()
                .expectMaskedFrames(endpointIsClient)
                .allowMaskMismatch(false)
                .maxFramePayloadLength(RECEIVE_LIMIT)
                .closeOnProtocolViolation(false)
                .build()));
    }

    @Override
    public void deliver(EndpointDriver driver, byte[] bytes) {
        inbound(driver, true, bytes);
    }

    @Override
    public void apply(EndpointDriver driver, JsonObject step, Map<String, Long> requestTickets) {
        if (StepKind.of(step).inbound() == StepKind.Inbound.MESSAGE) {
            // 메시지 하나를 제품 통로에 프레임으로 넣는다. 통로가 core에 단위 입력 또는 "받았다" 신호로 넘긴다.
            inbound(
                    driver,
                    step.get("opcode").getAsString().equals("binary"),
                    Bytes.fromHex(step.get("hex").getAsString()));
            return;
        }
        StepKind.of(step).execute(driver, step, requestTickets);
    }

    @Override
    public List<String> observeAfter(List<Event> events) {
        return closesNormally(events)
                ? Collections.singletonList(FrameKind.webSocketClose(Protocol.NORMAL_CLOSURE))
                : Collections.<String>emptyList();
    }

    /** 상대가 메시지 하나를 보낸다. 통로가 프레임을 풀어 core에 단위 입력으로, 바이트로 넘길 수 없는 입력은 "받았다" 신호로 넣는다. */
    void inbound(EndpointDriver driver, boolean binary, byte[] payload) {
        ByteBuf content = Unpooled.wrappedBuffer(payload);
        WebSocketFrame frame = binary ? new BinaryWebSocketFrame(content) : new TextWebSocketFrame(content);
        assertThat(peerEncoder.writeOutbound(frame)).as("메시지 프레임 인코딩").isTrue();
        try {
            wire.feed(readAll(peerEncoder));
        } catch (java.io.IOException e) {
            throw new AssertionError("업그레이드를 마친 통로가 입력을 거절했다", e);
        }
        WireMessage message = wire.pollMessage();
        while (message != null) {
            switch (message.kind()) {
                case BINARY:
                    driver.receiveUnit(message.bytes());
                    break;
                case UNSUPPORTED:
                    driver.receiveUnsupportedInput();
                    break;
                default:
                    driver.endpoint().disconnect();
                    driver.settle();
                    break;
            }
            message = wire.pollMessage();
        }
    }

    /**
     * 엔드포인트가 낸 메시지를 제품 통로로 보내고, 통로가 내보낸 바이트를 프레임으로 풀어 바이너리 메시지의 내용을 돌려준다. Close 프레임은 {@link #closeStatuses}에 쌓는다.
     */
    @Override
    public List<byte[]> transmit(List<OutboundMessage> messages) {
        try {
            for (OutboundMessage message : messages) {
                wire.sendMessage(message.bytes());
            }
        } catch (java.io.IOException e) {
            throw new AssertionError("통로가 엔드포인트의 메시지를 보내지 못했다", e);
        }
        return decodeWire();
    }

    /**
     * 연결이 끝났으면 제품 규칙({@link WebSocketStatus})대로 WebSocket Close를 보내고, 나간 Close 가운데 1000이 있는지 돌려준다. 보낼 Close가 없거나(전송 계층이 끊겨 끝난
     * 연결) 1000이 아니면 false다.
     */
    boolean closesNormally(List<Event> events) {
        for (Event event : events) {
            if (event.kind() == Event.Kind.CLOSED) {
                int status = WebSocketStatus.of(event.closeReason().get());
                if (status != WebSocketStatus.NONE) {
                    wire.sendClose(status);
                }
            }
        }
        decodeWire();
        return closeStatuses.contains(1000);
    }

    /** 통로가 내보낸 바이트를 프레임으로 푼다. */
    private List<byte[]> decodeWire() {
        List<byte[]> binaries = new ArrayList<>();
        byte[] out = wire.pollWire();
        while (out != null) {
            peerDecoder.writeInbound(Unpooled.wrappedBuffer(out));
            Object next = peerDecoder.readInbound();
            while (next != null) {
                try {
                    if (next instanceof BinaryWebSocketFrame) {
                        binaries.add(ByteBufUtil.getBytes(((BinaryWebSocketFrame) next).content()));
                    } else if (next instanceof CloseWebSocketFrame) {
                        CloseWebSocketFrame close = (CloseWebSocketFrame) next;
                        closeStatuses.add(close.statusCode());
                        assertThat(close.reasonText()).as("Close 사유는 늘 비어 있다").isEmpty();
                    } else {
                        throw new AssertionError("예상하지 못한 프레임: " + next);
                    }
                } finally {
                    ReferenceCountUtil.release(next);
                }
                next = peerDecoder.readInbound();
            }
            out = wire.pollWire();
        }
        return binaries;
    }

    private static byte[] readAll(EmbeddedChannel channel) {
        ByteBuf all = Unpooled.buffer();
        try {
            Object next = channel.readOutbound();
            while (next != null) {
                ByteBuf buffer = (ByteBuf) next;
                all.writeBytes(buffer);
                buffer.release();
                next = channel.readOutbound();
            }
            return ByteBufUtil.getBytes(all);
        } finally {
            all.release();
        }
    }
}
