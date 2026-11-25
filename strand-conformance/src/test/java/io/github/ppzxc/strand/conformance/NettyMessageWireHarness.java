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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

final class NettyMessageWireHarness implements Transmission {
    private static final int RECEIVE_LIMIT = 1 << 26;

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

    private final EmbeddedChannel peerEncoder;

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
        StepKind kind = StepKind.of(step);
        if (kind.inbound() == StepKind.Inbound.MESSAGE) {

            inbound(
                    driver,
                    step.get("opcode").getAsString().equals("binary"),
                    Bytes.fromHex(step.get("hex").getAsString()));
            return;
        }
        kind.execute(driver, step, requestTickets);
    }

    @Override
    public List<String> observeAfter(List<Event> events) {
        return closesNormally(events)
                ? Collections.singletonList(FrameKind.webSocketClose(Protocol.NORMAL_CLOSURE))
                : Collections.<String>emptyList();
    }

    void inbound(EndpointDriver driver, boolean binary, byte[] payload) {
        ByteBuf content = Unpooled.wrappedBuffer(payload);
        WebSocketFrame frame = binary ? new BinaryWebSocketFrame(content) : new TextWebSocketFrame(content);
        assertThat(peerEncoder.writeOutbound(frame)).as("메시지 프레임 인코딩").isTrue();
        try {
            wire.feed(readAll(peerEncoder));
        } catch (IOException e) {
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

    @Override
    public List<byte[]> transmit(List<OutboundMessage> messages) {
        try {
            for (OutboundMessage message : messages) {
                wire.sendMessage(message.bytes());
            }
        } catch (IOException e) {
            throw new AssertionError("통로가 엔드포인트의 메시지를 보내지 못했다", e);
        }
        return decodeWire();
    }

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
