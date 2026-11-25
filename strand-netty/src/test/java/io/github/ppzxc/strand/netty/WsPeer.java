package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

final class WsPeer implements AutoCloseable {
    private final Socket socket;
    private final InputStream in;
    private final EndpointDriver driver;
    private final List<Event> events = new CopyOnWriteArrayList<>();
    private final List<RawWebSocket.Frame> controlFrames = new CopyOnWriteArrayList<>();
    private final List<RawWebSocket.Frame> textFrames = new CopyOnWriteArrayList<>();
    private final List<byte[]> binaryMessages = new CopyOnWriteArrayList<>();
    private final List<String> arrivals = new CopyOnWriteArrayList<>();

    private volatile boolean ended;
    private volatile boolean silent;
    private final boolean autoPump;

    private WsPeer(Socket socket, InputStream in, EndpointConfig config, boolean autoPump) {
        this.socket = socket;
        this.in = in;
        this.autoPump = autoPump;
        this.driver = EndpointDriver.of(Endpoint.client(config))
                .anonymousOnly(OptionalLong.empty())
                .space(CollectionSpace.of(1 << 16, 1 << 16))
                .eventBatch(1024)
                .eagerEvents(true)
                .messageChannel(true);
        Thread thread = new Thread(this::readLoop, "ws-peer");
        thread.setDaemon(true);
        thread.start();
    }

    static WsPeer connect(int port, String path) throws Exception {
        return connect(port, path, EndpointConfig.defaults().maxConcurrentStreams(8), true);
    }

    static WsPeer connect(int port, String path, EndpointConfig config, boolean sendPreface) throws Exception {
        Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
        socket.setTcpNoDelay(true);
        String key = RawWebSocket.newKey();
        socket.getOutputStream()
                .write(RawWebSocket.upgradeRequest(path, "localhost:" + port, key, "strand")
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        InputStream in = socket.getInputStream();
        String response = RawWebSocket.readHeaders(in);
        if (!response.startsWith("HTTP/1.1 101")
                || !RawWebSocket.acceptFor(key).equals(RawWebSocket.header(response, "sec-websocket-accept"))
                || !"strand".equals(RawWebSocket.header(response, "sec-websocket-protocol"))) {
            socket.close();
            throw new AssertionError("업그레이드가 기대와 다르다:\n" + response);
        }
        WsPeer peer = new WsPeer(socket, in, config, sendPreface);
        if (sendPreface) {
            peer.pump();
        }
        return peer;
    }

    private void readLoop() {
        try {
            for (; ; ) {
                RawWebSocket.Frame frame = RawWebSocket.read(in);
                arrivals.add(describe(frame));
                switch (frame.opcode) {
                    case RawWebSocket.BINARY:
                        binaryMessages.add(frame.payload);
                        if (autoPump && !silent) {
                            synchronized (this) {
                                driver.receiveUnit(frame.payload);
                                events.addAll(driver.takeEvents());
                                pump();
                            }
                        }
                        break;
                    case RawWebSocket.TEXT:
                        textFrames.add(frame);
                        break;
                    default:
                        controlFrames.add(frame);
                        break;
                }
            }
        } catch (IOException e) {
        }
        ended = true;
    }

    private static String describe(RawWebSocket.Frame frame) {
        switch (frame.opcode) {
            case RawWebSocket.BINARY:
                return "BINARY";
            case RawWebSocket.TEXT:
                return "TEXT";
            case RawWebSocket.CLOSE:
                return "CLOSE:" + frame.closeStatus();
            case RawWebSocket.PING:
                return "PING";
            case RawWebSocket.PONG:
                return "PONG";
            default:
                return "OPCODE:" + frame.opcode;
        }
    }

    void goSilent() {
        silent = true;
    }

    List<String> arrivals() {
        return new ArrayList<>(arrivals);
    }

    void sendPing(byte[] payload) throws IOException {
        sendRaw(RawWebSocket.encode(RawWebSocket.PING, true, payload, true));
    }

    synchronized void pump() {
        try {
            for (OutboundMessage message : driver.flush()) {
                RawWebSocket.write(
                        socket.getOutputStream(),
                        RawWebSocket.encode(RawWebSocket.BINARY, true, message.bytes(), true));
            }
        } catch (IOException e) {
            ended = true;
        }
        events.addAll(driver.takeEvents());
    }

    synchronized void run(java.util.function.Consumer<Endpoint> action) {
        action.accept(driver.endpoint());
        pump();
    }

    synchronized List<byte[]> runDetached(java.util.function.Consumer<Endpoint> action) {
        action.accept(driver.endpoint());
        List<byte[]> captured = new ArrayList<>();
        for (OutboundMessage message : driver.flush()) {
            captured.add(message.bytes());
        }
        events.addAll(driver.takeEvents());
        return captured;
    }

    synchronized void sendRaw(byte[] bytes) throws IOException {
        RawWebSocket.write(socket.getOutputStream(), bytes);
    }

    void sendBinary(byte[] payload) throws IOException {
        sendRaw(RawWebSocket.encode(RawWebSocket.BINARY, true, payload, true));
    }

    void sendText(String text) throws IOException {
        sendRaw(RawWebSocket.encode(
                RawWebSocket.TEXT, true, text.getBytes(java.nio.charset.StandardCharsets.UTF_8), true));
    }

    void sendClose(int status) throws IOException {
        sendRaw(RawWebSocket.encode(RawWebSocket.CLOSE, true, RawWebSocket.closePayload(status), true));
    }

    void sendFragmented(byte[] payload, int parts) throws IOException {
        int size = (payload.length + parts - 1) / parts;
        for (int i = 0; i < parts; i++) {
            int from = i * size;
            int to = Math.min(payload.length, from + size);
            byte[] piece = java.util.Arrays.copyOfRange(payload, from, to);
            boolean last = i == parts - 1;
            sendRaw(RawWebSocket.encode(i == 0 ? RawWebSocket.BINARY : RawWebSocket.CONTINUATION, last, piece, true));
        }
    }

    List<Event> events() {
        return new ArrayList<>(events);
    }

    List<RawWebSocket.Frame> closes() {
        List<RawWebSocket.Frame> closes = new ArrayList<>();
        for (RawWebSocket.Frame frame : controlFrames) {
            if (frame.opcode == RawWebSocket.CLOSE) {
                closes.add(frame);
            }
        }
        return closes;
    }

    List<RawWebSocket.Frame> pongs() {
        List<RawWebSocket.Frame> pongs = new ArrayList<>();
        for (RawWebSocket.Frame frame : controlFrames) {
            if (frame.opcode == RawWebSocket.PONG) {
                pongs.add(frame);
            }
        }
        return pongs;
    }

    List<byte[]> binaryMessages() {
        return new ArrayList<>(binaryMessages);
    }

    List<RawWebSocket.Frame> textFrames() {
        return new ArrayList<>(textFrames);
    }

    boolean ended() {
        return ended;
    }

    Endpoint endpoint() {
        return driver.endpoint();
    }

    boolean sawClosed() {
        return events().stream().anyMatch(event -> event.kind() == Event.Kind.CLOSED);
    }

    static void await(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("기다리던 조건이 15초 안에 이뤄지지 않았다");
            }
            Thread.sleep(5);
        }
    }

    void disconnect() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() {
        disconnect();
    }
}
