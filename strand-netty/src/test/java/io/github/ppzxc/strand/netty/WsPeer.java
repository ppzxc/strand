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

/**
 * 실제 소켓 위에서 WebSocket 업그레이드를 직접 하고, 맞은편에서 core 클라이언트 {@link Endpoint}를 메시지 통로로 굴리는 시험 상대. 프레임은 {@link RawWebSocket}으로 직접 읽고
 * 쓰므로 제품 코덱과 독립이다. 시험은 임의의 프레임(텍스트, 조각, 거짓 길이)을 {@link #sendRaw}로 보낼 수 있다.
 */
final class WsPeer implements AutoCloseable {
    private final Socket socket;
    private final InputStream in;
    private final EndpointDriver driver;
    private final List<Event> events = new CopyOnWriteArrayList<>();
    private final List<RawWebSocket.Frame> controlFrames = new CopyOnWriteArrayList<>();
    private final List<RawWebSocket.Frame> textFrames = new CopyOnWriteArrayList<>();
    private final List<byte[]> binaryMessages = new CopyOnWriteArrayList<>();
    /** 받은 프레임을 도착한 순서대로 적은 기록: {@code BINARY}, {@code TEXT}, {@code CLOSE:<코드>}, {@code PING}, {@code PONG}. */
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

    /** 경로로 업그레이드하고(하위 프로토콜 {@code strand}) 101을 받은 뒤 상대를 시작한다. 연결 서문과 HELLO를 보낸다. */
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
            // 어느 쪽이 끊겼다.
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

    /** 이후 받은 메시지에 Endpoint가 답하지 않는다(상대가 응답하지 않는 연결을 만든다). */
    void goSilent() {
        silent = true;
    }

    /** 받은 프레임을 도착한 순서대로. */
    List<String> arrivals() {
        return new ArrayList<>(arrivals);
    }

    void sendPing(byte[] payload) throws IOException {
        sendRaw(RawWebSocket.encode(RawWebSocket.PING, true, payload, true));
    }

    /** Endpoint가 내려는 출력을 메시지 하나씩 바이너리 프레임으로 보낸다. */
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

    /** Endpoint를 부르고 출력을 보낸다. */
    synchronized void run(java.util.function.Consumer<Endpoint> action) {
        action.accept(driver.endpoint());
        pump();
    }

    /** Endpoint를 부르고 나온 출력을 보내지 않고 메시지 바이트로 돌려준다(시험이 변형해서 보내려는 용도). */
    synchronized List<byte[]> runDetached(java.util.function.Consumer<Endpoint> action) {
        action.accept(driver.endpoint());
        List<byte[]> captured = new ArrayList<>();
        for (OutboundMessage message : driver.flush()) {
            captured.add(message.bytes());
        }
        events.addAll(driver.takeEvents());
        return captured;
    }

    /** 소켓에 바이트를 그대로 쓴다. */
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

    /** 메시지를 {@code parts}개 조각(바이너리 + 연속 프레임)으로 나눠 보낸다. */
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

    /** 연결 시험에서 기다리는 조건. */
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
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }

    @Override
    public void close() {
        disconnect();
    }
}
