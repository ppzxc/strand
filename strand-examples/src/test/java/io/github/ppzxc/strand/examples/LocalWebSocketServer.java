package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.Passage;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 실제 소켓 위에서 {@link WebSocketPassage}를 시험하려고 시험 안에 둔 최소 WebSocket 서버(RFC 6455). JDK에는 서버가 없고 새 의존성은 쓸 수
 * 없다. HTTP Upgrade 핸드셰이크를 하고, 클라이언트가 마스킹해 보낸 프레임을 읽고, 서버 프레임은 마스킹 없이 쓴다. 포트는 0(임시 포트)이다.
 */
final class LocalWebSocketServer implements AutoCloseable {

    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    /** 연결마다 핸드셰이크 뒤에 불린다. */
    interface SessionHandler {
        void handle(Session session) throws Exception;
    }

    /** 프레임 하나. */
    static final class Frame {
        final int opcode;
        final boolean fin;
        final byte[] payload;

        Frame(int opcode, boolean fin, byte[] payload) {
            this.opcode = opcode;
            this.fin = fin;
            this.payload = payload;
        }
    }

    /** 핸드셰이크를 마친 연결 하나. */
    static final class Session {
        private final Socket socket;
        private final DataInputStream in;
        private final OutputStream out;

        Session(Socket socket, InputStream in, OutputStream out) {
            this.socket = socket;
            this.in = new DataInputStream(in);
            this.out = out;
        }

        /** 클라이언트가 마스킹해 보낸 프레임 하나를 읽고 마스크를 푼다. */
        Frame readFrame() throws IOException {
            int first = in.readUnsignedByte();
            int second = in.readUnsignedByte();
            long length = second & 0x7F;
            if (length == 126) {
                length = in.readUnsignedShort();
            } else if (length == 127) {
                length = in.readLong();
            }
            byte[] mask = new byte[4];
            if ((second & 0x80) != 0) {
                in.readFully(mask);
            }
            byte[] payload = new byte[(int) length];
            in.readFully(payload);
            if ((second & 0x80) != 0) {
                for (int i = 0; i < payload.length; i++) {
                    payload[i] ^= mask[i % 4];
                }
            }
            return new Frame(first & 0x0F, (first & 0x80) != 0, payload);
        }

        /** 마스킹하지 않은 서버 프레임 하나를 쓴다. */
        synchronized void send(int opcode, boolean fin, byte[] payload) throws IOException {
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            frame.write((fin ? 0x80 : 0) | opcode);
            if (payload.length < 126) {
                frame.write(payload.length);
            } else if (payload.length <= 0xFFFF) {
                frame.write(126);
                frame.write(payload.length >> 8);
                frame.write(payload.length);
            } else {
                frame.write(127);
                for (int shift = 56; shift >= 0; shift -= 8) {
                    frame.write((int) ((long) payload.length >> shift));
                }
            }
            frame.write(payload, 0, payload.length);
            out.write(frame.toByteArray());
            out.flush();
        }

        void close() {
            try {
                socket.close();
            } catch (IOException e) {
                // 정리 중의 오류는 할 수 있는 일이 없다.
            }
        }
    }

    private final ServerSocket listening;
    private final boolean chooseSubprotocol;
    private final SessionHandler handler;
    private final List<Session> sessions = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private LocalWebSocketServer(ServerSocket listening, boolean chooseSubprotocol, SessionHandler handler) {
        this.listening = listening;
        this.chooseSubprotocol = chooseSubprotocol;
        this.handler = handler;
    }

    /**
     * 서버를 열고 연결을 받기 시작한다.
     *
     * @param chooseSubprotocol 클라이언트가 제시한 하위 프로토콜 {@code strand}를 응답에 고를지
     */
    static LocalWebSocketServer start(boolean chooseSubprotocol, SessionHandler handler) throws IOException {
        ServerSocket listening = new ServerSocket();
        listening.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        LocalWebSocketServer server = new LocalWebSocketServer(listening, chooseSubprotocol, handler);
        Thread acceptor = new Thread(server::acceptConnections, "local-websocket-accept");
        acceptor.setDaemon(true);
        acceptor.start();
        return server;
    }

    URI uri() {
        return URI.create("ws://127.0.0.1:" + listening.getLocalPort() + "/");
    }

    private void acceptConnections() {
        while (!closed.get()) {
            try {
                Socket socket = listening.accept();
                Thread thread = new Thread(() -> serve(socket), "local-websocket-session");
                thread.setDaemon(true);
                thread.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(Socket socket) {
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            String key = null;
            String request = readHeaders(in);
            for (String line : request.split("\r\n")) {
                if (line.toLowerCase(Locale.ROOT).startsWith("sec-websocket-key:")) {
                    key = line.substring(line.indexOf(':') + 1).trim();
                }
            }
            if (key == null) {
                socket.close();
                return;
            }
            String response = "HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + accept(key) + "\r\n"
                    + (chooseSubprotocol ? "Sec-WebSocket-Protocol: strand\r\n" : "")
                    + "\r\n";
            out.write(response.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Session session = new Session(socket, in, out);
            sessions.add(session);
            handler.handle(session);
        } catch (Exception e) {
            // 시험이 닫았거나 클라이언트가 끊었다.
        }
    }

    private static String readHeaders(InputStream in) throws IOException {
        StringBuilder headers = new StringBuilder();
        while (!headers.toString().endsWith("\r\n\r\n")) {
            int next = in.read();
            if (next < 0) {
                throw new EOFException();
            }
            headers.append((char) next);
        }
        return headers.toString();
    }

    private static String accept(String key) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            return Base64.getEncoder().encodeToString(sha1.digest((key + GUID).getBytes(StandardCharsets.US_ASCII)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        closed.set(true);
        try {
            listening.close();
        } catch (IOException e) {
            // 정리 중의 오류는 할 수 있는 일이 없다.
        }
        sessions.forEach(Session::close);
    }

    /**
     * 서버 쪽 연결 드라이버가 쓰는 통로. 받은 WebSocket 바이너리 프레임을 메시지로 올리고 보낼 메시지를 프레임으로 쓴다. 클라이언트의 Close에는 Close로
     * 응답한다.
     */
    static final class SessionPassage implements Passage {
        private final Session session;
        private final LinkedBlockingQueue<Inbound> queue = new LinkedBlockingQueue<>();
        private boolean ended;

        SessionPassage(Session session) {
            this.session = session;
            Thread reader = new Thread(this::readFrames, "local-websocket-reader");
            reader.setDaemon(true);
            reader.start();
        }

        private void readFrames() {
            try {
                while (true) {
                    Frame frame = session.readFrame();
                    if (frame.opcode == 2) {
                        queue.add(Inbound.message(WebSocketMessageKind.BINARY, frame.payload));
                    } else if (frame.opcode == 8) {
                        try {
                            session.send(8, true, new byte[] {0x03, (byte) 0xE8});
                        } catch (IOException e) {
                            // 이미 끊겼다. 끝남으로 알린다.
                        }
                        break;
                    }
                }
            } catch (IOException e) {
                // 끊김은 끝남으로 알린다.
            }
            queue.add(Inbound.end());
        }

        @Override
        public Inbound await(long timeoutMs) throws IOException {
            if (ended) {
                return Inbound.end();
            }
            Inbound inbound;
            try {
                inbound = timeoutMs == 0
                        ? queue.poll()
                        : timeoutMs < 0 ? queue.take() : queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            if (inbound == null) {
                return Inbound.none();
            }
            ended = inbound.kind() == Inbound.Kind.END;
            return inbound;
        }

        @Override
        public void write(OutboundMessage message) throws IOException {
            if (message.kind() == OutboundMessage.Kind.BINARY) {
                session.send(2, true, message.bytes());
            } else {
                int code = message.webSocketCloseCode();
                session.send(8, true, new byte[] {(byte) (code >> 8), (byte) code});
            }
        }

        @Override
        public void wakeup() {
            queue.add(Inbound.none());
        }

        @Override
        public void close() {
            session.close();
        }
    }
}
