package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.ConnectionListener;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 서버 역할로 받은 연결마다 연결 드라이버를 하나씩 만든다. 받은 요청에는 핸들러가 {@code echo:} 뒤에 본문을 붙여 {@code OK}로 응답한다. 클라이언트
 * 예제({@link BlockingTcpExample}, {@link NioTcpExample})의 상대로 쓴다. 사용법: {@code ServerExample [port]}
 */
public final class ServerExample {

    private ServerExample() {}

    public static void main(String[] args) throws Exception {
        Server server = start(Integer.parseInt(ExampleSupport.arg(args, 0, "9000")));
        System.out.println("대기 중 " + server.port());
        Thread.currentThread().join();
    }

    /**
     * 서버를 열고 연결을 받기 시작한다.
     *
     * @param port 받을 포트. 0이면 운영체제가 빈 포트를 고른다
     */
    static Server start(int port) throws IOException {
        return new Server(ServerSocketChannel.open().bind(new InetSocketAddress(port)));
    }

    /** 열려 있는 서버. 닫으면 받는 것을 멈추고 이어진 연결을 모두 끊는다. */
    static final class Server implements AutoCloseable {
        private final ServerSocketChannel listening;
        private final ExecutorService handlers = ExampleSupport.newHandlerExecutor("strand-server-handler");
        private final Set<ConnectionDriver> connections = ConcurrentHashMap.newKeySet();

        private Server(ServerSocketChannel listening) {
            this.listening = listening;
            Thread acceptor = new Thread(this::acceptConnections, "strand-server-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() {
            try {
                return ((InetSocketAddress) listening.getLocalAddress()).getPort();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private void acceptConnections() {
            try {
                while (true) {
                    serve(listening.accept());
                }
            } catch (IOException e) {
                // 서버가 닫혔다.
            }
        }

        private void serve(SocketChannel channel) throws IOException {
            // 연결이 start()가 돌아오기 전에 끝나 onClosed가 먼저 불릴 수 있다. 끝났다는 표시를 남기고, 등록한 뒤에 다시 확인해 어느 순서로
            // 와도 끝난 연결이 목록에 남지 않게 한다.
            AtomicReference<ConnectionDriver> started = new AtomicReference<>();
            AtomicBoolean ended = new AtomicBoolean();
            ConnectionDriver driver = ConnectionDriver.server(
                            ExampleSupport.withAuthenticator(EndpointConfig.defaults()), TcpPassage.of(channel))
                    .handler(handlers, request -> {
                        String body = new String(request.body(), StandardCharsets.UTF_8);
                        System.out.println("[서버] 요청 type=" + request.type() + " body=" + body);
                        request.respond(ExampleSupport.STATUS_OK, ("echo:" + body).getBytes(StandardCharsets.UTF_8));
                    })
                    .listener(new ConnectionListener() {
                        @Override
                        public void onClosed(ConnectionClosed event) {
                            System.out.println("[서버] 연결 종료 " + event.reason().kind());
                            ended.set(true);
                            ConnectionDriver self = started.get();
                            if (self != null) {
                                connections.remove(self);
                            }
                        }
                    })
                    .start();
            started.set(driver);
            connections.add(driver);
            if (ended.get()) {
                connections.remove(driver);
            }
        }

        @Override
        public void close() {
            try {
                listening.close();
            } catch (IOException e) {
                // 닫는 중의 오류는 할 수 있는 일이 없다.
            }
            connections.forEach(ConnectionDriver::close);
            handlers.shutdownNow();
        }
    }
}
