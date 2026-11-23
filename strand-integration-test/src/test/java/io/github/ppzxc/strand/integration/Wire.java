package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * nio와 netty 제품 공개 API를 같은 통합 시나리오로 돌리기 위한 시험 전용 중립 모양. 전송별 타입(서버·클라이언트·연결·받은 요청)만 감싸고 동작은 그대로 위임한다. 제품 API가 아니다.
 * 구현은 {@link NioWire}와 {@link NettyWire}다.
 */
interface Wire {

    /** 서버를 포트 0에 바인딩해 시작한다. 인증을 통과한 연결마다 {@code handlers}가 서버 쪽 핸들러를 돌려준다. */
    Server startServer(Function<Conn, Handler> handlers) throws Exception;

    /** 클라이언트를 만든다. {@code idlePingInterval}이 null이면 기본 간격이다. */
    Client newClient(Handler handler, @Nullable Duration idlePingInterval);

    /**
     * 시험 전용 통로 감싸기 지점({@code PassageWrapping})으로 감싼 통로 중 아직 가져가지 않은 것. 이 지점은 nio 엔진에만 있어서 netty는 항상 null이다.
     *
     * @param server 서버 쪽 통로인가
     */
    @Nullable
    ControlledPassage claimPassage(boolean server);

    /** 앱이 받은 요청. */
    interface Incoming {
        int type();

        byte[] payload();

        Result<ResponseAcceptance> respond(int status, byte[] body);
    }

    /** 받은 요청 핸들러. */
    interface Handler {
        void handle(Incoming request) throws Exception;
    }

    /** 앱에 보이는 연결 하나. */
    interface Conn {
        RequestHandle request(Request request);

        RequestHandle request(Request request, RequestOptions options);

        void close();

        void abort();

        Result<CloseNotice> closed();

        OptionalLong credentialLifetime();
    }

    interface Server {
        int port();

        ResourceUsage usage(Resource resource);

        /** 수락을 멈춘다. */
        void closeListener();

        CompletableFuture<Void> shutdown();
    }

    interface Client {
        CompletableFuture<Conn> connect(InetSocketAddress address);

        CompletableFuture<Void> shutdown();
    }
}
