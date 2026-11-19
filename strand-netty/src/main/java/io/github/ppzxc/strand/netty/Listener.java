package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.netty.internal.StrandServerChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;

/**
 * 서버의 리스닝. {@link #close}는 새 연결 수락만 멈춘다. 이미 맺어 등록한 준비 중·활성 연결은 유지한다. {@link #closed}는 리스닝 채널이 실제로 닫히면 완료하며
 * 기존 연결의 종료는 기다리지 않는다. 앱의 EventLoopGroup은 종료하지 않는다.
 */
public final class Listener {
    private final StrandServerChannel channel;
    private volatile @Nullable SocketAddress localAddress;
    private final AtomicBoolean settled = new AtomicBoolean();
    /** 앱이 관찰하는 정리 완료. 앱용 실행기를 거치지 않고 정리가 끝나는 곳에서 곧바로 완료한다. 앱이 취소할 수 없다. */
    private final Promise<Void> closed;
    /** 소유 객체가 내부 기록용으로 보는 정리 완료. */
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();

    /**
     * @param channel 리스닝 채널. 아직 등록·바인딩하지 않았을 수 있다
     * @param loop 앱이 관찰하는 {@link #closed}의 리스너를 실행할 EventLoop
     */
    Listener(StrandServerChannel channel, EventLoop loop) {
        this.channel = channel;
        this.closed = loop.newPromise();
        closed.setUncancellable();
        // 등록한 채널은 닫히고 선택기 등록이 풀린 뒤에 완료한다. 소켓을 닫아도 선택기에 등록돼 있으면 JDK가 포트를 바로 놓지 않는다.
        // 등록에 실패한 채널은 등록 해제 사건이 없으므로 소유자가 close()로 정리 완료를 확정한다.
        // 수락한 채널은 이 EventLoop에서 읽기 한 번 안에 등록되거나 정리되므로(Acceptor) 닫힘과 경합할 수 없다.
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                super.channelUnregistered(ctx);
                // 등록 해제와 같은 작업 안에서 선택 키가 취소됐다. 키를 비운 뒤 완료한다.
                ctx.executor().execute(() -> {
                    channel.releaseClosedSocket();
                    settle();
                });
            }
        });
    }

    /** 바인딩이 끝났다. 닫힌 뒤에도 주소를 조회할 수 있게 지금 보관한다. */
    void bound() {
        this.localAddress = channel.localAddress();
    }

    /**
     * 바인딩된 로컬 주소.
     *
     * @return 로컬 주소
     */
    public SocketAddress localAddress() {
        return Objects.requireNonNull(localAddress, "바인딩하기 전에는 앱에 보이지 않는다");
    }

    /** 새 연결 수락을 멈추고 리스닝 소켓을 닫는다. 호출 스레드를 막지 않는다. 반복해도 추가 효과가 없다. */
    @SuppressWarnings("FutureReturnValueIgnored") // 닫기 완료는 closed()와 채널의 closeFuture 리스너가 관찰한다.
    public void close() {
        if (channel.isRegistered()) {
            channel.close();
        } else {
            // 등록한 적 없는 채널은 closeFuture가 끝나지 않는다. 소켓을 직접 닫고 같은 정리 경로를 지난다.
            channel.unsafe().closeForcibly();
            settle();
        }
    }

    /**
     * 리스닝 소켓의 정리가 끝나면 완료하는 보존된 결과. 닫기 전에는 완료하지 않는다. 이미 등록된 준비 중·활성 연결의 종료는 기다리지 않는다. 앱용 실행기를 거치지 않으므로
     * 앱 실행기가 작업을 거절하거나 실행하지 않아도 완료한다. Future에 붙인 리스너는 Netty 계약에 따라 EventLoop에서 실행된다. 취소할 수 없다.
     *
     * @return 정리 완료
     */
    public Future<Void> closed() {
        return closed;
    }

    /** 소유 객체가 리스닝 정리 완료를 기록하는 내부 관찰. */
    CompletableFuture<Void> terminated() {
        return terminated;
    }

    private void settle() {
        if (settled.compareAndSet(false, true)) {
            terminated.complete(null);
            closed.trySuccess(null);
        }
    }
}
