package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Observation;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/** 요청 결과를 그대로 위임하며 그 연결의 EventLoop를 안다. {@link NettyResults#toFuture}가 앱에 EventLoop를 요구하지 않게 한다. */
final class LoopBoundResult implements Result<RequestResult> {
    private final ResultCompletion<RequestResult> delegate;
    private final EventLoop loop;

    LoopBoundResult(ResultCompletion<RequestResult> delegate, EventLoop loop) {
        this.delegate = delegate;
        this.loop = loop;
    }

    Future<RequestResult> toFuture() {
        ConversionPromise promise = new ConversionPromise(loop);
        try {
            promise.listening = delegate.listen(promise::trySuccess);
        } catch (Throwable error) {
            promise.tryFailure(error);
        }
        return promise;
    }

    /** 취소는 이 Future만 취소하고 등록 예약을 돌려준다. 해제를 EventLoop 리스너에 맡기지 않아 실행 거절이 예약을 붙잡지 않는다. */
    private static final class ConversionPromise extends DefaultPromise<RequestResult> {
        volatile ResultCompletion.Listening listening = () -> {};

        ConversionPromise(EventLoop loop) {
            super(loop);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) listening.cancel();
            return cancelled;
        }
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public RequestResult resultNow() {
        return delegate.resultNow();
    }

    @Override
    public Observation observe(Consumer<? super RequestResult> observer) {
        return delegate.observe(observer);
    }

    @Override
    public Observation observe(Executor executor, Consumer<? super RequestResult> observer) {
        return delegate.observe(executor, observer);
    }

    @Override
    public CompletionStage<RequestResult> toCompletionStage(Executor executor) {
        return delegate.toCompletionStage(executor);
    }
}
