package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Result;
import io.netty.util.concurrent.Future;
import java.util.Objects;

public final class NettyResults {
    private NettyResults() {}

    public static Future<RequestResult> toFuture(Result<RequestResult> result) {
        Objects.requireNonNull(result);
        if (!(result instanceof LoopBoundResult)) {
            throw new IllegalArgumentException("not a result of a Netty strand request");
        }
        return ((LoopBoundResult) result).toFuture();
    }
}
