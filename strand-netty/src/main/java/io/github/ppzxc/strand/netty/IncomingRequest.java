package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Extensions;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ResultCompletion;
import java.util.Objects;

public final class IncomingRequest {
    private final ConnectionEngine engine;
    private final NettyStrandConnection connection;
    private final IncomingState state;
    private final int type;
    private final byte[] payload;
    private final Extensions extensions;
    private final ResultCompletion<RequestEnd> ended;

    IncomingRequest(
            ConnectionEngine engine,
            NettyStrandConnection connection,
            IncomingState state,
            int type,
            Extensions extensions,
            byte[] payload,
            ResultCompletion<RequestEnd> ended) {
        this.engine = engine;
        this.connection = connection;
        this.state = state;
        this.type = type;
        this.extensions = extensions;
        this.payload = payload;
        this.ended = ended;
    }

    public int type() {
        return type;
    }

    public byte[] payload() {
        return payload;
    }

    public Extensions extensions() {
        return extensions;
    }

    public NettyStrandConnection connection() {
        return connection;
    }

    public Result<ResponseAcceptance> respond(int status, byte[] body) {
        if (!io.github.ppzxc.strand.core.internal.Status.isValid(status)) {
            throw new IllegalArgumentException("status는 0~16이어야 한다: " + status);
        }
        return engine.respond(this, status, Objects.requireNonNull(body, "body"));
    }

    public Result<RequestEnd> ended() {
        return ended;
    }

    IncomingState state() {
        return state;
    }
}
