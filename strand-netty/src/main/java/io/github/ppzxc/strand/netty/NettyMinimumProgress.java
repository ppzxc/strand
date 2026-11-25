package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.TlsSizes;
import java.util.Objects;
import javax.net.ssl.SSLContext;

public final class NettyMinimumProgress {
    private NettyMinimumProgress() {}

    public static MinimumProgressSpace of(
            NettySecurity security, long maxFrameSize, long outgoingRequests, long incomingRequests) {
        Objects.requireNonNull(security, "security");
        SSLContext context = security.tlsContext();
        ProgressSpace space = ProgressSpace.derive(
                outgoingRequests, incomingRequests, maxFrameSize, context == null ? null : TlsSizes.of(context));
        return space.toApi();
    }
}
