package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.TlsSizes;
import java.util.Objects;
import javax.net.ssl.SSLContext;

public final class NioMinimumProgress {
    private NioMinimumProgress() {}

    public static MinimumProgressSpace of(
            NioSecurity security, long maxFrameSize, long outgoingRequests, long incomingRequests) {
        Objects.requireNonNull(security, "security");
        SSLContext context = security.tlsContext();
        ProgressSpace space = ProgressSpace.derive(
                outgoingRequests, incomingRequests, maxFrameSize, context == null ? null : TlsSizes.of(context));
        return space.toApi();
    }
}
