package io.github.ppzxc.strand.netty;

import java.util.Objects;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.Nullable;

public final class NettySecurity {
    private final @Nullable SSLContext tlsContext;

    private NettySecurity(@Nullable SSLContext tlsContext) {
        this.tlsContext = tlsContext;
    }

    public static NettySecurity tls(SSLContext context) {
        return new NettySecurity(Objects.requireNonNull(context, "context"));
    }

    public static NettySecurity externallySecured() {
        return new NettySecurity(null);
    }

    @Nullable
    SSLContext tlsContext() {
        return tlsContext;
    }
}
