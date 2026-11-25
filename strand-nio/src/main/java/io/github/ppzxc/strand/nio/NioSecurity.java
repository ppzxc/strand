package io.github.ppzxc.strand.nio;

import java.util.Objects;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.Nullable;

public final class NioSecurity {
    private final @Nullable SSLContext tlsContext;

    private NioSecurity(@Nullable SSLContext tlsContext) {
        this.tlsContext = tlsContext;
    }

    public static NioSecurity tls(SSLContext context) {
        return new NioSecurity(Objects.requireNonNull(context, "context"));
    }

    public static NioSecurity externallySecured() {
        return new NioSecurity(null);
    }

    @Nullable
    SSLContext tlsContext() {
        return tlsContext;
    }
}
