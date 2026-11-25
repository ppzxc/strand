package io.github.ppzxc.strand.integration.app;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ShutdownResult;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

final class App {
    private App() {}

    interface Conn {
        RequestHandle request(Request request, RequestOptions options);

        void close();

        void abort();

        void expireCredential();

        void revokeCredential();

        void closeExcessiveUse();

        Result<CloseNotice> closed();

        ResourceUsage usage(Resource resource);

        Object principal();

        OptionalLong credentialLifetime();
    }

    interface In {
        int type();

        byte[] payload();

        Result<ResponseAcceptance> respond(int status, byte[] body);

        Result<RequestEnd> ended();

        Conn connection();
    }

    interface Handler {
        void handle(In request) throws Exception;
    }

    static final class Prepared {
        final Handler handler;
        final Runnable disposal;

        Prepared(Handler handler, Runnable disposal) {
            this.handler = handler;
            this.disposal = disposal;
        }
    }

    interface Factory {
        Prepared create(Conn connection) throws Exception;
    }

    interface AuthCheck {
        Object verify(String mechanism) throws Exception;
    }

    interface Lsn {
        InetSocketAddress address();

        void close();

        CompletableFuture<Void> closed();
    }

    interface Unreleased {
        Optional<Prepared> claim();
    }

    interface Server {
        CompletableFuture<Lsn> listen();

        CompletableFuture<Void> shutdown();

        Result<ShutdownResult> shutdownResult();

        CompletableFuture<Void> resourcesTerminated();

        List<Unreleased> unreleased();

        ResourceUsage usage(Resource resource);
    }

    interface Attempt {
        CompletableFuture<Conn> result();

        boolean cancel();

        CompletableFuture<Void> transportReleased();
    }

    interface Client {
        Attempt connectAttempt(InetSocketAddress address);

        CompletableFuture<Conn> connect(InetSocketAddress address);

        CompletableFuture<Void> shutdown();

        Result<ShutdownResult> shutdownResult();

        CompletableFuture<Void> resourcesTerminated();

        ResourceUsage usage(Resource resource);
    }
}
