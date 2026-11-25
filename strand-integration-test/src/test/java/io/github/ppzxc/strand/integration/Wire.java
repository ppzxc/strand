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

interface Wire {

    Server startServer(Function<Conn, Handler> handlers) throws Exception;

    Client newClient(Handler handler, @Nullable Duration idlePingInterval);

    @Nullable
    ControlledPassage claimPassage(boolean server);

    interface Incoming {
        int type();

        byte[] payload();

        Result<ResponseAcceptance> respond(int status, byte[] body);
    }

    interface Handler {
        void handle(Incoming request) throws Exception;
    }

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

        void closeListener();

        CompletableFuture<Void> shutdown();
    }

    interface Client {
        CompletableFuture<Conn> connect(InetSocketAddress address);

        CompletableFuture<Void> shutdown();
    }
}
