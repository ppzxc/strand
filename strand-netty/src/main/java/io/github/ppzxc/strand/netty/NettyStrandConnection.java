package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.OptionalLong;
import org.jspecify.annotations.Nullable;

public final class NettyStrandConnection {
    private final ConnectionEngine engine;
    private final SocketAddress remoteAddress;
    private volatile @Nullable AuthenticatedPeer peer;
    private volatile OptionalLong credentialLifetime = OptionalLong.empty();

    NettyStrandConnection(ConnectionParams params, StrandChannel channel) {
        this.remoteAddress = params.remoteAddress;
        this.engine = new ConnectionEngine(this, params, channel);
    }

    void startClient(InetSocketAddress target) {
        engine.startClient(target);
    }

    void startServer() {
        engine.startServer();
    }

    public RequestHandle request(Request request) {
        return engine.request(Objects.requireNonNull(request, "request"), RequestOptions.noDeadline());
    }

    public RequestHandle request(Request request, RequestOptions options) {
        return engine.request(Objects.requireNonNull(request, "request"), Objects.requireNonNull(options, "options"));
    }

    public void close() {
        engine.close();
    }

    public void abort() {
        engine.abort();
    }

    public void expireCredential() {
        engine.expireCredential();
    }

    public void revokeCredential() {
        engine.revokeCredential();
    }

    public void closeExcessiveUse() {
        engine.closeExcessiveUse();
    }

    public Result<CloseNotice> closed() {
        return engine.closed();
    }

    public AuthenticatedPeer authenticatedPeer() {
        return Objects.requireNonNull(peer, "인증되지 않은 연결은 앱에 보이지 않는다");
    }

    public OptionalLong credentialLifetime() {
        return credentialLifetime;
    }

    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    public ResourceUsage resourceUsage(Resource resource) {
        return engine.usage(resource);
    }

    void shutdownClose() {
        engine.shutdownClose();
    }

    void shutdownAbort() {
        engine.shutdownAbort();
    }

    void announcedCredentialLifetime(OptionalLong value) {
        this.credentialLifetime = value;
    }

    void authenticated(AuthenticatedPeer value) {
        this.peer = value;
    }
}
