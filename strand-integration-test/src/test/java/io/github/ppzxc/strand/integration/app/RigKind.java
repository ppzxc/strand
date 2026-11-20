package io.github.ppzxc.strand.integration.app;

import java.util.stream.Stream;

/** 시나리오를 돌리는 조합: 전송(NIO·Netty) × 보안(TCP·TLS). */
final class RigKind {
    private final boolean netty;
    private final boolean tls;

    private RigKind(boolean netty, boolean tls) {
        this.netty = netty;
        this.tls = tls;
    }

    static Stream<RigKind> all() {
        return Stream.of(
                new RigKind(false, false), new RigKind(false, true), new RigKind(true, false), new RigKind(true, true));
    }

    static Stream<RigKind> tls() {
        return all().filter(kind -> kind.tls);
    }

    static Stream<RigKind> netty() {
        return all().filter(kind -> kind.netty);
    }

    boolean isNetty() {
        return netty;
    }

    boolean isTls() {
        return tls;
    }

    Rig open(Spec spec) throws Exception {
        return netty ? new NettyRig(tls, spec) : new NioRig(tls, spec);
    }

    Rig open() throws Exception {
        return open(new Spec());
    }

    @Override
    public String toString() {
        return (netty ? "netty" : "nio") + (tls ? "/tls" : "/tcp");
    }
}
