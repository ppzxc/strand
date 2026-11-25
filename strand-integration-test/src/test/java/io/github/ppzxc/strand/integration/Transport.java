package io.github.ppzxc.strand.integration;

import java.util.Locale;
import java.util.function.Supplier;

enum Transport {
    NIO(NioWire::new),
    NETTY(NettyWire::new);

    private final Supplier<Wire> factory;

    Transport(Supplier<Wire> factory) {
        this.factory = factory;
    }

    Wire wire() {
        return factory.get();
    }

    boolean observesPassage() {
        return this == NIO;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
