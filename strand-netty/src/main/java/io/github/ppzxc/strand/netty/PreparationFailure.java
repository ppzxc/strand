package io.github.ppzxc.strand.netty;

import java.net.SocketAddress;
import java.util.Objects;

public final class PreparationFailure {
    private final SocketAddress remoteAddress;
    private final PreparationStage stage;
    private final Throwable cause;

    PreparationFailure(SocketAddress remoteAddress, PreparationStage stage, Throwable cause) {
        this.remoteAddress = Objects.requireNonNull(remoteAddress);
        this.stage = Objects.requireNonNull(stage);
        this.cause = Objects.requireNonNull(cause);
    }

    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    public PreparationStage stage() {
        return stage;
    }

    public Throwable cause() {
        return cause;
    }

    @Override
    public String toString() {
        return "PreparationFailure[remoteAddress=" + remoteAddress + ", stage=" + stage + ", cause="
                + cause.getClass().getName() + "]";
    }
}
