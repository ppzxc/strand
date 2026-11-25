package io.github.ppzxc.strand.netty;

public enum PreparationStage {
    CONNECTION,
    SECURITY,
    UPGRADE,
    HELLO,
    AUTHENTICATION,
    HANDLER_FACTORY
}
