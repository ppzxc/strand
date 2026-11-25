package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.nio.internal.PassageWrapping;

public final class PassageWrapped {

    private PassageWrapped() {}

    public static NioStrandServer.Builder server(NioStrandServer.Builder builder, PassageWrapping.Wrapper wrapper) {
        return builder.passageWrapper(wrapper);
    }

    public static NioStrandClient.Builder client(NioStrandClient.Builder builder, PassageWrapping.Wrapper wrapper) {
        return builder.passageWrapper(wrapper);
    }
}
