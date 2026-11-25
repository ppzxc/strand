package io.github.ppzxc.strand.core.internal;

public final class WriteQueueLimit {

    public static final long DEFAULT_ABSOLUTE_LIMIT = 64L * 1024 * 1024;

    private WriteQueueLimit() {}

    public static long of(
            long peerMaxFrameSize, long peerMaxConcurrentStreams, long ownMaxConcurrentStreams, long absoluteLimit) {
        long range = (peerMaxConcurrentStreams + ownMaxConcurrentStreams) * peerMaxFrameSize + peerMaxFrameSize;
        return Math.max(peerMaxFrameSize, Math.min(range, absoluteLimit));
    }
}
