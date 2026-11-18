package io.github.ppzxc.strand.integration.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Experimental ByteBuf-only outbound path, not a production channel. */
public final class ProgressChannel extends NioSocketChannel {
    private int zeroWrites;

    int zeroWrites() {
        return zeroWrites;
    }

    private final List<Integer> progress = new ArrayList<>();
    private final List<Integer> referenceCounts = new ArrayList<>();

    private final java.util.Set<ByteBuf> withdrawn =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<ByteBuf, Boolean>());
    private final java.util.Set<ByteBuf> started =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<ByteBuf, Boolean>());

    private final java.util.Set<ByteBuf> pending =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<ByteBuf, Boolean>());

    io.netty.channel.ChannelFuture send(ByteBuf frame) {
        if (!frame.isDirect()) throw new IllegalArgumentException("direct buffer required");
        io.netty.channel.ChannelPromise promise = newPromise();
        try {
            eventLoop().execute(() -> {
                pending.add(frame);
                promise.addListener(future -> {
                    pending.remove(frame);
                    started.remove(frame);
                    withdrawn.remove(frame);
                });
                writeAndFlush(frame, promise);
            });
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            frame.release();
            promise.tryFailure(rejected);
        }
        return promise;
    }

    int retainedCount() {
        return pending.size() + started.size() + withdrawn.size();
    }

    void withdraw(ByteBuf frame) {
        if (!eventLoop().inEventLoop()) throw new IllegalStateException("event loop required");
        if (pending.contains(frame) && !started.contains(frame)) withdrawn.add(frame);
    }

    @Override
    protected void doClose() throws Exception {
        try {
            super.doClose();
        } finally {
            pending.clear();
            started.clear();
            withdrawn.clear();
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer outbound) throws Exception {
        for (int attempts = 0; attempts < 16; attempts++) {
            Object current = outbound.current();
            if (current == null) return;
            if (!(current instanceof ByteBuf)) throw new IllegalArgumentException("only ByteBuf is supported");
            ByteBuf frame = (ByteBuf) current;
            if (withdrawn.remove(frame)) {
                outbound.remove();
                continue;
            }
            if (!frame.isReadable()) {
                outbound.remove();
                continue;
            }
            ByteBuffer bytes = frame.nioBuffer();
            int count = javaChannel().write(bytes);
            if (count == 0) {
                zeroWrites++;
                incompleteWrite(true);
                return;
            }
            started.add(frame);
            progress.add(count);
            referenceCounts.add(frame.refCnt());
            boolean complete = count == frame.readableBytes();
            outbound.removeBytes(count);
            if (complete) started.remove(frame);
        }
        incompleteWrite(false);
    }

    List<Integer> progress() {
        return new ArrayList<>(progress);
    }

    List<Integer> bufferReferenceCountsAtReport() {
        return new ArrayList<>(referenceCounts);
    }
}
