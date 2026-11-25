package io.github.ppzxc.strand.integration.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class TcpOutputDriver {
    private final BoundaryChannel channel;
    private final Endpoint endpoint;
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    private final List<ByteBuf> allocated = new ArrayList<>();
    private final List<Integer> reported = new ArrayList<>();
    private ByteBuf current;
    private long currentId = -1;
    private int zeroWrites;
    private boolean retryScheduled;

    TcpOutputDriver(BoundaryChannel channel, Endpoint endpoint) {
        this.channel = channel;
        this.endpoint = endpoint;

        channel.closeFuture().addListener(future -> {
            endpoint.disconnect();
            releaseCurrent();
        });
    }

    void command(Runnable command) {
        channel.eventLoop().execute(() -> {
            command.run();
            pump();
        });
    }

    void enqueue(Runnable command) {
        channel.eventLoop().execute(command);
    }

    void enqueuePump() {
        channel.eventLoop().execute(this::pump);
    }

    void pump() {
        if (!channel.isOpen()) return;
        try {
            while (true) {
                EndpointOutput output = endpoint.nextOutput();
                if (output == null) {
                    releaseCurrent();
                    return;
                }
                if (output.id() != currentId) {
                    releaseCurrent();
                    current = allocator.directBuffer(output.length());
                    current.writeBytes(output.bytes());
                    allocated.add(current);
                    currentId = output.id();
                }
                int count = channel.writeAttempt(current.nioBuffer());
                endpoint.outputProgress(currentId, count);
                reported.add(count);
                if (count == 0) {
                    zeroWrites++;
                    scheduleRetry();
                    return;
                }
                current.skipBytes(count);
                if (!current.isReadable()) {
                    releaseCurrent();
                }
            }
        } catch (IOException failure) {
            channel.close();
        }
    }

    private void scheduleRetry() {
        if (retryScheduled) return;
        retryScheduled = true;
        channel.eventLoop()
                .schedule(
                        () -> {
                            retryScheduled = false;
                            pump();
                        },
                        1,
                        TimeUnit.MILLISECONDS);
    }

    private void releaseCurrent() {
        if (current != null) {
            current.release();
            current = null;
            currentId = -1;
        }
    }

    boolean hasPendingOutput() {
        return endpoint.nextOutput() != null;
    }

    int zeroWrites() {
        return zeroWrites;
    }

    List<Integer> reported() {
        return new ArrayList<>(reported);
    }

    boolean allReleased() {
        for (ByteBuf buffer : allocated) {
            if (buffer.refCnt() != 0) return false;
        }
        return allocator.metric().usedDirectMemory() == 0;
    }

    int allocatedCount() {
        return allocated.size();
    }
}
