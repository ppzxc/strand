package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.ResultCompletion;
import java.util.concurrent.atomic.AtomicBoolean;

final class IncomingState {
    final long streamId;
    final long reservedBytes;
    final ResultCompletion<RequestEnd> ended;
    boolean responded;
    final AtomicBoolean responding = new AtomicBoolean();

    private boolean countReturned;
    private boolean bytesReturned;
    private boolean handlerSettled;

    IncomingState(long streamId, long reservedBytes, ResultCompletion<RequestEnd> ended) {
        this.streamId = streamId;
        this.reservedBytes = reservedBytes;
        this.ended = ended;
    }

    synchronized boolean claimCountReturn() {
        if (countReturned) {
            return false;
        }
        countReturned = true;
        return true;
    }

    synchronized void handlerSettled() {
        handlerSettled = true;
    }

    synchronized boolean claimBytesReturn() {
        if (bytesReturned || !countReturned || !handlerSettled) {
            return false;
        }
        bytesReturned = true;
        return true;
    }
}
