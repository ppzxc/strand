package io.github.ppzxc.strand.examples;

import java.io.IOException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 소켓 없이 {@link WebSocketPassage}를 시험하려고 {@link WebSocket}을 흉내 낸다. 보낸 것을 기록하고, {@link #link}로 두 개를 이으면 한쪽이 보낸 것이
 * 상대의 {@link WebSocket.Listener}로 곧바로 넘어간다(JDK는 다른 스레드에서 부르지만 통로는 어느 스레드에서 불려도 같아야 한다).
 */
final class FakeWebSocket implements WebSocket {

    final List<Long> requests = new ArrayList<>();
    final List<byte[]> sentBinary = new ArrayList<>();
    final List<Boolean> sentBinaryLast = new ArrayList<>();
    final List<Integer> sentCloseCodes = new ArrayList<>();
    boolean aborted;
    boolean failSends;

    private boolean outputClosed;
    private FakeWebSocket peer;
    private Listener peerListener;

    /** 두 흉내 소켓을 잇고 양쪽 통로에 열렸음을 알린다. 돌려주는 것은 각 통로 쪽 소켓이다. */
    static FakeWebSocket[] link(WebSocketPassage first, WebSocketPassage second) {
        FakeWebSocket a = new FakeWebSocket();
        FakeWebSocket b = new FakeWebSocket();
        a.peer = b;
        a.peerListener = second.listener();
        b.peer = a;
        b.peerListener = first.listener();
        first.listener().onOpen(a);
        second.listener().onOpen(b);
        return new FakeWebSocket[] {a, b};
    }

    @Override
    public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) {
        if (failSends || outputClosed) {
            return CompletableFuture.failedFuture(new IOException("보낼 수 없다"));
        }
        byte[] copy = new byte[data.remaining()];
        data.get(copy);
        sentBinary.add(copy);
        sentBinaryLast.add(last);
        if (peerListener != null) {
            // JDK는 콜백이 돌아온 뒤 버퍼를 다시 쓸 수 있다. 통로가 복사해 두는지 보려고 건넨 배열을 되돌려 놓는다.
            ByteBuffer delivered = ByteBuffer.wrap(copy.clone());
            peerListener.onBinary(peer, delivered, last);
            java.util.Arrays.fill(delivered.array(), (byte) 0);
        }
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) {
        if (failSends || outputClosed) {
            return CompletableFuture.failedFuture(new IOException("보낼 수 없다"));
        }
        outputClosed = true;
        sentCloseCodes.add(statusCode);
        if (peerListener != null) {
            peerListener.onClose(peer, statusCode, reason);
        }
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public void abort() {
        aborted = true;
        outputClosed = true;
        if (peerListener != null) {
            peerListener.onClose(peer, 1006, "");
        }
    }

    @Override
    public void request(long count) {
        requests.add(count);
    }

    @Override
    public boolean isOutputClosed() {
        return outputClosed;
    }

    @Override
    public boolean isInputClosed() {
        return false;
    }

    @Override
    public String getSubprotocol() {
        return "strand";
    }

    @Override
    public CompletableFuture<WebSocket> sendText(CharSequence data, boolean last) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<WebSocket> sendPing(ByteBuffer message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<WebSocket> sendPong(ByteBuffer message) {
        throw new UnsupportedOperationException();
    }
}
