package io.github.ppzxc.strand.core.internal;

import org.jspecify.annotations.Nullable;

final class ConnectionPhase {

    enum Phase {
        PREFACE,

        FIRST_FRAME,

        PEER_HELLO,

        OWN_HELLO_ACK,

        AUTH,

        OPEN,

        CLOSING,

        CLOSED
    }

    enum Admission {
        ALLOW,
        CONNECTION_ERROR,

        RESERVED
    }

    enum ApplicationSend {
        NOW,

        DEFER,

        REFUSE
    }

    private enum Ending {
        NONE,

        CLOSING,

        DRAINED,

        FAILED,

        DISCONNECTED;

        boolean exchanged() {
            return this != NONE && this != DISCONNECTED;
        }

        Ending afterNormalClose() {
            if (this == NONE) {
                return CLOSING;
            }
            return this == DISCONNECTED ? FAILED : this;
        }

        Ending afterError() {
            return FAILED;
        }

        Ending afterDisconnect() {
            switch (this) {
                case NONE:
                    return DISCONNECTED;
                case CLOSING:
                case DRAINED:
                    return FAILED;
                default:
                    return this;
            }
        }

        Ending afterPendingStreamsZero() {
            return this == CLOSING ? DRAINED : this;
        }

        boolean due() {
            return this == DRAINED || this == FAILED || this == DISCONNECTED;
        }
    }

    private final boolean server;

    private Phase established;

    private Ending ending = Ending.NONE;

    private boolean closeReceived;

    private boolean closed;

    private CloseReason.@Nullable Sender firstCloseSender;

    private long errorCloseCode;

    private CloseReason.@Nullable Sender errorCloseSender;

    private boolean disconnected;

    private ConnectionPhase(boolean server, Phase established) {
        this.server = server;
        this.established = established;
    }

    static ConnectionPhase server() {
        return new ConnectionPhase(true, Phase.PREFACE);
    }

    static ConnectionPhase client() {
        return new ConnectionPhase(false, Phase.FIRST_FRAME);
    }

    Phase phase() {
        if (closed) {
            return Phase.CLOSED;
        }
        return ending.exchanged() ? Phase.CLOSING : established;
    }

    boolean hasPeerHello() {
        return established == Phase.OWN_HELLO_ACK || established == Phase.AUTH || established == Phase.OPEN;
    }

    boolean hasPassedAuthGate() {
        return established == Phase.OPEN;
    }

    boolean awaitingPreface() {
        return established == Phase.PREFACE;
    }

    boolean awaitingFirstFrame() {
        return established == Phase.FIRST_FRAME;
    }

    boolean isEnded() {
        return closed;
    }

    boolean closeExchanged() {
        return ending.exchanged();
    }

    boolean isActive() {
        return !ending.exchanged() && !closed;
    }

    boolean shouldCloseTransport() {
        return !closed && ending.due();
    }

    CloseReason closeReason() {
        if (errorCloseSender != null) {
            return CloseReason.errorClose(errorCloseCode, errorCloseSender);
        }
        if (disconnected) {
            return CloseReason.disconnected();
        }
        return CloseReason.close(firstCloseSender != null ? firstCloseSender : CloseReason.Sender.LOCAL);
    }

    Admission admit(int type, boolean ack) {
        if (closed) {
            throw new IllegalStateException("끝난 연결은 받은 프레임을 묻지 않는다");
        }
        if (type == FrameType.CLOSE) {
            return Admission.ALLOW;
        }
        if (FrameType.isReserved(type)) {
            return Admission.RESERVED;
        }
        boolean allowed;
        if (type == FrameType.HELLO) {
            allowed = ack ? established == Phase.OWN_HELLO_ACK : established == Phase.PEER_HELLO;
        } else if (type == FrameType.AUTH) {
            allowed = established == Phase.AUTH && ack != server;
        } else {
            allowed = established == Phase.OPEN;
        }
        return allowed ? Admission.ALLOW : Admission.CONNECTION_ERROR;
    }

    ApplicationSend applicationSend() {
        if (closed) {
            return ApplicationSend.REFUSE;
        }
        return established == Phase.OPEN ? ApplicationSend.NOW : ApplicationSend.DEFER;
    }

    boolean mayIdlePing() {
        return established == Phase.OPEN && !closed;
    }

    void prefaceVerified() {
        advance(Phase.PREFACE, Phase.FIRST_FRAME);
    }

    void firstFrameVerified() {
        advance(Phase.FIRST_FRAME, Phase.PEER_HELLO);
    }

    void peerHelloAcked() {
        advance(Phase.PEER_HELLO, Phase.OWN_HELLO_ACK);
    }

    void ownHelloAckReceived() {
        advance(Phase.OWN_HELLO_ACK, Phase.AUTH);
    }

    void authGatePassed() {
        advance(Phase.AUTH, Phase.OPEN);
    }

    void closeSent(long errorCode) {
        exchange(errorCode, CloseReason.Sender.LOCAL);
    }

    boolean closeReceived(long errorCode) {
        if (closed) {
            return false;
        }
        boolean first = !closeReceived;
        closeReceived = true;
        exchange(errorCode, CloseReason.Sender.PEER);
        return first;
    }

    void connectionFailed() {
        connectionFailed(ErrorCode.PROTOCOL_ERROR);
    }

    void connectionFailed(long errorCode) {
        if (closed) {
            return;
        }
        if (errorCloseSender == null) {
            errorCloseCode = errorCode;
            errorCloseSender = CloseReason.Sender.LOCAL;
        }
        ending = ending.afterError();
    }

    void transportDisconnected() {
        if (closed) {
            return;
        }
        if (!ending.due()) {
            disconnected = true;
        }
        ending = ending.afterDisconnect();
    }

    void pendingStreamsReachedZero() {
        if (!closed) {
            ending = ending.afterPendingStreamsZero();
        }
    }

    boolean endedByDisconnect() {
        return disconnected;
    }

    void transportClosed() {
        closed = true;
    }

    private void exchange(long errorCode, CloseReason.Sender sender) {
        if (closed) {
            return;
        }
        if (firstCloseSender == null) {
            firstCloseSender = sender;
        }
        if (errorCode != ErrorCode.NO_ERROR && errorCloseSender == null) {
            errorCloseCode = errorCode;
            errorCloseSender = sender;
        }
        if (errorCode != ErrorCode.NO_ERROR) {
            ending = ending.afterError();
        } else {
            ending = ending.afterNormalClose();
        }
    }

    private void advance(Phase from, Phase to) {
        if (established != from) {
            throw new IllegalStateException(from + " 단계의 사건인데 현재 수립 단계는 " + established);
        }
        established = to;
    }
}
