package io.github.ppzxc.strand.core.internal;

import org.jspecify.annotations.Nullable;

/**
 * 연결 단계. 연결이 지금 어느 단계인지를 값 하나로 소유하고, 규격 3.3절의 ACK 관문과 AUTH 관문이 단계별로 허용하는 프레임 타입, 애플리케이션
 * 송신을 미룰지, 유휴 PING을 보낼 수 있는지, 규격 3.5절의 종료(CLOSE 교환, 닫는 중, 끝)를 한 곳에서 답한다. 서버·클라이언트 차이(누가 AUTH를
 * 보내고 누가 ACK하는가)는 생성 시 역할로 안다. 입출력과 시계를 모르고 프레임 내용도 보지 않는다(타입과 ACK 플래그, 오류 코드만 받는다). 연결
 * 하나의 단계이므로 한 스레드에서만 쓴다.
 *
 * <p>수립 쪽 사건은 정해진 단계에서만 일어나며 다른 단계에서 부르면 {@link IllegalStateException}이다. 호출자는 먼저 {@link #admit}으로
 * 받아도 되는 프레임인지 묻고, 허용된 프레임만 사건으로 옮긴다.
 *
 * <p>종료 쪽은 CLOSE 교환이 일어나면 {@link Phase#CLOSING}, 전송 계층이 닫히면 {@link Phase#CLOSED}다. CLOSE 교환은 수립이 어디까지
 * 진행됐는지와 무관하게 일어나고, 닫는 중에도 도착한 프레임은 수립 진행에 따라 받는다. 그래서 수립 진행은 내부에 따로 두고 {@link #phase()}는
 * 종료 쪽이 있으면 그것을 돌려준다. 전송 계층을 닫을 때가 되면 {@link #shouldCloseTransport()}가 참이 되고, 호출자는 닫은 뒤
 * {@link #transportClosed()}를 알린다. 닫는 길은 이 하나다.
 */
final class ConnectionPhase {

    /** 연결의 단계. 위에서 아래로 나아간다. */
    enum Phase {
        /** 서문을 기다린다. 서버만 이 단계로 시작한다. */
        PREFACE,
        /** 서문을 확인했고 첫 프레임(상대 HELLO)을 기다린다(규격 3.2.4절). */
        FIRST_FRAME,
        /** 첫 프레임을 확인했다. 상대 HELLO를 처리해 ACK를 보내기 전이다. */
        PEER_HELLO,
        /** 상대 HELLO에 ACK를 보냈다. 내 HELLO의 ACK를 기다린다(규격 3.3절 ACK 관문). */
        OWN_HELLO_ACK,
        /** 두 HELLO가 모두 끝났다. 서버는 AUTH를, 클라이언트는 AUTH(ACK)를 기다린다(규격 3.3절 AUTH 관문). */
        AUTH,
        /** AUTH 관문을 통과했다. 애플리케이션 프레임을 주고받는다. */
        OPEN,
        /** CLOSE를 보냈거나 받았다(규격 3.5절). 오류 CLOSE이면 곧 닫고, {@code NO_ERROR}이면 대기 중인 스트림이 없어질 때까지 둔다. */
        CLOSING,
        /** 전송 계층이 닫혀 연결이 끝났다(규격 1.3절 연결이 끝남). 어떤 프레임도 받지 않는다. */
        CLOSED
    }

    /** {@link #admit}의 판단: 받은 프레임 타입의 허용 여부. */
    enum Admission {
        ALLOW,
        CONNECTION_ERROR,
        IGNORE
    }

    private final boolean server;
    /** 수립 진행. {@link Phase#PREFACE}부터 {@link Phase#OPEN}까지의 값만 가진다. */
    private Phase established;
    /** CLOSE를 보냈거나 받았다(규격 1.3절, 3.5절). */
    private boolean closeExchanged;
    /** 상대의 CLOSE를 이미 받았다. 처음 받은 CLOSE의 {@code last_stream_id}만 쓴다(규격 3.5절). */
    private boolean closeReceived;
    /** 이 연결의 끝이 {@code NO_ERROR}로 정리되는 중이다. 오류나 전송 계층 끊김이 겹치면 꺼진다. */
    private boolean normalEnd;
    /** 지금 전송 계층을 닫아야 한다. */
    private boolean closeDue;

    private boolean closed;
    /** 연결을 닫게 한 로컬 원인. {@code CLOSED} 이벤트에 싣는다(규격 3.5절). */
    private @Nullable Throwable cause;

    private ConnectionPhase(boolean server, Phase established) {
        this.server = server;
        this.established = established;
    }

    /** 서버 역할. 서문을 기다리며 시작한다. */
    static ConnectionPhase server() {
        return new ConnectionPhase(true, Phase.PREFACE);
    }

    /** 클라이언트 역할. 서문은 클라이언트가 보내므로 서문 확인을 마친 단계에서 시작한다. */
    static ConnectionPhase client() {
        return new ConnectionPhase(false, Phase.FIRST_FRAME);
    }

    /** 지금 단계. 연결이 끝났으면 {@link Phase#CLOSED}, CLOSE를 교환했으면 {@link Phase#CLOSING}, 아니면 수립 진행이다. */
    Phase phase() {
        if (closed) {
            return Phase.CLOSED;
        }
        return closeExchanged ? Phase.CLOSING : established;
    }

    // 질의

    boolean awaitingPreface() {
        return established == Phase.PREFACE;
    }

    boolean awaitingFirstFrame() {
        return established == Phase.FIRST_FRAME;
    }

    /** 전송 계층이 닫혀 연결이 끝났는가. */
    boolean isEnded() {
        return closed;
    }

    /** CLOSE를 보냈거나 받았는가(규격 3.5절). */
    boolean closeExchanged() {
        return closeExchanged;
    }

    /** 새 요청에 스트림 번호를 줄 수 있는가. CLOSE를 보냈거나 받았거나 연결이 끝났으면 열지 않는다(규격 3.5절). */
    boolean mayOpenRequest() {
        return !closeExchanged && !closeDue && !closed;
    }

    /** 지금 전송 계층을 닫아야 하는가. 닫은 뒤에는 {@link #transportClosed()}를 알린다. */
    boolean shouldCloseTransport() {
        return closeDue && !closed;
    }

    /** 닫는 일이 {@code NO_ERROR}로 끝나는가. 참이면 WebSocket Close(1000)를 덧붙인다(규격 1.2절). */
    boolean endsNormally() {
        return normalEnd;
    }

    /** 연결을 닫게 한 로컬 원인. 없으면 null이다. */
    @Nullable
    Throwable closeCause() {
        return cause;
    }

    /**
     * 받은 프레임 타입을 지금 받아도 되는가(규격 3.3절). CLOSE는 항상 허용한다. 예약 타입은 단계와 무관하게 무시한다(규격 2.2.2절). 그 밖에는
     * 수립 진행이 정한다(닫는 중에도 같다): HELLO·HELLO(ACK)는 순서대로 한 번씩, AUTH는 AUTH 단계에서 서버는 AUTH만 클라이언트는
     * AUTH(ACK)만, PING·RESET·애플리케이션 프레임은 열림 단계에서만 받는다. 어긋나면 연결 오류다. 연결이 끝났으면 아무것도 받지 않는다.
     *
     * @param ack ACK 플래그가 켜져 있는가
     */
    Admission admit(int type, boolean ack) {
        if (closed) {
            return Admission.IGNORE;
        }
        if (type == FrameType.CLOSE) {
            return Admission.ALLOW;
        }
        if (FrameType.isReserved(type)) {
            return Admission.IGNORE;
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

    /** 애플리케이션 송신(요청, PING)을 AUTH 관문 뒤로 미뤄야 하는가(규격 3.3절). */
    boolean defersApplicationSends() {
        return established != Phase.OPEN;
    }

    /** 유휴 PING을 보낼 수 있는가. AUTH 관문을 통과한 뒤 연결이 끝나기 전까지, 종료 중에도 보낸다(규격 3.3절, 3.4.1절, 3.5절). */
    boolean mayIdlePing() {
        return established == Phase.OPEN && !closed;
    }

    // 사건

    /** 서문을 확인했다. */
    void prefaceVerified() {
        advance(Phase.PREFACE, Phase.FIRST_FRAME);
    }

    /** 첫 프레임이 첫 프레임 검사를 통과했다. */
    void firstFrameVerified() {
        advance(Phase.FIRST_FRAME, Phase.PEER_HELLO);
    }

    /** 상대 HELLO를 받아 ACK를 보냈다. */
    void peerHelloAcked() {
        advance(Phase.PEER_HELLO, Phase.OWN_HELLO_ACK);
    }

    /** 내 HELLO의 ACK를 받았다. */
    void ownHelloAckReceived() {
        advance(Phase.OWN_HELLO_ACK, Phase.AUTH);
    }

    /** AUTH 관문을 통과했다. 서버는 AUTH(ACK)를 보냈고 클라이언트는 AUTH(ACK)를 받았다. */
    void authGatePassed() {
        advance(Phase.AUTH, Phase.OPEN);
    }

    // 종료 쪽 사건. 연결이 끝난 뒤에는 모두 아무것도 하지 않는다.

    /**
     * CLOSE를 보냈다. 오류 CLOSE이면 곧 닫을 때가 되고, {@code NO_ERROR}이면 종료 중이 된다.
     *
     * @param errorCode 보낸 CLOSE의 {@code error_code}
     */
    void closeSent(long errorCode) {
        exchange(errorCode);
    }

    /**
     * 형식이 올바른 CLOSE를 받았다.
     *
     * @return 처음 받은 CLOSE인가. 처음 받은 CLOSE의 {@code last_stream_id}만 쓴다(규격 3.5절)
     */
    boolean closeReceived(long errorCode) {
        if (closed) {
            return false;
        }
        boolean first = !closeReceived;
        closeReceived = true;
        exchange(errorCode);
        return first;
    }

    /**
     * 형태가 잘못된 CLOSE를 받았다. 받은 CLOSE로 치고 오류 CLOSE처럼 다룬다(규격 3.5절). 이 CLOSE의 {@code last_stream_id}는 쓰지 않으므로
     * {@link #closeReceived}의 처음 받은 CLOSE로 세지 않는다.
     */
    void malformedCloseReceived() {
        connectionFailed();
    }

    /**
     * 협상 실패 HELLO: 상대 {@code max_frame_size}를 넘는 AUTH를 보낼 수 없어 CLOSE를 보내지 않은 채 CLOSE 교환으로 넘어간다(#196). 연결
     * 오류와 같은 전이다.
     */
    void negotiationFailed() {
        connectionFailed();
    }

    /** 연결 오류(규격 5.1절)와 그 밖의 오류 종료. CLOSE를 이미 보냈거나 받았든 아니든 곧 닫을 때가 되고 정상 종료가 아니다. */
    void connectionFailed() {
        if (closed) {
            return;
        }
        closeExchanged = true;
        normalEnd = false;
        closeDue = true;
    }

    /** 상대가 CLOSE 없이 전송 계층을 끊었다(TCP EOF·RST, WebSocket close). 곧 닫을 때가 되고 정상 종료가 아니다. */
    void transportDisconnected() {
        if (closed) {
            return;
        }
        normalEnd = false;
        closeDue = true;
    }

    /** 대기 중인 스트림이 0개다. 종료 중이면 닫을 때가 된다(규격 3.5절). 종료 중이 아니면 아무것도 하지 않는다. */
    void pendingStreamsReachedZero() {
        if (closeExchanged && !closed) {
            closeDue = true;
        }
    }

    /** 애플리케이션이 종료 중인 연결을 더 기다리지 않기로 했다. 종료 중이면 닫을 때가 된다. */
    void closingAborted() {
        if (closeExchanged && !closed) {
            closeDue = true;
        }
    }

    /** 연결을 닫게 한 로컬 원인을 기록한다. {@code CLOSED} 이벤트에 싣는다. */
    void causedBy(Throwable cause) {
        this.cause = cause;
    }

    /** 전송 계층을 닫았다. 연결이 끝났다. */
    void transportClosed() {
        closed = true;
        closeDue = false;
    }

    private void exchange(long errorCode) {
        if (closed) {
            return;
        }
        closeExchanged = true;
        if (errorCode == ErrorCode.NO_ERROR) {
            normalEnd = true;
        } else {
            normalEnd = false;
            closeDue = true;
        }
    }

    private void advance(Phase from, Phase to) {
        if (established != from) {
            throw new IllegalStateException(from + " 단계의 사건인데 현재 수립 단계는 " + established);
        }
        established = to;
    }
}
