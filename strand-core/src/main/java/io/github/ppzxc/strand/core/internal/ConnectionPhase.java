package io.github.ppzxc.strand.core.internal;

/**
 * 연결 단계(수립 쪽). 연결이 지금 어느 단계인지를 값 하나로 소유하고, 규격 3.3절의 ACK 관문과 AUTH 관문이 단계별로 허용하는 프레임 타입,
 * 애플리케이션 송신을 미룰지, 유휴 PING을 보낼 수 있는지를 한 곳에서 답한다. 서버·클라이언트 차이(누가 AUTH를 보내고 누가 ACK하는가)는 생성 시
 * 역할로 안다. 입출력과 시계를 모르고 프레임 내용도 보지 않는다(타입과 ACK 플래그만 받는다). 연결 하나의 단계이므로 한 스레드에서만 쓴다.
 *
 * <p>단계는 한 줄로만 나아간다. 사건은 정해진 단계에서만 일어나며 다른 단계에서 부르면 {@link IllegalStateException}이다. 호출자는 먼저
 * {@link #admit}으로 받아도 되는 프레임인지 묻고, 허용된 프레임만 사건으로 옮긴다. 종료 쪽 단계(닫는 중, 끝)는 이 열거에 이어서 늘린다.
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
        OPEN
    }

    /** {@link #admit}의 판단: 받은 프레임 타입의 허용 여부. */
    enum Admission {
        ALLOW,
        CONNECTION_ERROR,
        IGNORE
    }

    private final boolean server;
    private Phase phase;

    private ConnectionPhase(boolean server, Phase phase) {
        this.server = server;
        this.phase = phase;
    }

    /** 서버 역할. 서문을 기다리며 시작한다. */
    static ConnectionPhase server() {
        return new ConnectionPhase(true, Phase.PREFACE);
    }

    /** 클라이언트 역할. 서문은 클라이언트가 보내므로 서문 확인을 마친 단계에서 시작한다. */
    static ConnectionPhase client() {
        return new ConnectionPhase(false, Phase.FIRST_FRAME);
    }

    Phase phase() {
        return phase;
    }

    // 질의

    boolean awaitingPreface() {
        return phase == Phase.PREFACE;
    }

    boolean awaitingFirstFrame() {
        return phase == Phase.FIRST_FRAME;
    }

    /**
     * 받은 프레임 타입을 지금 받아도 되는가(규격 3.3절). CLOSE는 항상 허용한다. 예약 타입은 단계와 무관하게 무시한다(규격 2.2.2절). 그 밖에는
     * 단계가 정한다: HELLO·HELLO(ACK)는 순서대로 한 번씩, AUTH는 AUTH 단계에서 서버는 AUTH만 클라이언트는 AUTH(ACK)만, PING·RESET·애플리케이션
     * 프레임은 열림 단계에서만 받는다. 어긋나면 연결 오류다.
     *
     * @param ack ACK 플래그가 켜져 있는가
     */
    Admission admit(int type, boolean ack) {
        if (type == FrameType.CLOSE) {
            return Admission.ALLOW;
        }
        if (FrameType.isReserved(type)) {
            return Admission.IGNORE;
        }
        boolean allowed;
        if (type == FrameType.HELLO) {
            allowed = ack ? phase == Phase.OWN_HELLO_ACK : phase == Phase.PEER_HELLO;
        } else if (type == FrameType.AUTH) {
            allowed = phase == Phase.AUTH && ack != server;
        } else {
            allowed = phase == Phase.OPEN;
        }
        return allowed ? Admission.ALLOW : Admission.CONNECTION_ERROR;
    }

    /** 애플리케이션 송신(요청, PING)을 AUTH 관문 뒤로 미뤄야 하는가(규격 3.3절). */
    boolean defersApplicationSends() {
        return phase != Phase.OPEN;
    }

    /** 유휴 PING을 보낼 수 있는가. AUTH 관문을 통과한 뒤에만 보낸다(규격 3.3절, 3.4.1절). */
    boolean mayIdlePing() {
        return phase == Phase.OPEN;
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

    private void advance(Phase from, Phase to) {
        if (phase != from) {
            throw new IllegalStateException(from + " 단계의 사건인데 현재 단계는 " + phase);
        }
        phase = to;
    }
}
