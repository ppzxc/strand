package io.github.ppzxc.strand.netty;

import java.net.SocketAddress;
import java.util.Objects;

/** 연결 준비 실패의 진단: 상대 주소, 실패한 단계, 원인. 자격 증명 원문은 담지 않는다. */
public final class PreparationFailure {
    private final SocketAddress remoteAddress;
    private final PreparationStage stage;
    private final Throwable cause;

    PreparationFailure(SocketAddress remoteAddress, PreparationStage stage, Throwable cause) {
        this.remoteAddress = Objects.requireNonNull(remoteAddress);
        this.stage = Objects.requireNonNull(stage);
        this.cause = Objects.requireNonNull(cause);
    }

    /**
     * 상대 주소. 접수 직후 실패해도 알려진다.
     *
     * @return 상대 주소
     */
    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * 실패한 단계.
     *
     * @return 단계
     */
    public PreparationStage stage() {
        return stage;
    }

    /**
     * 실패 원인.
     *
     * @return 원인
     */
    public Throwable cause() {
        return cause;
    }

    @Override
    public String toString() {
        return "PreparationFailure[remoteAddress=" + remoteAddress + ", stage=" + stage + ", cause="
                + cause.getClass().getName() + "]";
    }
}
