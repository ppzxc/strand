package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * 시험이 도는 동안 제품 엔진이 연 모든 통로를 {@link ControlledPassage}로 감싼다. 감싼 통로는 쓴 프레임, 받은 바이트, 쓰기 시점을 시험이 관찰하고 조절하게 한다. 엔진이
 * 통로를 안에서 만들어 시험이 직접 끼울 수 없어서 엔진의 테스트 전용 지점({@link PassageWrapping})에 {@link #wrapper()}를 주입한다. 쌓인 통로는 시험마다 비운다.
 *
 * <p>감싼 통로는 연결이 열린 순서대로 쪽(클라이언트·서버)별 대기열에 쌓이고, 연결을 감싸는 {@link Peer}가 자기 것을 순서대로 가져간다. 연결을 한 번에 하나씩 맺는 시험에서
 * 연결과 통로가 정확히 짝지어진다.
 */
public final class PassageObservation implements BeforeEachCallback, AfterEachCallback {

    private static final ConcurrentLinkedQueue<ControlledPassage> CLIENT = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<ControlledPassage> SERVER = new ConcurrentLinkedQueue<>();
    private static volatile int readChunkBytes = Integer.MAX_VALUE;

    @Override
    public void beforeEach(ExtensionContext context) {
        reset();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        reset();
    }

    /**
     * 통로를 감싸는 감싸개. 시험이 만드는 서버·클라이언트 builder에 주입해 그 연결의 통로만 감싼다({@code NioWire}). 프로세스 전역 설치가 아니라서 이 감싸개를 주입하지 않은
     * 연결은 영향이 없다.
     */
    static PassageWrapping.Wrapper wrapper() {
        return (side, passage) -> {
            ControlledPassage controlled = new ControlledPassage(passage);
            if (readChunkBytes != Integer.MAX_VALUE) {
                controlled.readChunkBytes(readChunkBytes);
            }
            queue(side).add(controlled);
            return controlled;
        };
    }

    /** 쌓인 통로와 읽기 조각 설정을 처음 상태로 되돌린다. */
    private static void reset() {
        CLIENT.clear();
        SERVER.clear();
        readChunkBytes = Integer.MAX_VALUE;
    }

    private static ConcurrentLinkedQueue<ControlledPassage> queue(PassageWrapping.Side side) {
        return side == PassageWrapping.Side.CLIENT ? CLIENT : SERVER;
    }

    /** 이 뒤에 열리는 모든 통로가 한 번에 이 바이트씩만 읽게 한다. 프레임이 쪼개져 오는 경우를 만든다. */
    static void splitReads(int bytes) {
        readChunkBytes = bytes;
    }

    /** 아직 어느 연결도 가져가지 않은 가장 먼저 열린 통로. 없으면 null이다. */
    static @Nullable ControlledPassage claim(PassageWrapping.Side side) {
        return queue(side).poll();
    }
}
