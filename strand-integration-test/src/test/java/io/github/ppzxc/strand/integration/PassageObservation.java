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
 * 통로를 안에서 만들어 시험이 직접 끼울 수 없어서 엔진의 테스트 전용 지점({@link PassageWrapping})에 설치한다. 시험 사이에 새지 않게 시험마다 설치하고 되돌린다.
 *
 * <p>감싼 통로는 연결이 열린 순서대로 쪽(클라이언트·서버)별 대기열에 쌓이고, 연결을 감싸는 {@link Peer}가 자기 것을 순서대로 가져간다. 연결을 한 번에 하나씩 맺는 시험에서
 * 연결과 통로가 정확히 짝지어진다.
 */
public final class PassageObservation implements BeforeEachCallback, AfterEachCallback {

    private static final ConcurrentLinkedQueue<ControlledPassage> CLIENT = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<ControlledPassage> SERVER = new ConcurrentLinkedQueue<>();
    private static volatile int readChunkBytes = Integer.MAX_VALUE;
    private static volatile PassageWrapping.@Nullable Installation installation;

    @Override
    public void beforeEach(ExtensionContext context) {
        CLIENT.clear();
        SERVER.clear();
        readChunkBytes = Integer.MAX_VALUE;
        installation = PassageWrapping.install((side, passage) -> {
            ControlledPassage controlled = new ControlledPassage(passage);
            if (readChunkBytes != Integer.MAX_VALUE) {
                controlled.readChunkBytes(readChunkBytes);
            }
            (side == PassageWrapping.Side.CLIENT ? CLIENT : SERVER).add(controlled);
            return controlled;
        });
    }

    @Override
    public void afterEach(ExtensionContext context) {
        PassageWrapping.Installation current = installation;
        if (current != null) {
            current.close();
            installation = null;
        }
        CLIENT.clear();
        SERVER.clear();
        readChunkBytes = Integer.MAX_VALUE;
    }

    /** 이 뒤에 열리는 모든 통로가 한 번에 이 바이트씩만 읽게 한다. 프레임이 쪼개져 오는 경우를 만든다. */
    static void splitReads(int bytes) {
        readChunkBytes = bytes;
    }

    /** 아직 어느 연결도 가져가지 않은 가장 먼저 열린 통로. 없으면 null이다. */
    static @Nullable ControlledPassage claim(PassageWrapping.Side side) {
        return (side == PassageWrapping.Side.CLIENT ? CLIENT : SERVER).poll();
    }
}
