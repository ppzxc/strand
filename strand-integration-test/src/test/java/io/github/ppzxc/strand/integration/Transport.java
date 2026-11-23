package io.github.ppzxc.strand.integration;

import java.util.Locale;
import java.util.function.Supplier;

/**
 * 통합 시나리오를 돌리는 전송. 시험 본문은 한 벌이고 이 값이 매개변수로 들어와 nio와 netty 제품 공개 API 위에서 각각 돈다. 이름이 리포트의 호출 이름에 들어가서 전송별 결과가 따로
 * 나온다.
 */
enum Transport {
    NIO(NioWire::new),
    NETTY(NettyWire::new);

    private final Supplier<Wire> factory;

    Transport(Supplier<Wire> factory) {
        this.factory = factory;
    }

    Wire wire() {
        return factory.get();
    }

    /** 통로 감싸기 지점으로 통로를 관찰할 수 있는가. nio 엔진에만 있다. */
    boolean observesPassage() {
        return this == NIO;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
