package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointDriver;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.WeakHashMap;

/**
 * netty 테스트용 정적 얇은 래퍼. 시험 상대의 core {@link Endpoint}마다 공용 구동 도구({@link EndpointDriver}, core testFixtures)를 하나씩 붙여 두고
 * 호출을 넘긴다. 위임된 인증 판단은 익명 인증만 통과시키는 정책으로 곧바로 입력하고, 그 밖의 사건은 돌려준다.
 */
final class EndpointFeed {
    private static final CollectionSpace SPACE = CollectionSpace.of(1 << 16, 1 << 16);
    private static final Map<Endpoint, EndpointDriver> DRIVERS = Collections.synchronizedMap(new WeakHashMap<>());

    private EndpointFeed() {}

    private static EndpointDriver driver(Endpoint endpoint) {
        return DRIVERS.computeIfAbsent(
                endpoint,
                e -> EndpointDriver.of(e)
                        .anonymousOnly(OptionalLong.empty())
                        .space(SPACE)
                        .eventBatch(1024)
                        .eagerEvents(true)
                        .unitsPerReceive(64));
    }

    /** 받은 바이트를 모두 넣고, 그 사이 나온 사건 가운데 인증 판단을 뺀 것을 돌려준다. */
    static List<Event> receive(Endpoint endpoint, byte[] bytes) {
        EndpointDriver driver = driver(endpoint);
        driver.receiveStream(bytes);
        return driver.takeEvents();
    }

    /** 쌓인 사건을 모두 수거하고 인증 판단은 입력한다. 인증 판단을 뺀 사건을 돌려준다. */
    static List<Event> drain(Endpoint endpoint) {
        return driver(endpoint).takeEvents();
    }
}
