package io.github.ppzxc.strand.core.internal;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.WeakHashMap;

/**
 * core 테스트용 정적 얇은 래퍼. 엔드포인트마다 공용 구동 도구({@link EndpointDriver}, core testFixtures)를 하나씩 붙여 두고 호출을 넘긴다. 입력은
 * 한 단위씩 넣고, 인증은 위임된 인증 판단({@code AUTHENTICATION_REQUIRED} 사건)으로 처리하며, 시계는 밀리초로 옮긴다.
 */
final class Drive {
    private static final Map<Endpoint, EndpointDriver> DRIVERS = new WeakHashMap<>();

    private Drive() {}

    private static EndpointDriver newDriver(Endpoint endpoint) {
        return EndpointDriver.of(endpoint).unitsPerReceive(1);
    }

    /** 익명 인증만 통과시키는 서버. 인증 판단은 사건으로 위임되고 구동 도구가 그 자리에서 입력한다. */
    static Endpoint server(EndpointConfig config) {
        Endpoint endpoint = Endpoint.server(config);
        DRIVERS.put(endpoint, newDriver(endpoint).anonymousOnly(config.credentialLifetimeSeconds()));
        return endpoint;
    }

    static Endpoint server(EndpointConfig config, EndpointDriver.Authentication authentication) {
        Endpoint endpoint = Endpoint.server(config);
        OptionalLong lifetime = config.credentialLifetimeSeconds();
        DRIVERS.put(endpoint, newDriver(endpoint).authentication(new EndpointDriver.Authentication() {
            @Override
            public AuthDecision decide(String mechanism, byte[] credentials) {
                return authentication.decide(mechanism, credentials);
            }

            @Override
            public OptionalLong credentialLifetimeSeconds() {
                return lifetime;
            }
        }));
        return endpoint;
    }

    private static EndpointDriver driver(Endpoint endpoint) {
        return DRIVERS.computeIfAbsent(endpoint, Drive::newDriver);
    }

    /** 모든 바이트를 넣는다. 한 단위씩 처리하며 위임된 인증 판단이 나오면 곧바로 입력한다. */
    static void receive(Endpoint endpoint, byte[] bytes) {
        driver(endpoint).receiveStream(bytes);
    }

    /** 단위 입력처럼 {@link #receive}를 거치지 않은 입력 뒤에 위임된 인증 판단이 나왔으면 입력한다. */
    static void settle(Endpoint endpoint) {
        driver(endpoint).settle();
    }

    /** 쌓인 출력을 꺼내 모두 쓴 것으로 보고하고 순서대로 돌려준다. */
    static List<OutboundMessage> drainMessages(Endpoint endpoint) {
        return driver(endpoint).flush();
    }

    static List<Event> drainEvents(Endpoint endpoint) {
        return driver(endpoint).takeEvents();
    }

    /** 가상 시계를 밀리초만큼 앞으로 보낸다. */
    static void advance(Endpoint endpoint, long ms) {
        driver(endpoint).advanceMilliseconds(ms);
    }
}
