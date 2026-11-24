---
status: accepted
---

# WebSocket은 strand-netty만 지원한다

공식 전송 모듈 가운데 `strand-netty`는 TCP와 WebSocket을 모두 지원하고, `strand-nio`는 TCP만 지원한다. `strand-nio`는 Java 8 기준(ADR 0001)이라 JDK 11의 `java.net.http.WebSocket`을 쓸 수 없고(클라이언트 전용이기도 하다), 지원하려면 RFC 6455(핸드셰이크·프레이밍·마스킹·Close)를 직접 구현해야 한다. Netty에는 WebSocket 코덱이 내장돼 있어 같은 기능을 훨씬 싸게 낸다. nio가 WebSocket을 지원하지 않는 것은 빠뜨린 것이 아니라 의도한 범위이고, 되돌리려면 RFC 6455 구현이라는 큰 비용이 든다.

## Considered Options

- **nio에 RFC 6455를 직접 구현한다**: Java 8 기준은 지키지만 핸드셰이크, 프레이밍, 마스킹, Close를 모두 직접 짜고 검증해야 한다.
- **Java 11 전용 모듈로 분리한다**: JDK `WebSocket`은 클라이언트 전용이라 서버는 어차피 직접 구현해야 하고, 모듈만 하나 늘어난다.

## Consequences

- WebSocket을 쓰려는 사용자는 `strand-netty`를 쓴다.
- ADR 0006이 가리키는 WebSocket 전용 요구사항의 확인은 netty 쪽 공식 지원 테스트가 맡는다.
