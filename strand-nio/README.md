# strand-nio

`strand-core`를 JDK NIO 소켓 위에서 구동하는 전송 모듈이다. Java 8 기준이며 외부 의존성이 없다.

## 지원하는 전송

TCP(과 TLS)만 지원한다. **WebSocket은 지원하지 않는다.** 빠뜨린 기능이 아니라 의도한 범위다. Java 8 기준에서는 JDK의 `java.net.http.WebSocket`을 쓸 수 없고, 지원하려면 RFC 6455(핸드셰이크·프레이밍·마스킹·Close)를 직접 구현해야 하기 때문이다. 근거와 대안은 [ADR 0007](../docs/adr/0007-WebSocket은-strand-netty만-지원한다.md)에 있다. WebSocket이 필요하면 `strand-netty`를 쓴다.
