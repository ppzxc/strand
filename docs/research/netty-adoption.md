# Strand의 Netty 도입 적합성

조사일: 2026-11-17. 이 문서는 코드와 공개 문서를 살펴본 판단이며 구현이나 성능 측정 결과가 아니다.

## 결론

기존 `strand-nio`와 별개로 **`strand-core` 위에 Netty 기반 클라이언트와 서버를 제공하는 것은 적합하다.** Netty가 이미 있는 서비스에 Strand를 붙이거나 TLS·WebSocket을 제공하는 데 특히 유용하다. 다만 현재는 설계 적합성 판단이며 구현의 안정성이나 성능을 검증한 결과는 아니다.

우선 **단일 선택형 `strand-netty` 모듈**에 클라이언트와 서버 진입점을 함께 두는 것을 권고한다. 두 역할은 연결마다 같은 `Endpoint` 구동, 타이머, 흐름 제어, 종료 로직을 공유하고, 연결 생성(`Bootstrap`)과 수락(`ServerBootstrap`), TLS 및 인증 설정에서 갈린다. 프로토콜은 연결 양쪽에서 요청을 시작할 수 있으므로 서버·클라이언트를 송신 전용·수신 전용으로 나누지 않는다([규격 1절](../specification/STRAND_SPECIFICATION.md)). 하나의 모듈이라도 애플리케이션은 필요한 역할만 사용한다.

| 구성 | 판단 |
| --- | --- |
| `strand-netty` 하나 | 공유 구현과 일관된 동작을 유지하기 쉽다. 첫 구현에 권고한다. |
| `strand-netty-client` + `strand-netty-server` | 독립 배포나 의존성 축소 요구가 실증되면 고려한다. 공통 드라이버를 제3 모듈에 두거나 중복을 피할 구조가 필요하다. |

모듈 분리가 실제로 의존성을 줄이는지는 사용하는 Netty 구성 요소와 발행 메타데이터로 확인해야 한다. 클라이언트와 서버의 릴리스 주기나 소유자가 따로 생길 때도 분리를 재검토한다. `strand-core`와 기존 `strand-nio` 사용자에게 Netty 의존성을 강제하지 않는다.

## 제안하는 구성

- `strand-netty`가 `strand-core`와 필요한 Netty 모듈에 의존한다. TCP와 WebSocket은 공통 연결 드라이버를 공유하되 입력 경계와 출력 변환을 구분한다. WebSocket은 Strand 프레임 하나를 바이너리 메시지 하나에 담아야 한다.
- 클라이언트 진입점은 연결 및 TLS/WS 핸드셰이크를, 서버 진입점은 바인딩·수락 및 연결별 설정을 담당한다. 연결 이후에는 양쪽 모두 요청과 응답을 주고받는 공통 연결 API를 제공한다.
- Netty를 이미 사용하는 서비스가 자체 파이프라인에 삽입할 수 있도록 핸들러 통합 경로도 고려한다. 편의 클라이언트·서버와 이 경로가 같은 드라이버를 사용하게 한다. 이벤트 루프와 채널을 누가 만들고 종료하는지, 외부에서 제공한 자원을 라이브러리가 종료하지 않는지를 API 계약에 명시한다.
- 첫 범위는 운영 요구에 따라 TCP/TLS 또는 WebSocket 중 정하고, 지원한다고 선언한 바인딩에 대해서는 규격의 채널 보안·서버 인증, ALPN 또는 WebSocket 서브프로토콜 및 종료 규칙을 검증한다. 미구현 전송을 지원한다고 표시하지 않는다.
- 현재 core와 NIO의 실질 API는 `internal`이다. core 구동 경계를 정하고 공개 연결 API와 호환성 정책을 설계하는 것이 제품화의 선행 과제다. 기존 라이브러리 빌드 규칙은 외부 런타임 의존성을 금지하므로 새 모듈에 한정된 명시적 예외와 발행 검증도 필요하다([빌드 규칙](../../build-logic/src/main/kotlin/VerifyDependencyAllowlist.kt)).

## 현 구조와 접점

- [`Endpoint`](../../strand-core/src/main/java/io/github/ppzxc/strand/core/internal/Endpoint.java)는 소켓·스레드·실제 시계를 직접 사용하지 않고 입력, 시간 진행, 출력 메시지와 이벤트로 동작한다. Netty는 프로토콜 상태 머신을 대체하기보다 이 경계를 구동하는 데 쓰는 것이 자연스럽다. 다만 현재 클래스는 `internal`이므로 모듈 간 연결과 장기 API 안정성을 별도로 설계해야 한다.
- [`ConnectionEngine`](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/ConnectionEngine.java)는 연결당 데몬 스레드를 만들고, [`TcpPassage`](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/internal/TcpPassage.java)는 연결당 Selector를 연다. Netty의 이벤트 루프 기반 채널 처리는 다수 연결에서 스레드·선택기 공유의 후보지만, 이 프로젝트에서 자원 절감이나 처리량 향상은 아직 입증되지 않았다([Netty EventLoop](https://netty.io/4.1/api/io/netty/channel/EventLoop.html)). 한두 개의 장기 연결에는 이 이점이 작을 수 있다.
- 현재 읽기는 direct buffer에서 새 `byte[]`로 복사하고, 쓰기도 `OutboundMessage.bytes()` 복사본을 사용한다. Netty `ByteBuf` 도입만으로 core 경계의 복사가 사라지지 않는다. 복사 제거를 목표로 하면 core의 소유권 계약부터 바꾸어야 하며 별도 검증이 필요하다([TcpPassage](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/internal/TcpPassage.java)).
- `Passage.await`는 호출 스레드가 읽기/쓰기 가능·타임아웃을 기다리고 `write`는 즉시 수용한 바이트 수를 반환한다. Netty I/O는 비동기이며 결과는 `ChannelFuture`로 통지한다. 따라서 Netty를 단순 `Passage` 구현으로 감싸면서 공유 이벤트 루프에서 `await`를 블로킹하는 방식은 부적합하다. 공유 이벤트 루프의 이점을 얻으려면 이벤트·타이머 중심 드라이버를 따로 만드는 편이 자연스럽다([Passage](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/internal/Passage.java), [ChannelFuture](https://netty.io/4.1/api/io/netty/channel/ChannelFuture.html)).

## 기대 효과와 제약

1. **TLS와 WebSocket:** Netty는 TLS 핸드셰이크를 처리하는 `SslHandler`와 WebSocket 핸드셰이크·제어 프레임 핸들러를 제공한다([TLS](https://netty.io/4.1/api/io/netty/handler/ssl/SslHandler.html), [WebSocket](https://netty.io/4.1/api/io/netty/handler/codec/http/websocketx/WebSocketServerProtocolHandler.html)). Strand의 TLS 인증 정책, ALPN, WebSocket 서브프로토콜·메시지 경계·닫기 규칙을 자동으로 충족한다는 뜻은 아니며, 각각 설정과 적합성 검사가 필요하다. 현재 TCP passage는 맺어진 일반 `SocketChannel`을 받는 구현이다.
2. **Java 8:** Netty 4.2의 최소 Java 버전은 8이어서 Strand의 기준선과 맞는다([4.2 이전 안내](https://netty.io/wiki/netty-4.2-migration-guide.html), [ADR 0001](../adr/0001-java-8-baseline.md)). 실제 선택 버전의 모든 의존성, 빌드·런타임, 유지보수 상태는 도입 시 재확인해야 한다.
3. **쓰기 흐름 제어:** Netty의 high/low watermark는 `isWritable()` 상태를 바꾸는 신호이며 자동적인 절대 메모리 한도가 아니다. 크기 추정의 정확성도 필요하다([WriteBufferWaterMark](https://netty.io/4.1/api/io/netty/channel/WriteBufferWaterMark.html)). Strand는 상대 한도와 기본 64 MiB 절대 한도를 토대로 읽기를 멈춘다. core 대기열, 어댑터 대기열과 Netty에 이미 넘긴 바이트를 포함하여 중복 없이 계수하고, 한도를 넘으면 추가 `write`를 제한해야 한다. 읽기 재개와 이미 전달된 입력까지 시험해야 한다([ConnectionEngine](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/ConnectionEngine.java), [ChannelConfig](https://netty.io/4.1/api/io/netty/channel/ChannelConfig.html)).
4. **결과 확정성과 종료:** 현 core는 `drainMessages()` 시점을 전송 계층에 넘긴 시점으로 간주하여 확인 대기를 시작한다. Netty 큐에 넣은 시점, 비동기 쓰기 성공과 상대의 처리 완료는 서로 다르다. 기존과 같은 보수적 결과 분류를 유지하고, 실패·부분 전송·CLOSE 이후 남은 출력·종료 상한을 검증해야 한다. 쓰기 future 성공을 상대 처리의 증거로 취급하면 안 된다([Endpoint](../../strand-core/src/main/java/io/github/ppzxc/strand/core/internal/Endpoint.java), [ConnectionEngine](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/ConnectionEngine.java), [ChannelFuture](https://netty.io/4.1/api/io/netty/channel/ChannelFuture.html)).
5. **이벤트 루프와 버퍼:** 인증기는 현재 동기로 호출되므로 블로킹하면 연결 진행을 막는다. 공유 이벤트 루프에서는 다른 연결까지 지연할 수 있으므로 인증 및 사용자 콜백의 실행 위치와 타이머 공정성을 확인해야 한다. `ByteBuf`의 참조 계수는 누수와 해제 후 접근 위험을 수반하므로 각 핸들러의 소유권과 예외·종료 경로를 정해야 한다([ConnectionEngine](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/ConnectionEngine.java), [Netty 참조 계수](https://netty.io/wiki/reference-counted-objects.html)).
6. **배포:** 현재 `strand-nio`는 core 외 런타임 의존성이 없도록 구성되어 있다. 별도 모듈에 한정하면 기존 사용자의 의존성 정책을 보전할 수 있다([build.gradle.kts](../../strand-nio/build.gradle.kts)). Netty 네이티브 전송은 별도 플랫폼별 바이너리가 필요하며 공식 Linux 빌드는 glibc를 사용한다. 우선 순수 Java NIO로 검증하고 네이티브는 대상 OS·아키텍처와 측정 근거가 생긴 뒤 선택한다([Netty native transports](https://netty.io/wiki/native-transports.html)).

## 도입 전 검증

1. 실사용 조건을 정한다: 동시 연결 수, 요청 크기·빈도, TLS/WS 필요 여부, 허용 지연 및 메모리, JDK·OS. Netty가 이미 있는 호스트에 삽입하는 경우와 독립 실행을 구분한다.
2. 클라이언트와 서버가 공유하는 실험용 드라이버에서 core를 이벤트 루프에 직렬로 연결한다. 기존 TCP 및 WebSocket 경계, 인증, deadline, EOF/RST, 부분 쓰기, 읽지 않는 상대, 읽기 중지/재개, 종료 및 요청 결과의 정확성을 기존 계약·적합성 테스트와 장애 주입으로 비교한다. 특히 공유 루프에서 한 연결의 느린 인증이나 콜백이 다른 연결을 막는지 확인한다.
3. 동등한 TLS 여부·버퍼·요청 부하에서 현 NIO와 비교한다. 단일 장기 연결과 목표 동시 연결 수 모두에서 처리량, p50/p99/p99.9 지연, CPU, 스레드·메모리·할당/GC, 쓰기 정체 시 메모리 상한과 장애 감지 시간을 재고 장시간 안정성도 확인한다. 기존 [JMH](../../strand-benchmark/README.md)는 core 비용의 증가율만 측정하므로 전송 비교 근거가 아니다.
4. 실험 전에 목표 수치와 수용 가능한 의존성·운영 비용을 합의하고 결과가 그 기준을 만족할 때만 제품화 여부를 결정한다. TLS 인증서 검증·서브프로토콜 불일치·메모리 누수·의존성 업데이트와 보안 대응을 릴리스 조건에 포함한다.
