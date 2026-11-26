# strand-netty

`strand-core`를 Netty 채널 위에서 구동하는 전송 모듈이다. TCP와 WebSocket을 모두 지원한다(규격 1.2절). WebSocket을 지원하는 공식 전송 모듈은 이 모듈뿐이다([ADR 0007](../docs/adr/0007-WebSocket은-strand-netty만-지원한다.md)).

## 의존성

Netty는 앱이 제공한다. 이 모듈은 Netty를 컴파일에만 쓰고 런타임 의존성으로 가져오지 않는다.

| 쓰는 전송 | 앱이 제공할 Netty 모듈 |
| --- | --- |
| TCP | `netty-transport` |
| WebSocket | `netty-transport`와 `netty-codec-http` (같은 Netty 버전) |

TCP만 쓰면 `netty-codec-http`가 클래스패스에 없어도 동작한다. WebSocket을 고른 `listen`·`connect`·`connectAttempt` 호출은 코덱이 없으면 `IllegalStateException`으로 곧바로 실패한다. 지원하는 Netty 하한은 4.1.141.Final이고 `-PnettyVersion=<버전>`으로 반복 검증한다.

## 전송 고르기

`listen`·`connect`·`connectAttempt`에 전송 값 `Transport`를 넘긴다. 인자가 없는 호출은 `Transport.tcp()`와 같다. 나머지 API(핸들러, 인증, 자원 한도, 종료)는 전송과 무관하다.

```java
// 서버: listener마다 전송 하나. TCP와 WebSocket을 함께 받으려면 포트를 달리해 두 번 부른다.
Listener tcp = server.listen(new InetSocketAddress(7000), Transport.tcp()).get();
Listener ws = server.listen(new InetSocketAddress(8000), Transport.webSocket("/strand")).get();

// 클라이언트
NettyStrandConnection connection =
        client.connect(new InetSocketAddress("example.com", 8000), Transport.webSocket("/strand")).get();
```

같은 포트에서 두 전송을 함께 받는 포트 공유는 지원하지 않는다. 코드는 `strand-examples`의 `NettyWebSocketExample`을 본다.

## 이벤트 루프와 실행기

`eventLoopGroup`과 `executor`는 앱이 만들고 소유한다. 클라이언트, 서버, 리스너와 연결을 닫아도 이 모듈은 두 자원을 종료하지 않는다. 연결 채널 하나는 그룹의 EventLoop 하나에 고정되며, core의 연결 상태와 프로토콜 처리는 그 EventLoop에서만 실행된다. 다른 스레드에서 들어온 연결 명령은 해당 EventLoop에 예약한다.

`executor`는 인증, 핸들러 팩터리, 핸들러 호출, 알림과 기본 관찰을 실행한다. 이 실행기는 작업을 호출자 스레드에서 바로 실행하면 안 된다. 앱은 연결을 반환하는 작업과 Netty Future 리스너를 짧게 유지하고, 오래 걸리는 업무는 앱 실행기로 넘긴다. `listen`과 `connect`의 Future는 관련 EventLoop에서 완료되며, `NettyResults.toFuture`로 만든 요청 Future와 그 Future에 등록한 리스너도 연결 EventLoop를 따른다. Future 완료는 리스너 실행 성공을 보장하지 않는다.

## WebSocket 규칙

- **경로**: `Transport.webSocket(path)`의 경로(생략하면 `/`)와 정확히 일치하는 요청만 업그레이드한다. 경로가 다르거나 WebSocket 업그레이드가 아닌 HTTP 요청은 HTTP 404로 거절하고 연결을 닫는다. 질의 문자열은 경로에 들지 않는다.
- **하위 프로토콜**: `strand`로 고정이며 설정으로 열지 않는다. 클라이언트는 `strand`만 제시한다. 서버는 `strand`가 제시되지 않은 업그레이드를 HTTP 400으로 거절하고, 클라이언트는 서버가 `strand`를 선택하지 않고 응답하면 연결 시도를 실패시킨다.
- **Host 헤더**: 클라이언트가 연결 주소에서 만든다.
- **메시지**: 프레임 하나가 바이너리 메시지 하나이고 연결 서문이 첫 메시지다. 받은 텍스트 메시지와 크기가 `4 + length`와 다른 메시지는 연결 오류다. 조각난 메시지는 `max_frame_size`까지 모아 한 메시지로 보며, 한도를 넘는 프레임·누적은 메모리를 쓰기 전에 거절한다.
- **Ping·Pong**: WebSocket Ping에는 Pong으로 답하지만 생존 확인에는 쓰지 않는다. 유휴 PING과 확인 대기는 규격 3.3절·3.4.1절을 따른다.
- **준비**: 준비 기한은 TCP 수락(연결)부터 WebSocket 업그레이드, TLS, HELLO, 인증까지 센다. 업그레이드 실패(경로·하위 프로토콜 거절 포함)는 `PreparationStage.UPGRADE`의 준비 실패로 `onConnectionFailure`·`PreparationFailedException`에 나오고 원인은 `WebSocketUpgradeException`이다.

## 보안(wss)

별도 스킴이나 보안 타입은 없다. `security(NettySecurity.tls(context))`를 쓰면 TLS 위의 WebSocket이다. WebSocket의 TLS 협상은 ALPN 없이 이뤄진다(규격 3.7절: ALPN은 TCP에만 적용된다). 보안을 설정하지 않으면 TCP와 같은 규칙의 설정 오류다.

## 종료와 Close 상태 코드

`NO_ERROR`로 끝나는 연결은 남은 출력을 다 보낸 뒤 WebSocket Close 1000으로 닫고, CLOSE를 보내는 종료에서는 CLOSE 프레임 송신 뒤에 Close를 보낸다. 오류 종료의 상태 코드는 아래 표다(CLOSE를 보내든 받든 같다). 오류로 끝난 연결에는 1000을 쓰지 않고, Close 사유 문자열은 항상 비운다.

| 종료 원인 | WebSocket 상태 코드 |
| --- | --- |
| `NO_ERROR` | 1000 |
| `PROTOCOL_ERROR` | 1002 |
| `EXCESSIVE_USE`, `UNAUTHENTICATED` | 1008 |
| `INTERNAL_ERROR`, `UNRESPONSIVE` | 1011 |
| CLOSE 없이 닫음(서문 오류, 첫 프레임 오류, CLOSE 뒤의 연결 오류, 텍스트 메시지, 메시지 크기 ≠ 4 + `length`) | 1002 |

상대가 CLOSE 없이 WebSocket을 닫으면 상대의 코드를 그대로 돌려주고 앱에는 노출하지 않는다. 연결은 TCP와 같은 `DISCONNECTED`로 끝난다. 이 표는 이 모듈의 동작이며 규격과 적합성 벡터는 값을 단언하지 않는다(규격 1.2절은 오류 종료의 상태 코드를 정하지 않고 `1000`만 금지한다).
