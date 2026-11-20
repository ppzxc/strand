# Strand 통합 사용 예제 — 검토용 초안

제품 코드가 아닌 Java 사용 모양이며 컴파일 대상이 아니다. 아래 승인 기록에 명시된 이름·계약을 제외한 타입·메서드·상수 이름은 제안이다. 전송별 생성 API의 공통화를 전제하지 않는다. `security`, 인증 함수, 업무 함수는 애플리케이션이 제공한다. 보안 설정은 규격의 채널 보안 조건을 충족해야 한다.

검토 티켓: [클라이언트와 서버의 통합 사용 예제 검토](https://github.com/ppzxc/strand/issues/303).

## 1. 공유 핸들러로 시작

```java
NettyStrandServer server = NettyStrandServer.builder()
    .security(serverSecurity)
    .authenticate(credentials -> verify(credentials))
    .handler(request -> request.respond(Status.OK, request.payload()))
    .onConnectionFailure(failure ->
        reportPreparationFailure(failure.remoteAddress(), failure.phase(), failure.cause()))
    .build();

Future<Listener> listening = server.listen(address);
observeOn(appExecutor, listening, listener -> keepListener(listener), error -> reportBindFailure(error));

NettyStrandClient client = NettyStrandClient.builder()
    .security(clientSecurity)
    .credentials(credentials)
    .handler(request -> request.respond(Status.OK, handleServerRequest(request.payload())))
    .build();

Future<NettyStrandConnection> connecting = client.connect(address);
observeOn(appExecutor, connecting, connection -> {
    connection.closed().observe(notice -> reportClose(notice));
    RequestHandle call = connection.request(Request.of(TYPE, bytes));
    call.result().observe(result ->
        record(result.status(), result.certainty(), result.payload()));
}, error -> reportConnectFailure(error));
```

공유 핸들러는 여러 연결에서 동시에 호출될 수 있다. 송신 본문은 호출 중 복사하며 수신 본문은 앱이 보관할 수 있다. 응답 접수 결과를 버리는 최소 예제이며 운영 예제에서는 아래처럼 관찰한다.

## 2. 연결별 상태·양방향 요청·비동기 응답

```java
NettyStrandServer server = NettyStrandServer.builder()
    .eventLoopGroup(appEventLoops)
    .executor(appExecutor)
    .security(serverSecurity)
    .authenticate(credentials -> verify(credentials))
    .handlerFactory(connection -> {
        ServiceState state = new ServiceState(connection.authenticatedPeer());
        connection.closed().observe(notice -> state.release());
        return request -> {
            // 연결별 핸들러는 직렬 호출되지만 비동기 업무는 겹칠 수 있다.
            BusinessWork work = startBusinessWork(state, request.payload());
            request.ended().observe(end -> {
                if (end.isPeerReset() || end.isConnectionEnd()) work.requestStop();
            });
            work.whenComplete((reply, error) -> {
                request.respond(error == null ? Status.OK : Status.INTERNAL,
                                error == null ? reply : new byte[0])
                    .observe(acceptance -> recordAcceptance(acceptance));
            });

            RequestHandle reverse = request.connection().request(Request.of(NOTIFY_TYPE, notification));
            reverse.result().observe(result -> recordReverseResult(result));
        };
    })
    .onConnectionFailure(failure -> reportPreparationFailure(failure))
    .build();
```

`ServiceState`와 `BusinessWork`는 앱 코드다. `release()`는 늦게 끝나는 업무와 동시 접근해도 안전해야 하며, 업무 중단은 협조 요청이다. 팩터리 실행 중 연결이 끝나도 종료 결과를 관찰할 수 있고 반환한 핸들러는 활성화하지 않는다. 해당 실패는 서버 준비 실패 경로로 전달한다.

## 3. 요청 기한·취소·관찰 작업

```java
RequestHandle call = connection.request(
    Request.of(TYPE, bytes).extensions(extensions),
    RequestOptions.deadlineAfter(Duration.ofSeconds(2)));

Observation observation = call.result().observe(result -> {
    record(result.status(), result.certainty(), result.payload());
});
if (observation.isDone()) {
    observation.failureNow().ifPresent(error -> reportObservationFailure(error));
}
// 완료 전 failureNow()는 즉시 예외. 성공 시 Optional.empty().
call.cancel(); // 성공 여부는 보존된 요청 결과로 확인한다.

// 기존 CompletionStage 기반 앱과 통합할 때만 명시적으로 변환한다.
call.result().toCompletionStage(appExecutor).thenAccept(result -> recordResult(result));
```

기한은 request 호출부터 로컬 대기를 포함한다. 위 설정만으로 와이어 timeout을 보내지 않는다. 응답과 취소가 경합하면 먼저 확정된 결과를 유지한다. 전송 스레드나 같은 연결의 핸들러에서 결과를 기다리지 않는다. 전송 스레드에서 직접 실행하는 Executor는 지원하지 않는다.

## 4. 수락 중지·연결 종료·전체 종료

```java
listener.close();       // 이 리스닝의 새 수락만 중지한다.
connection.close();     // CLOSE 절차 시작. 실제 종료를 기다리지 않는다.
connection.abort();     // 진행 중 CLOSE를 기다리지 않고 끊는다.
connection.closed().observe(notice -> recordClose(notice));

Future<Void> serverStopped = server.shutdown();
Future<Void> clientStopped = client.shutdown();
// Netty 결과 리스너에서 앱 업무를 직접 실행하지 않는다.
// 앱 제공 appExecutor/appEventLoops는 각 소유 객체 종료 완료 후 앱이 종료한다.
```

소유 객체는 새 작업 차단·진행 중 준비 정리·수락 중지·기존 연결 CLOSE·기한 후 abort·내부 자원 종료를 수행한다. 준비·종료의 시간·버퍼 한도와 완료 경계는 별도 티켓에서 결정한다.

## 실패 흐름 검토표

| 사건 | 앱이 보는 결과 |
| --- | --- |
| 서버 바인딩 실패 | listen 실패. 개별 연결 준비 실패와 구별 |
| 인증 거절·팩터리 예외·준비 기한 초과 | 클라이언트 connect 실패, 서버 준비 실패. 리스닝 유지 |
| 준비 완료 직후 연결 끊김 | connect 성공 뒤에도 closed와 요청 결과로 관찰 가능 |
| 응답 전 연결 끊김 | 규격에 맞는 status와 결과 확정성을 요청 결과에 보존 |
| 취소·기한·응답 경합 | 최초 확정 결과 유지. 취소 호출만으로 처리 안 됨을 추측하지 않음 |
| 중복 응답·RESET 뒤 늦은 응답 | 이미 응답함 / 요청이 끝남. 추가 응답 송신 없음 |
| 핸들러 실행 거절 | 살아 있는 요청을 REFUSED로 거절. 연결 유지 |
| 핸들러 예외 | 살아 있고 미응답이면 본문 없는 INTERNAL. 로컬 진단, 연결 유지 |
| 관찰 실행 거절 | 원래 결과 보존. 해당 관찰 작업 실패를 별도 조회 |

## 검토 중 승인 기록과 후속 범위

- Netty listen/connect는 io.netty.util.concurrent.Future를 반환하기로 승인했다. NIO listen/connect는 CompletionStage를 반환하며 실패는 예외 완료다.
- observeOn은 성공·실패 처리를 앱용 실행기로 넘기는 설명용 앱 보조 함수다. Strand API가 아니며, 실행기 거절은 앱이 진단해야 한다. Netty Future 리스너에서 업무 코드를 직접 실행하지 않는다.
- listener.close / connection.close / connection.abort / server.shutdown / client.shutdown 이름과 역할을 승인했다.
- 받은 요청 이름은 IncomingRequest, 응답은 respond(), 보존된 요청 종료는 ended()로 승인했다. 응답 접수 값은 ACCEPTED / ALREADY_RESPONDED / REQUEST_ENDED이며 ACCEPTED는 상대 수신 보장이 아니다. 응답 접수 결과도 늦게 관찰할 수 있다.
- observe()는 Observation을 반환한다. isDone()과 failureNow()로 관찰 작업의 완료·실패를 비차단 조회한다. 완료 전 failureNow()는 즉시 예외, 성공은 빈 값, 사용자 예외·실행기 거절은 원인을 반환한다. 원래 요청 결과에는 영향을 주지 않는다.
- 공통 타입의 배치, core 책임 경계, Netty Future·ByteBuf API, 유한 자원 설정.

NIO 대응 예제는 아래에 제시한다.

## 승인된 NIO 대응 사용 모양

```java
NioStrandServer server = NioStrandServer.builder()
    .security(nioServerSecurity)
    .authenticate(credentials -> verify(credentials))
    .handler(request -> request.respond(Status.OK, request.payload()))
    .build();
CompletionStage<Listener> listening = server.listen(address);
listening.whenCompleteAsync((listener, error) -> {
    if (error != null) reportBindFailure(error);
    else keepListener(listener);
}, appExecutor);

NioStrandClient client = NioStrandClient.builder()
    .security(nioClientSecurity)
    .credentials(credentials)
    .handler(request -> request.respond(Status.OK, handleServerRequest(request.payload())))
    .build();
CompletionStage<NioStrandConnection> connecting = client.connect(address);
connecting.whenCompleteAsync((connection, error) -> {
    if (error != null) reportConnectFailure(error);
    else {
        connection.closed().observe(notice -> reportClose(notice));
        connection.request(Request.of(TYPE, bytes)).result().observe(result -> recordResult(result));
    }
}, appExecutor);
CompletionStage<Void> stopped = server.shutdown();
```

NIO listen/connect 실패는 예외 완료다. Stage 취소는 연결 시도 취소를 뜻하지 않는다. 진행 중 연결 시도는 클라이언트 전체 종료로 정리한다. NIO 보안 설정은 NIO 환경에 맞는 타입이며 구체 타입과 builder 메서드 전체는 아직 제안이다. 기본 요청·연결 종료 관찰은 앱용 실행기에서 같은 연결의 핸들러와 직렬화한다. closed()는 전용 결과 인터페이스이며 shutdown()은 Netty Future<Void> / NIO CompletionStage<Void>다. 정확한 종료 완료 경계는 별도 티켓에서 결정한다.

## 보존된 결과의 즉시 조회와 요청 옵션 — 승인

```java
if (call.result().isDone()) {
    RequestResult result = call.result().resultNow();
    record(result.status(), result.certainty(), result.payload());
}
```

요청 결과·응답 접수·받은 요청 종료·연결 종료의 전용 결과 인터페이스는 isDone()/resultNow() 문법을 사용한다. 완료 전 resultNow()는 즉시 예외다. 요청 실패도 status·결과 확정성이 있는 RequestResult로 반환한다.

request(Request)는 기한 없음이다. request(Request, RequestOptions.deadlineAfter(Duration))는 호출 시점부터 로컬 대기를 포함해 기한을 센다. 0은 즉시 만료, 음수는 호출 시 예외다. Request.extensions(extensions)는 와이어 확장 항목이며 deadlineAfter는 timeout을 자동 설정하지 않는다.

## 검토 자료의 한계

Java 8 사용 모양을 검토한 문서이며 컴파일·네트워크 실행·성능 측정은 수행하지 않았다. 익명화한 앱 업무 함수와 설정 객체를 실제 구현으로 간주하지 않는다. Netty/NIO 생성·전체 종료 결과 타입을 달리하되 작은 공통 요청 핸들과 전용 결과 관찰을 유지한다. NIO listen/connect의 Stage 후속 업무는 명시적으로 앱용 실행기에 전달한다.

자원 한도와 종료 완료 경계, Netty Future 변환과 ByteBuf 소유권, core·전송 모듈의 책임 및 공통 타입 배치는 기존 후속 결정 티켓에서 정한다. 기본 byte[] 경로를 검토한 것이며 ByteBuf 제공을 약속하지 않는다.

## 최종 확인

사용자가 통합 예제 전체를 승인했다. 이 문서는 통합 사용 계약 검토의 고정 자료이며, 실제 라이브러리 구현과 컴파일 가능한 예제는 후속 작업이다.
