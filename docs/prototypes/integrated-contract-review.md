# 앱 사용·core 구동·구현 경계 통합 검토

검토 티켓: [통합 계약의 검증 기준과 구현 티켓 경계](https://github.com/ppzxc/strand/issues/311).

사용자가 Q1–Q12와 통합 자료 전체를 최종 승인했다. 아래 설정·회수·진단 문법도 승인 범위에 포함한다. 컴파일 가능한 구현·실행 검증 결과가 아니다. 기존 두 prototype 문서는 당시 승인 기록으로 보존하며, 후속 결정을 적용한 현재 검토는 이 문서를 사용한다. 동작의 정본은 각 결정 티켓의 최종 resolution이다.

## 1. 결정의 출처

| 계약 | 결정 |
| --- | --- |
| 앱 수명주기 | [TCP 연결 생성부터 종료까지의 애플리케이션 사용 흐름](https://github.com/ppzxc/strand/issues/301#issuecomment-6939869628) |
| 비동기 요청·결과·본문 | [양방향 요청과 응답의 비동기 사용 계약](https://github.com/ppzxc/strand/issues/302#issuecomment-6940175734) |
| 기존 사용 문법 | [클라이언트와 서버의 통합 사용 예제 검토](https://github.com/ppzxc/strand/issues/303#issuecomment-6940331603) |
| core 책임·시간·소유권 | [core 내부 구동 계약과 전송 모듈의 책임 경계](https://github.com/ppzxc/strand/issues/304#issuecomment-6940710104) |
| 준비·종료 경계와 입력 처리 정정 | [연결 준비와 종료의 자원 한도 및 완료 경계](https://github.com/ppzxc/strand/issues/305#issuecomment-6940908095) |
| Netty 변환·TCP/TLS 전달 | [Netty Future 연동과 ByteBuf 소유권](https://github.com/ppzxc/strand/issues/306#issuecomment-6941248379) |
| 요청·응답·실행·수거 예산 | [요청 접수와 앱용 실행 대기의 유한한 자원 한도](https://github.com/ppzxc/strand/issues/307#issuecomment-6941473287) |
| 메시지 입력·전달·종료 | [WebSocket 메시지 경계와 core 내부 구동 계약의 양립](https://github.com/ppzxc/strand/issues/308#issuecomment-6941886089) |
| 준비 총량·늦은 팩터리 결과 | [동시 연결 준비와 준비 작업의 총량 제한](https://github.com/ppzxc/strand/issues/309#issuecomment-6942089016) |

## 2. 필수 설정

설정값은 배포 환경에서 앱이 제공한다. 아래 변수는 모두 명시적인 설정 입력이며 제품 기본값이 아니다. 각 범위의 요청·바이트·작업 한도는 양수·유한하다. 필수값 누락·범위 오류·최소 진행 공간 부족은 build()에서 거절한다.

```java
ResourceLimits limits = ResourceLimits.builder()
    .perConnection(ConnectionLimits.builder()
        .outgoingRequests(outgoingCount, outgoingBytes)
        .incomingRequests(incomingCount, incomingBytes)
        .responseBytes(responseBytes)
        .handlerTasks(handlerTasks)
        .observations(observationTasks)
        .preparationBytes(preparationBytes)
        .progressBytes(frameAssemblyBytes, controlOutputBytes,
                       resultEventBytes, tlsPlaintextBytes,
                       tlsCiphertextBytes, tlsControlBytes)
        .build())
    .perOwner(OwnerLimits.builder()
        .preparingConnections(preparingConnections)
        .totalConnections(totalConnections)
        .preparationTasks(resolveTasks, authenticationTasks, factoryTasks)
        .outgoingRequests(totalOutgoingCount, totalOutgoingBytes)
        .incomingRequests(totalIncomingCount, totalIncomingBytes)
        .responseBytes(totalResponseBytes)
        .handlerTasks(totalHandlerTasks)
        .observations(totalObservationTasks)
        .preparationBytes(totalPreparationBytes)
        .progressBytes(totalFrameAssemblyBytes, totalControlOutputBytes,
                       totalResultEventBytes, totalTlsPlaintextBytes,
                       totalTlsCiphertextBytes, totalTlsControlBytes)
        .build())
    .build();
```


progressBytes는 내부 최소 필요량보다 작게 진행 공간을 지정할 수 있다는 뜻이 아니다. 연결 생성 때 필수 공간을 전체 예산에서도 예약한다. 같은 저장 공간은 중복 계산하지 않고, 평문·암호문 등 서로 다른 저장 공간은 모두 계산한다. 아래 시간 설정도 Netty·NIO builder 양쪽에 전달한다.

```java
.resourceLimits(limits)
.preparationDeadline(preparationDuration) // 양수·유한
.shutdownGrace(shutdownGrace)           // 0 이상
.shutdownDeadline(shutdownDeadline)     // 양수·유한, grace보다 큼
.writeQueueBytes(writeQueueBytes)       // 기존 기본값 64 MiB 유지
.closeFinishDeadline(closeFinish)       // 기존 기본값 5초 유지
```

종류·범위별 설정값·현재 예약량·최고 예약량·거절 횟수는 유한한 집계로 조회한다. 자격 증명 원문을 포함하지 않는다. core는 앱용 ResourceLimits에 의존하지 않고 전송 모듈이 내부 설정으로 변환한다.

## 3. Netty 클라이언트·서버

설정·보안·실행기·업무 함수는 앱이 제공한다. 아래는 하나의 사용 흐름을 나눈 발췌다. 전송별 Handler 타입을 해당 파일에서 import하며, 공통 모듈에 연결 타입을 가진 Handler를 추가하지 않는다.

```java
NettyStrandServer server = NettyStrandServer.builder()
    .eventLoopGroup(appEventLoops).executor(appExecutor)
    .resourceLimits(limits)
    .preparationDeadline(preparationDuration)
    .shutdownGrace(shutdownGrace).shutdownDeadline(shutdownDeadline)
    .writeQueueBytes(writeQueueBytes).closeFinishDeadline(closeFinish)
    .security(serverSecurity)
    .authenticate(credentials -> verify(credentials))
    .handlerFactory(connection -> {
        ServiceState state = createState(connection.authenticatedPeer());
        connection.closed().observe(notice -> state.release());
        Handler handler = request -> {
            BusinessWork work = startWork(state, request.payload());
            request.ended().observe(end -> work.requestStop());
            work.whenComplete((reply, error) -> {
                byte[] body = error == null ? reply : new byte[0];
                int status = error == null ? Status.OK : Status.INTERNAL;
                request.respond(status, body).observe(acceptance -> {
                    if (acceptance.kind() == AcceptanceKind.RESOURCE_EXHAUSTED) {
                        recordShortage(acceptance.resource());
                        // 필요하면 앱이 유한한 재시도 정책을 적용한다.
                        // 라이브러리는 본문을 보관하지 않고 응답 권한도 소비하지 않는다.
                    }
                });
            });
            request.connection().request(Request.of(NOTIFY_TYPE, notification))
                .result().observe(result -> recordReverseResult(result));
        };
        return PreparedHandler.of(handler, () -> state.release());
    })
    .onConnectionFailure(failure -> reportPreparationFailure(failure))
    .build();
Future<Listener> listening = server.listen(address);

NettyStrandClient client = NettyStrandClient.builder()
    .eventLoopGroup(appEventLoops).executor(appExecutor)
    .resourceLimits(limits)
    .preparationDeadline(preparationDuration)
    .shutdownGrace(shutdownGrace).shutdownDeadline(shutdownDeadline)
    .writeQueueBytes(writeQueueBytes).closeFinishDeadline(closeFinish)
    .security(clientSecurity).credentials(credentials)
    .handler(request -> request.respond(Status.OK, handleReverse(request.payload())))
    .build();

NettyConnectAttempt attempt = client.connectAttempt(address);
Future<NettyStrandConnection> connecting = attempt.result();
// 성공·실패의 앱 처리는 appExecutor로 넘긴다. observeOn은 설명용 앱 함수다.
observeOn(appExecutor, connecting, connection -> {
    connection.closed().observe(notice -> recordClose(notice));
    RequestHandle handle = connection.request(Request.of(TYPE, bytes),
        RequestOptions.deadlineAfter(requestDuration));
    handle.result().observe(result -> recordResult(result));
    Future<RequestResult> converted = NettyResults.toFuture(handle.result());
    converted.cancel(false); // 관찰 Future만 취소
    handle.cancel();         // 실제 요청 취소 시도
}, error -> reportConnectFailure(error));

// 취소가 성공보다 먼저 처리된 경우에만 연결 시도 결과를 취소로 확정한다.
boolean cancellationAccepted = attempt.cancel();
attempt.transportReleased().observe(done -> recordAttemptRelease());
```


응답 접수 결과는 ACCEPTED / ALREADY_RESPONDED / REQUEST_ENDED / RESOURCE_EXHAUSTED다. 자원 부족은 응답 권한을 소비하지 않는다. 재시도 시점의 종료·다른 응답 선접수를 다시 판정한다. 자동 재시도·용량 회복 알림을 추가하지 않는다.

송신 byte[]는 호출 중 복사하고 수신 본문은 앱이 보관·수정할 수 있다. ByteBuf 앱용 API와 retain/release 책임을 추가하지 않는다. 결과 확정 후에도 내부 출력 참조가 남으면 바이트 예약은 유지한다. 관찰 실행 거절은 원래 결과를 바꾸지 않는다.

## 4. NIO 대응

NioStrandServer/Client.builder()에도 2절의 동일한 한도·시간 설정을 모두 전달한다. NIO용 security·executor·인증·handler 또는 PreparedHandler를 반환하는 handlerFactory를 설정한다. NIO 생성에는 Netty EventLoopGroup을 전달하지 않는다.

```java
CompletionStage<Listener> listening = nioServer.listen(address);
NioConnectAttempt attempt = nioClient.connectAttempt(address);
CompletionStage<NioStrandConnection> connecting = attempt.result();
connecting.whenCompleteAsync((connection, error) -> {
    if (error != null) reportConnectFailure(error);
    else {
        connection.closed().observe(notice -> recordClose(notice));
        connection.request(Request.of(TYPE, bytes),
            RequestOptions.deadlineAfter(requestDuration))
            .result().observe(result -> recordResult(result));
    }
}, appExecutor);
attempt.cancel();
attempt.transportReleased().observe(done -> recordAttemptRelease());
CompletionStage<Void> stopped = nioServer.shutdown();
```


기존 connect(address)는 Netty Future / NIO CompletionStage 편의 호출로 유지한다. 연결 시도 취소는 attempt.cancel()을 사용한다. Future/Stage 취소를 연결 시도 취소와 같다고 보지 않는다. 공통 결과의 toCompletionStage(appExecutor)는 기존 계약을 유지한다.

## 5. 종료·미정리 결과 회수

```java
listener.close();
listener.closed().observe(done -> recordListeningStopped());
connection.close(); // CLOSE 시작
connection.abort(); // 필요한 경우 즉시 끊기
connection.closed().observe(notice -> recordClose(notice));

Future<Void> stopped = server.shutdown(); // NIO는 CompletionStage<Void>
if (server.shutdownResult().isDone()) {
    ShutdownResult result = server.shutdownResult().resultNow();
    // result.kind(): COMPLETED / DEADLINE_EXCEEDED
    recordShutdown(result.kind(), result.remainingResources());
}
server.resourcesTerminated().observe(done -> recordActualTermination());

// 활성화되지 못한 핸들러의 폐기 실행이 거절된 경우.
for (UnreleasedHandler item : server.unreleasedHandlers()) {
    Optional<PreparedHandler> owned = item.claim();
    if (owned.isPresent()) {
        appDispose(owned.get()); // 앱이 인수한 폐기 책임을 실행
    }
}
```


claim()은 이미 다른 쪽으로 소유권이 이전됐으면 Optional.empty()를 반환한다. 성공 때만 소유권과 정리 책임을 앱으로 이전하고 해당 라이브러리 예약을 반환한다. 앱 정리가 실패해도 자동 반환하지 않는다. unreleasedHandlers()는 내부에 보관 중인 유한한 항목을 열거하며 조회만으로 소유권을 이전하지 않는다. 자동 폐기와 앱 회수가 같은 결과를 함께 소유하지 않는다.

PreparedHandler 폐기는 활성화되지 못한 결과 전용이다. 활성화 뒤의 앱 자원은 종료 알림으로 정리한다. 종료 알림 등록이 거절된 Observation도 앱이 진단하고 정리해야 한다. 예제의 state.release()는 늦은 업무 완료와 동시 접근해도 안전해야 한다.

shutdown()은 기한 초과 시 ShutdownDeadlineExceededException으로 실패하고 shutdownResult()에도 같은 진단을 보존한다. 이후 성공으로 바꾸지 않는다. resourcesTerminated()는 실제 정리와 잔존 작업·미정리 결과 해결까지 기다린다. 외부 실행기 자체 종료는 앱 책임이다.

| 관찰 | 완료 경계 | 기다리지 않는 것 |
| --- | --- | --- |
| connection.closed() | 요청 결과·종료 원인 보존과 실제 통로 종료 | 핸들러 반환·종료 알림 실행 |
| attempt.transportReleased() | 시도 소유 통로·타이머·등록 자원 정리 | 사용자 작업 반환 |
| listener.closed() | 리스닝·경합 수락 소켓 정리 | 기존 연결·이미 등록한 준비 중 연결 종료 |
| owner.shutdownResult() | 전체 정리 완료 또는 고정된 기한 초과 결과 보존 | 기한 이후의 실제 정리 |
| owner.resourcesTerminated() | 실제 내부 자원·잔존 작업·미정리 결과 해결 | 외부 실행 자원 자체 종료 |

## 6. core 내부 구동 계약

core는 별도 모듈이며 Java 8·런타임 외부 의존성 0을 유지한다. 공통 앱용 모듈·Netty·소켓·실행기·사용자 함수에 의존하지 않는다. 아래 작업 이름은 내부 설명용이고 제품 공개 API가 아니다.

| 입력·작업 | core의 판단·출력 | 전송 모듈의 책임 |
| --- | --- | --- |
| 바이트 스트림 입력 | 소비 위치·수거 필요·프레임 검사 | 호출 중 입력 대여·미완성 부분 유한 보관 |
| 메시지 종류·시작·조각·끝 | 끝과 정확한 길이 검증 후 프레임 효과 적용 | 메시지 경계 보존·WebSocket Ping/Pong/Close 처리 |
| 요청 등록·응답·취소 | 번호표·접수·결과·출력 | 호출 중 복사·예약·핸들 연결 |
| 인증 판단·핸들러 준비 결과 | 인증·준비 상태 전이 | 사용자 함수 실행·식별자와 결과 재입력 |
| 출력 확인·진행 보고 | 철회·순서·결과 확정성 | 실제 쓰기·평문 소비·메시지 접수 |
| 경과 시간 | 기한·생존 확인·다음 기한 | 단조 시계 읽기·타이머 예약 |
| EOF·쓰기 실패·abort·close | 남은 요청 결과·종료 원인·종료 지시 | 예외 추가·실제 통로와 자원 정리 |

인증 판단·핸들러 준비 중에도 규격 처리를 계속한다. AUTH 전 금지 입력은 규격대로 처리하고, AUTH 뒤 준비 전 요청은 REFUSED로 거절한다. 준비 전용 입력 보관 큐를 만들지 않는다.

상태 변경에 필요한 공간을 먼저 확보하고 core 처리·출력·사건 수거를 유한한 단위로 수행한다. 공간이 부족하면 소비 위치와 수거 필요 상태를 반환한다. 전송 모듈이 수거·결과 보존 후 미소비 위치부터 재개하며 중복 소비하지 않는다. 사건은 발생 순서대로, 남은 요청 결과는 연결 종료 사건보다 먼저 보존한다. core가 사용자 코드를 호출하지 않는다.

```java
// 전송 스레드의 개념 절차. 각 단계에 유한한 작업량·예약 공간이 있다.
ReceiveResult r = core.receive(input, elapsedNanos);
advanceInput(r.consumed());
collectAndPreserveInOrderWithinBudget();
// r.needsCollection()이면 수거 뒤 같은 미소비 위치부터 재개한다.
scheduleWakeup(core.nextDeadline());

// 확인→실제 시도→보고 사이에 같은 연결의 다른 core 명령을 처리하지 않는다.
Output out = core.nextOutput();
if (core.isWritable(out.id())) {
    int count = channel.write(out.readBuffer()); // TCP. 0은 쓰기 시작이 아니다.
    core.reportWritten(out.id(), count, elapsedNanos);
}
collectAndPreserveInOrderWithinBudget();
```

TLS는 암호문 공간을 예약한 뒤 SSLEngine.wrap하고 실제 평문 소비량을 보고한다. 초기 설계는 프레임 하나씩 wrap한다. WebSocket은 메시지 전체 접수 보고를 사용하며 TCP 쓰기량·TLS 평문 소비로 대신하지 않는다.

| 통로 | 쓰기 시작 | 전체 전달 | 전달 뒤 끊긴 미확정 요청 |
| --- | --- | --- | --- |
| TCP | 첫 양수 소켓 쓰기 | 마지막 바이트 쓰기 | 다른 근거가 없으면 결과 모름 |
| TLS | 첫 평문 소비 | 마지막 평문 바이트 소비 | 암호문 미송신이어도 결과 모름 가능 |
| WebSocket | 승인한 메시지 단위 접수 | 메시지 전체 접수 | 소켓 미송신이어도 결과 모름 |

수거만으로 시작·전체 전달을 보고하지 않는다. 진행 0이면 다음 시도 전에 다시 확인한다. 첫 진행 전에는 철회할 수 있고, 부분 진행 뒤에는 연결이 유지되면 프레임을 마저 처리하고 RESET을 뒤에 보낸다. 전체 전달은 상대 수신 보장이 아니다. 기한 만료와 끊김은 각각 규격의 결과 판단 규칙을 따른다.

시간은 연결별 기준부터의 비음수 나노초다. 같은 시각은 허용하고 음수·역행은 상태 변경 없는 즉시 예외다. 기한 없음을 명시하고 오버플로를 방지한다. 호출부터 core 등록 전 대기도 요청 기한에 포함한다. 같은 시점의 응답·취소·만료는 전송 처리 순서로 판단한다.

입력은 호출 중 빌리고 core 보관 부분은 복사한다. 출력 내용은 불변이며 읽기 위치는 전송 모듈이 관리한다. 전체 전달·철회·끊김으로 실제 참조가 끝나면 해제한다. 번호표·인증 판단 식별자로 늦은 완료와 중복 완료를 구별한다. core 종료 지시와 실제 통로·전체 자원 종료를 구별한다.

## 7. 구현 티켓 경계·선행 조건

각 구현 티켓에는 해당 최종 resolution과 아래 검증 행을 연결한다. 이 표는 구현 티켓 작성 준비 자료이며 이번 지도에서 구현 티켓을 실행하지 않는다.

| 경계 | 선행 조건 | 수용 기준 |
| --- | --- | --- |
| 공통 앱용 API | 통합 자료 승인 | Request·Options·Handle·전용 결과·Observation·접수·종료 값·ResourceLimits. Java 8, Netty/core 타입 노출 없음, 송신 복사·결과 보존·포화 진단 검증 |
| core 내부 계약 | 통합 자료 승인. 공통 앱용 모듈에 의존하지 않음 | 유한 수거·재개·철회·진행·번호표·시간·준비 중 입력·메시지 효과 적용 검증 |
| Netty 진행 경계 선행 구현 검증 | 통합 자료 승인 | 지원 릴리스·채널·TLS provider를 고정하고 실제 TCP/TLS 경계·철회·부분 진행·암호문 정리를 실행 검증. 실패하면 설계 재개 |
| NIO 전송 구동 | 공통 API·core 계약 | 예약·직렬화·TCP/TLS 진행·타이머·결과 보존·관찰 거절 검증 |
| Netty 전송 구동 | 공통 API·core 계약·대상 조합 선행 검증 성공 | NIO와 같은 구동 기준 및 변환 Future 독립 취소·EventLoop 연결·내부 ByteBuf 소유권 검증 |
| NIO 준비·종료 | 공통 API·core 계약. 통로 통합에는 NIO 구동 필요 | 순차 주소 시도·보안·인증·팩터리·취소·늦은 결과 폐기/claim·종료 경계 검증 |
| Netty 준비·종료 | 공통 API·core 계약. 통로 통합에는 검증된 Netty 구동 필요 | 같은 준비·종료 기준과 Netty 생성 Future 검증. 미검증 채널 조합 지원 선언 금지 |
| TCP/TLS 통합 검증 | 대상 전송의 구동·준비·종료 | 다음 표를 NIO/Netty 각각 실행하고 실제 지원 조합 명시 |

구동과 준비·종료는 계약을 기준으로 독립 부분을 진행할 수 있으나 최종 통합 수용에는 양쪽이 필요하다. WebSocket 실제 구현·외부 SPI·공통 드라이버 제품화·NIO 전면 교체·재연결·연결 풀은 이번 경계에 포함하지 않는다. WebSocket core 계약 검증은 core 내부 계약에 포함한다.

## 8. 통합 검증

| 시나리오 | 기대 결과 |
| --- | --- |
| 보안·인증·준비 완료와 양방향 요청 | connect 성공 때 핸들러 사용 가능, 방향별 결과 한 번 보존 |
| 인증 판단 중 CLOSE·중복 AUTH·금지 입력 | 규격 처리 유지, 늦은 판단으로 연결 복구 없음 |
| AUTH 뒤 팩터리 완료 전 요청 | REFUSED, 핸들러 미실행, PING/CLOSE 유지 |
| 준비 취소·기한·성공 경합 | 전송 처리 순서로 결과 하나, 늦은 소켓 정리, 순차 주소 시도는 최초 기한 공유 |
| 늦은 팩터리의 폐기 성공·미반환·거절·claim 경합 | 활성화 없음, 소유권 한 번 이전, 실제 정리나 앱 인수까지 예약 유지 |
| TCP/TLS 진행 0·부분·전체 전후 취소·기한·끊김 | 철회·RESET 순서·규격의 결과 확정성, 확정 후 불변 |
| TLS 평문 전체 소비·암호문 0/부분/전체 쓰기 | 소비 경계 구별, 암호문 정리와 통로 종료 별도 확인 |
| 동시 큰 본문·요청/응답/준비/관찰 포화 | 연결별·전체 수/바이트/작업 한도 유지, 부분 예약 반환, 취소·종료 진행 |
| 응답 부족 뒤 재시도와 다른 응답·종료 경합 | 최초 유효 응답만 접수, 실패 시 본문 미보관·응답 권한 유지 |
| 실행하지 않는 실행기·반환하지 않는 사용자 코드 | 작업 예약 유지, 내부 대기 유한, 전송·결과 확정·조회 지속 |
| observe/변환 거절·Future별 취소 | 원래 결과 불변, 관찰/변환 실패 조회, 다른 Future·실제 요청 영향 없음 |
| 수거 중단·재개·종료 때 여러 요청 결과 | 중복 소비 없음, 결과가 종료 사건보다 먼저 보존, 알림 성공과 무관한 조회 |
| close/abort/리스닝 중지/전체 종료 기한/늦은 실제 정리 | 각 관찰 경계 일치, 기한 초과 결과를 성공으로 변경하지 않음 |
| WebSocket 잘못된 경계·여분 바이트·조각·과대 메시지 | 끝 검증 전 업무 효과 없음, 유한 공간, 규격 오류 반응 |
| WebSocket 전체 접수 전후와 Close 1000 | 메시지 단위 결과 판단, Strand CLOSE/last_stream_id를 만들어내지 않음 |

## 9. 최종 승인과 검토 한계

기존 최종 결정과의 문서 대조만 수행했다. 컴파일·네트워크 실행·부하·성능 측정은 수행하지 않았다. 실제 Netty/TLS 조합 적합성은 선행 구현 검증에서 확인한다. 사용자가 설정 메서드 세부, claim()의 Optional 반환, 진단 값 접근자를 포함한 전체 자료를 최종 승인했다. 구현 티켓 작성 준비 기준을 충족했으며 실제 구현·실행 검증은 후속 작업이다.
