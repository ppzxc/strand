# core 내부 구동 계약 — 검토용 예제

검토 티켓: [core 내부 구동 계약과 전송 모듈의 책임 경계](https://github.com/ppzxc/strand/issues/304).

제품 구현이나 컴파일 가능한 Java 예제가 아니다. 이름·시그니처는 설명용이며 제품 공개 API가 아니다. 계약의 정본은 티켓의 최종 resolution이다. 사용자가 예제를 포함한 전체 합의를 승인했다. 최종 결정은 [resolution](https://github.com/ppzxc/strand/issues/304#issuecomment-6940710104)에 기록했다.

## 모듈과 타입

- `strand-core`: 공통 규격 구현을 유지하는 별도 모듈. 설정·명령·번호표·종류별 사건·입출력·쓰기 진행은 내부 구동 계약이다. 스레드·소켓·실행기·사용자 인증 함수를 소유하거나 직접 실행하지 않는다.
- 별도 공통 앱용 API 모듈: Request·RequestOptions·RequestHandle, 요청 결과·응답 접수·종료 결과의 값, 전용 결과 관찰·Observation. 모듈 이름은 미정이다.
- 각 전송 모듈: core와 공통 앱용 모듈에 의존한다. 전송별 연결·클라이언트·서버·Listener·IncomingRequest·인증 함수·보안 설정을 제공한다. core는 공통 앱용 모듈에 의존하지 않는다.

## 입력·출력의 범위

| core에 전달 | core가 판단하거나 출력 | 전송 모듈이 수행 |
| --- | --- | --- |
| 보안 조건 충족 뒤 수신 데이터 | 서문·프레이밍·규격 상태 검사, 소비 길이·중단 이유 | 소켓 읽기와 미소비 입력의 유한 보관 |
| 송신 요청·응답·취소·거절 | 번호표, 응답 접수, 요청 결과, 송신 순서·철회 | 앱 호출 접수·복사·직렬 전달, 핸들과 번호표 연결 |
| 인증 판단 결과와 판단 식별자 | 인증 대기·통과·거절, 중복·늦은 완료 처리 | 인증 함수를 앱용 실행기에서 수행 |
| 핸들러 준비 결과 | 준비 경계 이후 입력 처리 재개 | 팩터리 실행·핸들러 활성화·connect 완료 관리 |
| 경과 시간 | 요청 기한·생존 확인·다음 기한 | 시계 읽기와 타이머 예약 |
| 쓰기 진행·EOF·쓰기 실패·abort·close | 결과 확정성·종료 원인·전송 종료 지시 | 실제 쓰기·통로 종료·I/O 원인 보관·자원 정리 |

사건과 출력은 전송 모듈이 수거한다. 사건은 종류별 타입이며 발생 순서대로 처리한다. 미완료 요청 결과는 연결 종료 사건보다 먼저 보존한다. core 사건에 앱용 연결 객체·실행기·Netty 타입을 넣지 않는다.

## NIO에서 core를 구동하는 모양

```java
// 전송 스레드: 앱 명령도 이 스레드로 전달한다.
ReceiveResult r = core.receive(input, clock.elapsedNanos());
retainUnconsumedWithinLimit(input, r.consumed());
collectEventsInOrder();
scheduleNextWakeup(core.nextDeadline());

// 쓰기 가능 시: 이 세 단계 사이에 다른 core 명령을 처리하지 않는다.
Output out = core.nextOutput();
if (core.isWritable(out.id())) {
    int count = channel.write(out.readBuffer());
    core.reportWritten(out.id(), count, clock.elapsedNanos());
}
collectEventsInOrder();
```

출력 수거는 쓰기 시작이나 전체 전달이 아니다. 0바이트 쓰기는 쓰기 시작이 아니다. core가 아직 쓰지 않은 출력의 철회·순서를 관리한다. 쓰기를 시작한 프레임은 연결이 유지되는 동안 마저 쓰고 RESET 등을 뒤에 보낸다. 전체 전달은 마지막 바이트를 전송 계층에 넘긴 것이며 상대 수신 보장이 아니다. 취소·기한과 연결 끊김의 결과 확정성은 각각 규격의 해당 규칙을 따른다.

앱 요청은 전송 모듈에서 즉시 핸들을 얻는다. core 등록 전 취소·기한 만료·접수 실패는 전송 모듈이 기존 앱 계약대로 처리한다. 등록 뒤에는 번호표로 core에 전달한다. core에 전달하기 전 시간도 요청 기한에 포함한다.

## Netty에서 core를 구동하는 모양

```java
// EventLoop: core 입력은 여기서 직렬 처리한다.
ReceiveResult r = core.receive(borrowInputForThisCall(buf), clock.elapsedNanos());
retainUnconsumedWithinLimit(buf, r.consumed());
collectEventsInOrder();
scheduleNextWakeup(core.nextDeadline());

// core 사건을 수거한 전송 모듈의 분기. 사용자 코드는 core 밖에서 실행한다.
onAuthenticationRequired(event -> {
    submitAuthenticationOnAppExecutor(event, decision ->
        postToEventLoop(() -> {
            core.completeAuthentication(event.id(), decision, clock.elapsedNanos());
            collectEventsInOrder();
        }));
});
```

보조 함수는 전송 모듈의 역할을 나타내며 새로운 공개 API가 아니다. 인증 예외·실행 거절은 준비 실패로 처리한다. 인증 결과가 늦게 도착해도 끝난 연결을 되살리지 않는다. 인증된 상대의 앱용 신원과 채널 보안 객체는 전송 모듈이 관리하며 TLS 객체를 core에 넣지 않는다.

Netty 출력 경로도 출력 식별자·철회·쓰기 진행 계약을 충족해야 한다. `channel.write`·Future 완료·실제 소켓 쓰기를 같은 것으로 가정하지 않는다. 전송 계층 전달 지점과 ByteBuf 소유권은 후속 Netty 결정에서 검토하므로 이 예제는 Netty 송신 API나 무복사를 확정하지 않는다.

## 준비·결과·종료

AUTH와 첫 요청이 같은 입력에 있어도 인증·핸들러 준비 경계에서 소비를 멈춘다. 전송 모듈은 남은 입력을 한도 내에서 보관한다. 기다리는 동안에도 기한과 종료 처리는 계속한다. 핸들러 팩터리 완료 뒤 활성화하고 남은 입력을 재전달한다. 읽기 보류와 제어 프레임 처리·생존 확인의 양립 조건은 준비·종료 후속 티켓에서 확인한다.

core의 요청 결과는 전송 모듈이 앱용 결과로 연결해 한 번 보존한다. 관찰자 실행 실패는 보존된 원래 결과를 바꾸지 않는다. 핸들러·기본 관찰의 연결별 직렬화, 별도 실행기 관찰, 관찰 작업의 실패 보존은 승인된 앱 사용 계약을 따른다.

core는 규격상 연결 종료 원인과 전송 종료 지시(남은 출력 처리 후 닫기 / 즉시 끊기)를 구별한다. 전송 모듈은 EOF·쓰기 실패·abort를 core에 전달하고, 실제 통로 정리와 I/O 예외 보강을 맡는다. 종료 결과 확정과 물리적 자원 정리는 구별한다. closed()·shutdown()의 정확한 완료 경계와 마무리 기한은 후속 결정 대상이다.

## 데이터와 시간

수신 입력은 호출 중 빌려 쓰고 core가 보관할 미완성 부분은 복사한다. 등록한 요청·응답 데이터는 외부 변경이 core 상태를 바꾸지 않도록 한다. 송신 출력은 전송 모듈이 읽기 위치만 관리하고 내용을 바꾸지 않으며, 더는 사용하지 않을 때 해제한다. 앱이 수정할 수 있는 수신 본문은 core 내부 상태나 미전송 출력과 공유하지 않는다. 앱 송신 호출 중 복사 계약을 유지하되 내부의 불필요한 복사 횟수를 고정하지 않는다.

시간은 연결별 기준 시점부터의 비음수 나노초다. 준비 단계부터 같은 시간축을 사용한다. 같은 시각 재입력은 허용하며 음수·역행 입력은 상태 변경 없이 즉시 예외다. 기한 계산 오버플로를 방지하고 기한 없음은 명시적으로 표현한다. 같은 시점의 응답·취소·만료는 전송 모듈 처리 순서로 판정한다.

## 후속 결정

- [연결 준비와 종료의 자원 한도 및 완료 경계](https://github.com/ppzxc/strand/issues/305): 준비 기한·보관 한도·제어 프레임과 읽기 보류·정확한 완료 경계.
- [Netty Future 연동과 ByteBuf 소유권](https://github.com/ppzxc/strand/issues/306): Future 변환·취소, 출력 진행의 Netty 매핑, ByteBuf 보관·해제.
- [요청 접수와 앱용 실행 대기의 유한한 자원 한도](https://github.com/ppzxc/strand/issues/307): 요청·출력·사건·실행 대기의 유한 처리, 예약·반환과 포화 반응.
- [WebSocket 메시지 경계와 core 내부 구동 계약의 양립](https://github.com/ppzxc/strand/issues/308). 이 문서의 TCP 예제만으로 WebSocket 입력을 바이트 스트림으로 축소하지 않는다.
- 후속 결정 뒤 구현 검증 기준과 구현 티켓의 경계 정리. 실제 라이브러리 구현은 이번 맵의 범위 밖이다.

검증: 승인된 앱 사용 예제와 책임 경계를 대조한 문서 검토다. 컴파일·네트워크 실행·성능 측정은 수행하지 않았다.
