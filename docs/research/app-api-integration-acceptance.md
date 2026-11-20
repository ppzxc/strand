---
ticket: 328
label: ready-for-agent
checked: 2026-11-20
---

# NIO·Netty 공개 API 통합 수용 검증 (#328)

대상: 이슈 #312의 앱 공개 API(NIO·Netty 전송)를 실제 TCP/TLS에서 교차 흐름으로 실행한 기록이다. 개별 티켓(#316~#327)의 시험을 되풀이하지 않고, 그 시험이 한 번에 하나씩만 보던 것을 겹쳐서 본다. 규격 본문은 바꾸지 않았다.

## 결론

아래 조합에서 교차 흐름 시험과 예제 실행이 모두 통과했다. 이 검증에서 결함 두 개를 찾아 고쳤다(마지막 절).

| 항목 | 검증한 값 |
| --- | --- |
| Netty 릴리스 | 4.1.101.Final(컴파일 기준·하한), 4.2.17.Final (`-PnettyVersion`) |
| 채널 | `NioSocketChannel`·`NioServerSocketChannel` 하위 클래스(`StrandChannel`, `StrandServerChannel`), `NioEventLoopGroup`, 출력 handler 없는 pipeline |
| TLS provider | JDK 기본 `SSLEngine`(`SSLContext`), 시험용 localhost 자체 서명 키(실행 때 `keytool`로 생성) |
| JDK | 25, 8(`-PtestJavaVersion=8`) |
| NIO 전송 | `strand-nio`(JDK `SocketChannel`·`Selector`)와 같은 TCP/TLS 시나리오 |

## 지원 제한

- 위 표 밖의 조합은 검증하지 않았고 지원을 선언하지 않는다. 구체적으로 Netty 4.1.101·4.2.17 밖의 릴리스, epoll·io_uring·kqueue 채널, netty-tcnative 같은 JDK 밖 TLS provider, JDK 8·25 밖 JVM, 임의 pipeline·`SslHandler`.
- 소켓 잔존 검사는 `/proc/self/fd`가 있는 Linux에서만 실행된다(없으면 건너뛴다). 이 기록은 Linux aarch64 5.14에서 실행했다.
- 실제 WebSocket 전송과 새 와이어 기능은 이 검증 범위가 아니다. WebSocket은 core 내부 계약과 통로 시험이 계속 통과함만 확인했다.
- 외부 실행 자원(앱 실행기, `EventLoopGroup`)은 앱이 종료한다. 시험도 앱 쪽에서 종료한다.

## 수용 기준 감사

개별 시험이 덮는 것과 교차로 새로 필요한 것을 구분했다.

| 수용 기준 | 이미 덮은 개별 시험 | 새로 추가한 교차 시험·자료 |
| --- | --- | --- |
| 1. 실제 TCP/TLS 준비·양방향 요청·취소·기한·종료, 예제 | `NioStrandUsageTest`, `NettyStrandUsageTest`(준비 뒤 양방향·종료) | `strand-examples`의 `NioAppExample`·`NettyAppExample`과 `AppExamplesTest`: 필수 한도·시간 설정을 모두 표시하고 보안·인증·팩터리 준비, 양방향 요청, 취소, 기한, 연결 종료, 전체 종료를 한 흐름에서 TCP·TLS 각각 실행 |
| 2. 연결별·전체 포화 중 취소·결과 보존·종료, 예약 반환 | `*SaturationTest`, `*ReservationTest`(자원 하나씩) | `SaturationCrossFlowTest`: 연결별 한도와 소유 객체 전체 한도가 찬 채로 취소·응답·abort를 잇고 모든 예약과 연결 슬롯이 0으로 돌아옴 |
| 3. 준비 취소·늦은 팩터리·폐기 거절·claim·종료 기한 초과 겹침 | `*UnreleasedHandlerTest`, `*ConnectAttemptTest`, `*ShutdownTest`(둘씩) | `PreparationOverlapTest`: 준비 포화, 준비 취소, 늦은 인증·팩터리, 폐기 거절, 8개 스레드 동시 claim, 기한 초과가 한 서버에서 겹침 |
| 4. 앱 실행기 정체·Netty 변환 취소 중 결과 조회, 결과가 종료 사건보다 먼저 | `NettyResultsTest`, `*SaturationTest`(관찰 자리), `*ShutdownTest`(정체 실행기) | `ExecutorStallTest`: 실제 TCP/TLS에서 실행기를 정체시킨 채 취소·abort, 변환 Future 취소 독립(Netty). 종료 알림 시점에 남은 결과가 확정되어 있음 |
| 5. 관찰별 완료 경계, TLS 평문 소비/암호문 정리, 남은 자원 없음 | 경계별 개별 시험, `CoreTlsDeliveryTest`·`TlsBoundaryTest`(#315), `*RealOutputReferenceTest` | `CompletionBoundaryTest`(리스닝·연결·전체 종료 결과·실제 자원 종료가 한 흐름에서 서로 다른 경계), `ResourceReleaseTest`(`closed()` 관찰 직후 소켓 없음, 종료 뒤 소켓·내부 스레드·Netty 예약 작업 없음) |
| 6. core 적합성·WebSocket 내부 계약 회귀 | `strand-conformance`, `strand-core`의 WebSocket 시험, `strand-examples`의 WebSocket 통로 시험 | 새 시험 없음. `./gradlew test check`가 함께 통과 |
| 7. 중복 없이 교차만 추가, 조합·명령·결과·제한 기록 | - | 이 문서 |

TLS 평문 소비와 암호문 정리의 구별은 core 구동 수준에서 #315가, 앱 수준의 예약 반환(TLS 평문·암호문 공간 포함)에서 개별 시험이 이미 실제 TLS로 단언한다. 이 검증은 거기에 더해 TLS 연결에서도 `closed()`와 전체 종료가 실제 소켓 해제 뒤에 완료함을 확인한다.

## 시험 구성

위치는 `strand-integration-test/src/test/java/io/github/ppzxc/strand/integration/app/`다. NIO와 Netty의 앱 공개 API를 같은 시나리오로 돌리려고 전송 타입만 감싸는 시험 전용 얇은 어댑터(`App`, `NioRig`, `NettyRig`)를 두었고, 시나리오는 전송 2종 × 보안 2종(TCP·TLS)으로 실행한다.

시험은 시간 경합에 기대지 않는다. 반환·완료는 `get`과 조건 대기(`awaitTrue`: 조건이 참이 될 때까지, 상한은 15초)로 기다리고, 시간 값이 필요한 곳(전체 종료 유예·기한)은 그 값이 만드는 결과가 결정적일 때만 쓴다(반환하지 않는 팩터리·핸들러가 있으면 기한은 반드시 초과한다). `closed()` 직후 소켓 단언은 완료를 읽는 즉시 보려고 `isDone()`을 바로 돈다.

## 재현 절차

```
./gradlew test check --rerun-tasks                                                  # 전체. 4.1.101.Final, JDK 25
./gradlew :strand-netty:test :strand-integration-test:test :strand-examples:test -PnettyVersion=4.2.17.Final
./gradlew :strand-netty:test :strand-integration-test:test :strand-examples:test -PtestJavaVersion=8
./gradlew :strand-netty:test :strand-integration-test:test :strand-examples:test -PnettyVersion=4.2.17.Final -PtestJavaVersion=8
```

## 실행 결과

실행 환경: Linux aarch64 5.14, Red Hat OpenJDK 25, JDK 8(Gradle 툴체인).

| Netty | JDK | 실행 명령 | 결과 |
| --- | --- | --- | --- |
| 4.1.101.Final | 25 | `./gradlew test check --rerun-tasks` | 전체 통과(jacoco 분기·줄 기준 0.90 검증 포함) |
| 4.2.17.Final | 25 | `./gradlew :strand-netty:test :strand-integration-test:test :strand-examples:test -PnettyVersion=4.2.17.Final` | 통과. `strand-netty` 294개, `strand-integration-test` 85개, `strand-examples` 61개, 실패 0 |
| 4.1.101.Final | 8 | `./gradlew :strand-netty:test :strand-integration-test:test -PtestJavaVersion=8` | 통과. 294개, 85개, 실패 0 |
| 4.2.17.Final | 8 | 위 명령에 `-PnettyVersion=4.2.17.Final` 추가 | 통과. 294개, 85개, 실패 0 (마지막 수정 전 실행. 수정은 이벤트 루프 종료 때의 방어 경로 한 곳) |

`strand-examples`는 release 11이라 JDK 8 JVM에서는 실행하지 않는다.


## 이 검증에서 찾아 고친 것

1. **Netty 연결 `closed()`가 소켓의 실제 해제보다 빨랐다(#326 이월).** `closeFuture`가 끝나도 JDK는 다음 선택 주기에 가서야 소켓을 놓는다. 그래서 `closed()`를 관찰한 직후 소켓이 남을 수 있었고, `ResourceReleaseTest`가 NIO와 달리 Netty에서 실패했다.
2. **Netty 4.2에서 `listener.closed()`가 완료하지 않았다(#326은 4.1에서만 검증).** 4.2는 등록 해제 뒤 `selectionKey()`를 내주지 않는다(어서션 실패). 그 결과 4.2에서 `NettyListenerCloseTest`·`NettyStrandShutdownTest` 등 15개 시험이 실패했다.

두 결함의 수정은 같다. 연결과 리스닝의 완료를 알리기 전에 JDK 채널이 선택기에서 풀렸는지(`isRegistered()`)를 보고, 아직 붙들려 있으면 1ms 타이머(쓰기 막힘 재시도와 같은 주기)로 이벤트 루프가 선택 주기를 지나가게 한 뒤 다시 본다. 처음 시도한 `selectNow()` 직접 호출은 다른 연결의 준비 사건을 거두어 드물게 연결 종료를 지연시켜 버렸다.

## 관찰한 주의 사항

- 병렬로 다른 Gradle 작업을 함께 돌리는 등 부하가 크면 15초 상한 조건 대기가 초과할 수 있다. 위 `selectNow()` 결함이 그 한 원인이었고 고친 뒤에는 병렬 실행에서 재현되지 않았다.
- `strand-examples`는 release 11이라 JDK 8 테스트 JVM에서 실행되지 않는다(기존 구성). JDK 8 칸은 `strand-netty`와 `strand-integration-test`만 해당한다.
- 예제(`NioAppExample`·`NettyAppExample`)는 연결별 최소 진행 공간을 전송 모듈 내부의 `ProgressSpace`로 도출한다. 공개 API에는 이 최소 필요량을 조회하는 방법이 없고, 승인된 예제는 이 값들을 앱이 제공하는 설정 변수로 둔다. 예제의 나머지는 공개 API만 쓴다. 최소 필요량 조회 API는 별도 결정이 필요하다.
- TLS 평문·암호문·제어 공간의 포화 자체(공간이 찬 채 취소·종료)는 이 검증에서 교차로 만들지 않았다. 앱 API는 이 공간을 최소 필요량으로만 설정할 수 있고, 공간 부족에서의 진행은 #315·#319·#322의 통로·구동 시험이 본다. 이 검증은 TLS 연결에서 평문·암호문·제어 공간이 따로 예약되고 실제 정리 뒤에 돌아옴을 확인한다.
