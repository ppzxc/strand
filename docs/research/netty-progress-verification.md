---
ticket: 315
label: ready-for-agent
checked: 2026-11-18
---

# Netty TCP/TLS 실제 진행 경계 선행 검증 (#315)

이 문서는 2026-11-18의 역사적 검증 기록이다. 여기서 검증한 4.1.101.Final은 #495에서 보안 지원 하한 4.1.141.Final로 대체했으며, 최신 하한 검증 결과는 `docs/quality/issue-495-remediation-evidence.md`에 기록한다.

대상: `strand-integration-test/src/test/java/io/github/ppzxc/strand/integration/netty/`. 이슈 #312의 구현 결정 21(지원 조합 선행 검증)과 #306·#310·#311의 승인 결정을 실행으로 확인한 기록이다.

## 결론

아래 검증 범위의 조합은 **통과했고 후속(#322) 지원 후보로 기록한다.** 이 기록은 그 조합에서 승인된 전달 경계(실제 쓰기량·평문 소비량 보고, 첫 진행 전 철회, 부분 진행 뒤 RESET 순서, 정리와 버퍼 해제)를 구현할 수 있음을 보인 것이며, 목록 밖 조합의 호환을 선언하지 않는다.

| 항목 | 지원 후보 | 근거 |
| --- | --- | --- |
| Netty 릴리스 | `netty-transport` 4.1.101.Final, 4.2.17.Final | 두 릴리스에서 `*netty*` 테스트 전부 통과(조합마다 29개, 실패 0) |
| 채널 | `NioSocketChannel` 하위 클래스(`BoundaryChannel`), 출력 handler 없는 pipeline | 쓰기는 `javaChannel().write`로 직접 시도, `channel.write/flush`와 Future를 쓰지 않음 |
| TLS provider | JDK 기본 `SSLEngine`, TLSv1.2와 TLSv1.3 | 두 프로토콜 모두 통과 |
| JVM | JDK 25와 JDK 8(툴체인 `-PtestJavaVersion=8`) | 두 JVM에서 통과 |

후보에서 **제외한 것(검증하지 않음)**: Netty 4.1.101·4.2.17 밖의 릴리스, 임의 pipeline·`SslHandler`·출력 변환 handler, epoll·io_uring·kqueue 채널, netty-tcnative(OpenSSL/BoringSSL) 등 JDK 밖 TLS provider, JDK 8·25 밖 JVM, 쓰기 가능 알림(`OP_WRITE`) 연결. 이들은 별도 검증 전에는 지원하지 않는다.

실패한 조합은 없다. 실패했다면 이 티켓을 성공 완료하거나 일반 `Future` 경계로 대체하지 않고 설계 재검토를 요구해야 한다.

## 재현 절차

```
./gradlew :strand-integration-test:test --tests '*netty*'                                   # 4.1.101.Final, JDK 25
./gradlew :strand-integration-test:test --tests '*netty*' -PnettyVersion=4.2.17.Final        # 릴리스 변경
./gradlew :strand-integration-test:test --tests '*netty*' -PtestJavaVersion=8                # 테스트 JVM 변경
```

실행 환경: Linux aarch64 5.14, Red Hat OpenJDK 25.0.4.1, JDK 8u504(Gradle 툴체인). 테스트의 localhost PKCS#12 키는 시험용이며 2036-11-15까지 유효하다. 실제 Netty NIO 채널, loopback 소켓, 실제 JDK TLS 서버 소켓, 실제 core `Endpoint`만 쓴다. 쓰기 막힘은 송신 버퍼 4096바이트와 읽지 않는 상대로 만든다.

## 검증 구성

- `TcpOutputDriver`: `Endpoint.nextOutput()` → 실제 비차단 소켓 쓰기 → `outputProgress(쓰기량)`을 이벤트 루프의 한 작업 안에서 잇는다. 취소·기한 같은 다른 core 명령은 이벤트 루프 작업으로 직렬화되어 이 세 단계 사이에 끼지 않는다. 출력마다 직접 `ByteBuf`를 하나 만들고 전체 전달·철회·통로 종료에서 정확히 한 번 해제한다.
- `TlsOutputDriver`: 프레임 하나씩 `SSLEngine.wrap`한다. wrap 전에 암호문 공간(패킷 버퍼 하나)을 예약하고(예약 순서는 구동 코드의 구성으로 보장하며 별도 순서 단언은 없다) 예약 합계가 한도를 넘으면 wrap하지 않고 소비량 0을 보고한다. wrap이 돌려준 `bytesConsumed`를 평문 소비량으로 보고하며, 암호문은 별도 대기열에서 실제 소켓 쓰기량대로 내보낸다.
- 어느 구동도 `Future` 완료나 `flush`를 진행의 증거로 쓰지 않는다. 쓰기 가능 알림 대신 1ms 주기 재시도를 쓴다(아래 한계).
- 테스트 입력의 와이어 확인은 상대 소켓이 읽은 바이트를 프레임으로 잘라 비교한다. 결과 확정성은 core가 낸 `RESULT` 사건의 `Certainty`로 확인한다.

## 수용 기준별 결과

| 수용 기준 | 검증 | 관측 |
| --- | --- | --- |
| 조합과 재현 절차·관측 기록, Future를 진행 증거로 쓰지 않음 | 이 문서, 위 구동 | 두 구동 모두 소켓 쓰기량·`wrap` 소비량만 보고 |
| TCP 쓰기 0·부분·전체, 첫 진행 전 철회, 명령 비삽입 | `CoreTcpDeliveryTest` | 포화 소켓에서 보고 0, 철회 뒤 와이어에 요청 없음·`NOT_PROCESSED`. 512 KiB 프레임에서 부분 쓰기 뒤 0. 전체 쓰기량은 프레임 길이와 같음 |
| TLS 프레임별 평문 소비량과 암호문 0·부분·전체, 예약 뒤 wrap, 소비 전 철회, 부분 뒤 RESET 순서 | `CoreTlsDeliveryTest` | 40 KB 프레임이 여러 wrap으로 나뉘어 소비되고 합이 프레임 길이와 같으며 암호문 쓰기량 합은 그보다 크다. 암호문 막힘에서 소비량 0과 철회 시 `NOT_PROCESSED`·와이어에 요청 없음. 암호문 일부만 쓰인 적이 있음을 단언. 부분 소비 뒤 취소에서 프레임 전체 다음 RESET이 평문으로 도착하고 `UNKNOWN` |
| 마지막 평문 소비와 통로·TLS 정리의 차이, ByteBuf 누락·중복 해제 없음 | `CoreTlsDeliveryTest`, `CoreTcpDeliveryTest` | 마지막 평문을 모두 소비한 시점에 암호문은 소켓에 한 바이트도 나가지 않았고 TLS 송신 방향과 통로는 열려 있다. close_notify까지 쓰고 통로를 닫은 뒤에야 TLS 종료·통로 종료. 모든 직접 버퍼의 참조 수 0, 할당기 직접 메모리 사용량 0 |
| 철회 경합 | `CoreWithdrawalRaceTest` | 취소를 네 순서(쓰기와 겹칠 수 있는 지연 취소, 쓰기 시도 전에 요청·취소가 함께 밀림, 같은 작업 안 취소, 프레임 전달 완료 뒤 취소)로 60회씩 TCP·TLS에서 시험한다. 앞의 두 순서는 결정적으로 철회가 되고 완료 뒤 취소는 결정적으로 전달 뒤 RESET이며, 지연 취소만 어느 쪽 결과든 허용하고 일관성만 확인한다. 두 결과가 모두 나와야 통과한다. 결과는 "와이어에 프레임 없음+`NOT_PROCESSED`" 또는 "프레임 전체 뒤 RESET+`UNKNOWN`"뿐이며 프레임 일부·RESET 단독·역순은 없었다 |
| 통과한 조합만 후보로 기록 | 위 표 | 실패 조합 없음 |

## 관찰한 주의 사항

- **쓰기 막힘은 일시적일 수 있다.** 읽지 않는 상대에서도 커널이 쓰기 0 뒤에 나중에 일부를 받는 경우가 있었다. 시험은 쓰기 0 보고, 철회, 상태 확인을 이벤트 루프 한 작업으로 묶거나 전제가 깨지면 새 연결로 다시 시도한다. 구현은 0 보고 뒤 다음 시도 전에 항상 유효성을 다시 확인해야 한다(core가 강제한다).
- **TLS 평문 소비는 암호문 예약 한도에 의존한다.** 예약 한도 안에서는 소켓이 막혀 있어도 프레임의 마지막 평문까지 소비할 수 있다. 이 상태의 끊김은 core가 `UNKNOWN`으로 처리하며 시험이 확인했다.
- **정리 완료 시점.** 예비 실험(`ProgressChannel`)에서 통로 종료의 `close()` Future가 끝난 직후에는 아직 내부 버퍼가 남을 수 있었다. 구동은 `closeFuture` 리스너에서 core에 끊김을 알리고 남은 참조를 해제하며, 시험은 `closeFuture` 뒤 이벤트 루프의 후속 작업을 거친 뒤 해제를 확인한다. 구현은 "통로 정리 완료"를 `close()` Future가 아니라 이 정리 이후로 정의해야 한다.
- 이전 예비 실험 클래스(`ProgressChannel`, `TlsProbe`와 그 테스트)는 core를 쓰지 않는 보조 관찰이며 지원 판정의 근거가 아니다.

## 남은 한계(후속 티켓 몫)

- 쓰기 가능 알림(`OP_WRITE`) 연결은 검증하지 않았다. 구동은 1ms 주기 재시도를 쓴다. 제품 구현은 `ChannelOutboundBuffer`가 비어 있을 때 쓰기 가능 알림을 받는 방법을 정해 다시 검증해야 한다.
- TLS handshake는 이벤트 루프에서 대기하며 완료한다. 제품 구현이 아니다.
- 직렬 구동, 읽기 경로, 타이머, 다중 연결, 임의 handler 호환은 범위 밖이다.
- 전체 전달은 상대 수신 보장이 아니다(결정 20).
