# strand-load

nio와 netty 전송을 실제 소켓 위에서 비교하는 부하 하네스(#422, 스펙 #430). 배포하지 않는 모듈이며 JMH 기반 `strand-benchmark`(core 내부 증가율)와 분리되어 있다.

서버(nio 또는 netty)와 부하 생성기는 별도 프로세스로 뜬다. 부하 생성기 코드 경로는 서버 전송과 무관하게 고정(nio 클라이언트)이라 **서버 전송이 유일한 변수**다.

## 실행

```
./gradlew :strand-load:run --args="run --transport netty --connections 100 --body-bytes 1024 --output result.json"
```

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--transport` | `nio` | 서버 전송: `nio`, `netty` |
| `--connections` | 1 | 연결 수 |
| `--body-bytes` | 64 | 요청·응답 본문 크기 |
| `--load-model` | `closed` | `closed`(폐쇄형 포화, 처리량 상한) 또는 `open`(개방형 고정 도착률, 지연) |
| `--rate` | | `open`에서 필수. 요청/초 또는 포화 처리량 비율(`50%`, `80%`) |
| `--warmup` / `--measure` | 30s / 60s | 워밍업과 측정 시간(`500ms`, `30s`, `2m`) |
| `--repetitions` | 3 | 반복 횟수. 중앙값과 범위를 `summary`에 낸다 |
| `--output` | 표준 출력 | 결과 JSON 경로 |

종료 상태: 0 성공, 1 실행 실패(자식 프로세스 비정상 종료 등; 부분 결과는 남기지 않는다), 2 설정 오류.

## 지연 측정

개방형 부하는 요청의 **예정 시각**을 미리 정해 두고 그 시각에 보내며, 지연은 예정 시각부터 응답까지다(서버가 느려도 지연이 축소되어 보이지 않는다, coordinated omission 방지). 지연은 HdrHistogram(마이크로초)으로 모아 p50/p99/p99.9를 낸다. 부하 생성기가 예정 시각을 지키지 못하면 `loadgenBottleneck`이 true가 되니, 그 실행의 수치는 서버 한계가 아니라 클라이언트 한계로 읽는다.

## TLS와 평문 대조군

기본은 TLS다. 운영 경로는 채널 보안이 필수이므로(ADR 0004) TLS를 켠 상태가 주 비교다. 양쪽 서버에 같은 조건을 강제한다.

- TLS 1.3만, 암호 스위트 `TLS_AES_128_GCM_SHA256`, JDK 기본 provider(자식 JVM의 `jdk.tls.*` 시스템 속성으로 고정)
- 실제 부하 전에 핸드셰이크를 한 번 해 협상된 프로토콜과 암호 스위트를 결과(`negotiatedTls`)에 기록한다. 전송 사이에 같음을 확인할 수 있다.

`--plaintext`는 **하네스 한정 대조군**이다. TLS 비용을 빼고 순수 전송 오버헤드를 분리해 보려는 용도로만 쓰며 결과에 `plaintextControl: true`로 표시된다. 이 옵션은 이 모듈의 CLI에만 있고 제품 API(`strand-nio`, `strand-netty`)나 기본값을 바꾸지 않는다. 제품 코드가 평문 연결을 허용하게 되는 것이 아니다.

## 하네스 자체 시험용 옵션

`--server-delay`(서버가 응답을 미룬다)와 `--chaos-kill-server-after`(서버를 강제 종료한다)는 하네스 자체 시험용이다. 측정 비교에는 쓰지 않는다.

## 결과

JSON이 원본이다(`schemaVersion`, `metadata`(JDK·OS·코어 수·git SHA·설정), `runs`, `summary`). 결과는 커밋하지 않는다. 해석은 `docs/research/`에 문서로 남긴다.

## CI

CI는 컴파일과 짧은 스모크 실행(`LoadCliTest`)만 돈다. 성능 수치는 단정하지 않는다. 러너의 수치는 잡음이 커서 근거로 쓸 수 없다.
