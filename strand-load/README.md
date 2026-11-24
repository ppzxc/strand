# strand-load

nio와 netty 전송을 실제 소켓 위에서 비교하는 부하 하네스(#422, 스펙 #430). 배포하지 않는 모듈이며 JMH 기반 `strand-benchmark`(core 내부 증가율)와 분리되어 있다. 측정 결과의 해석은 [`docs/research/transport-load-comparison.md`](../docs/research/transport-load-comparison.md), 측정이 필요한 이유는 [`docs/research/netty-adoption.md`](../docs/research/netty-adoption.md)에 있다.

서버(nio 또는 netty)와 부하 생성기는 **별도 프로세스**로 뜬다. 부하 생성기 코드 경로는 서버 전송과 무관하게 고정(기본 nio 클라이언트)이라 **서버 전송이 유일한 변수**다. 측정 때마다 두 프로세스를 새로 띄우고, 끝나면 정리한다(남는 프로세스가 없다).

## 빠른 시작: 핵심 행렬 재현

필요한 것: JDK 25(빌드 toolchain), `keytool`(JDK에 포함), `kill`(쓰기 정체 시나리오). 서버와 부하 생성기가 코어를 다투지 않도록 코어가 넉넉한 머신에서 돌린다.

```
./gradlew :strand-load:installDist
LOAD=strand-load/build/install/strand-load/lib
java -cp "$LOAD/*" io.github.ppzxc.strand.load.LoadCli matrix --output core-matrix.json
java -cp "$LOAD/*" io.github.ppzxc.strand.load.LoadCli table --input core-matrix.json
```

`matrix`는 핵심 행렬 8셀(연결 {1, 100} × 본문 {64B, 1KiB} × {nio, netty} × TLS 켬)을 한 명령으로 돌린다. 셀마다

1. 폐쇄형 포화로 처리량 상한을 재고,
2. 같은 셀 포화 처리량(반복 중앙값)의 50%와 80% 도착률 개방형으로 지연을 잰다.

기본은 워밍업 30초, 측정 60초, 3회 반복이라 전체에 한 시간 안팎이 걸린다(셀 8 × 시나리오 3 × 반복 3 × 약 100초). 먼저 `--warmup 3s --measure 8s --repetitions 3`처럼 줄여 형태를 확인하고, 근거로 쓸 수치는 기본값으로 잰다. `--plan`은 돌리지 않고 셀 목록만 JSON으로 낸다. `--extended`는 연결 1,000·본문 64KiB·평문 대조군을 더한 확장 행렬이다. `--connections-list 1,100`, `--bodies-list 64,1024`, `--transports nio,netty`로 일부 셀만 고를 수 있다.

## 명령

| 명령 | 하는 일 |
|---|---|
| `run` | 한 설정을 측정한다(아래 옵션) |
| `matrix` | 핵심 행렬 8셀(또는 `--extended`)을 한 번에 측정한다 |
| `table --input <json>` | 행렬 결과 JSON에서 Markdown 표를 만든다 |
| `stall` | 읽기를 멈춘 상대를 붙여 서버 메모리 증가와 감지 시간·원인을 잰다 |
| `soak` | 일정 부하로 오래 돌며 자원 추이와 ByteBuf 누수를 본다(기본 1시간, `--long` 8시간) |

종료 상태: 0 성공, 1 실행 실패(자식 프로세스 비정상 종료·감지 실패 등; 부분 결과는 남기지 않는다), 2 설정 오류.

### `run` 옵션

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--transport` | `nio` | 서버 전송: `nio`, `netty` |
| `--connections` | 1 | 연결 수 |
| `--body-bytes` | 64 | 요청·응답 본문 크기 |
| `--load-model` | `closed` | `closed`(폐쇄형 포화, 처리량 상한) 또는 `open`(개방형 고정 도착률, 지연) |
| `--rate` | | `open`에서 필수. 요청/초 또는 포화 처리량 비율(`50%`, `80%`) |
| `--warmup` / `--measure` | 30s / 60s | 워밍업과 측정 시간(`500ms`, `30s`, `2m`) |
| `--repetitions` | 3 | 반복 횟수. 중앙값과 범위를 `summary`에 낸다 |
| `--plaintext` | (TLS 켬) | 평문 대조군(아래) |
| `--binding` / `--loadgen-transport` | `tcp` / `nio` | netty WebSocket 비교용. `--binding websocket`은 양쪽 netty가 필요하다 |
| `--jfr` / `--profile-dir` | 끔 / `strand-load-profiles` | 프로세스별 JFR 기록을 남긴다 |
| `--async-profiler <lib>` | 끔 | 선택 사항. `libasyncProfiler` 경로를 주면 프로세스별 프로파일을 남긴다 |
| `--output` | 표준 출력 | 결과 JSON 경로 |

## 부하 모델과 지연 측정

- **폐쇄형 포화**: 연결마다 미완료 요청 `inflight`(기본 8)개를 유지하고 하나가 끝나면 곧바로 다음을 보낸다. 처리량 상한을 잰다.
- **개방형 고정 도착률**: 요청의 **예정 시각**을 미리 정해 두고 그 시각에 보내며, 지연은 예정 시각부터 응답까지다. 서버가 느려도 지연이 축소되어 보이지 않는다(coordinated omission 방지). 지연은 HdrHistogram(마이크로초)으로 모아 p50/p99/p99.9를 낸다.
- 부하 생성기가 예정 시각을 지키지 못하면 `loadgenBottleneck`이 true가 된다. 그 실행의 수치는 서버 한계가 아니라 **클라이언트 한계**로 읽는다.
- 워밍업 요청은 세지 않는다. 측정 구간의 요청 오류(`errors`)와 결과 모름(`unknown`) 수를 처리량과 함께 기록한다.

## TLS와 평문 대조군

기본은 TLS다. 운영 경로는 채널 보안이 필수이므로(ADR 0004) TLS를 켠 상태가 주 비교다. 양쪽 서버에 같은 조건을 강제한다.

- TLS 1.3만, 암호 스위트 `TLS_AES_128_GCM_SHA256`, JDK 기본 provider(자식 JVM의 `jdk.tls.*` 시스템 속성으로 고정)
- 실제 부하 전에 핸드셰이크를 한 번 해 협상된 프로토콜과 암호 스위트를 결과(`negotiatedTls`)에 기록한다. 전송 사이에 같음을 확인할 수 있다.

`--plaintext`는 **하네스 한정 대조군**이다. TLS 비용을 빼고 순수 전송 오버헤드를 분리해 보려는 용도로만 쓰며 결과에 `plaintextControl: true`로 표시된다. 이 옵션은 이 모듈의 CLI에만 있고 제품 API(`strand-nio`, `strand-netty`)나 기본값을 바꾸지 않는다. 제품 코드가 평문 연결을 허용하게 되는 것이 아니다.

## 자원 수집

모든 `run`의 측정 구간에서 서버와 부하 생성기 각각의 CPU(`cpuSeconds`, `cpuCores`), RSS, 스레드 수, 힙, GC(횟수·시간), 할당량을 처리량과 같은 JSON(`measurement.resources`)에 담는다. CPU·스레드·힙·GC·할당은 JMX, RSS는 `/proc/self/status`에서 읽으므로 **Linux가 필요**하다(없으면 분명한 메시지로 설정 오류가 난다). 할당량은 표본 시점에 살아 있는 스레드의 합이라 근사값이다. 정확한 할당과 핫스팟은 `--jfr` 기록을 `jfr` 도구나 JDK Mission Control로 본다. `--jfr`은 JFR이 없는 JVM에서, `--async-profiler`는 라이브러리 경로가 틀리면 설정 오류로 끝난다.

## 쓰기 정체(`stall`)

서버가 상대에게 큰 요청을 계속 밀어 넣게 한 뒤 상대 프로세스를 `SIGSTOP`으로 멈춰 읽지 않게 하고, 서버 RSS 증가(`memory.peakGrowthBytes`)와 연결이 끊기기까지의 시간(`detectionMillis`), 끊긴 원인(`close`)을 기록한다. 새 타이머 없이 기존 유휴 PING·확인 대기만으로 끊겨야 하며(ADR 0004), `--max-wait` 안에 끊기지 않으면 실패(종료 1)다. `--idle-ping`(기본 30s)·`--ack-timeout`(기본 5s)으로 감지 시간을 조절한다. 유휴 PING을 0으로 끈 설정에서는 읽기 멈춤만으로 끊기지 않는다(ADR 0004의 한계).

## soak

`soak`은 개방형 고정 도착률(`--rate`, 기본 1000/s)로 1시간(기본), `--long`이면 8시간 돌며 `--sample-interval`(기본 10s)마다 서버·부하 생성기의 RSS·스레드·힙·GC를 시계열(`series`)로 남기고, 시간당 RSS 증가량(`trend`)을 요약한다. 서버는 netty 누수 감지기를 `paranoid`로 켜고 표준 오류의 `LEAK:` 줄 수를 `leaks.count`로 센다. `--plan`은 설정만 낸다.

## netty WebSocket 비교

`matrix --transports netty-tcp,netty-ws`는 같은 netty 서버·netty 클라이언트에서 TCP와 WebSocket 바인딩만 바꿔 같은 JSON 스키마로 비교한다(ADR 0007: WebSocket은 netty만 지원한다).

## 하네스 자체 시험용 옵션

`--server-delay`(서버가 응답을 미룬다)와 `--chaos-kill-server-after`(서버를 강제 종료한다)는 하네스 자체 시험용이다. 측정 비교에는 쓰지 않는다.

## 결과

JSON이 원본이다(`schemaVersion`, `metadata`(JDK·OS·코어 수·git SHA·설정), `runs`, `summary`; 행렬은 `cells`). 표는 항상 JSON에서 만든다. 결과는 커밋하지 않는다. 머신 종속 수치가 저장소를 오염하기 때문이다. 해석만 `docs/research/`에 문서로 남긴다.

## 재현 조건

수치는 다음에 따라 달라진다. 결과를 공유할 때 JSON의 `metadata`를 함께 남기고 아래를 기록한다.

- 코어 수와 서버·부하 생성기의 코어 분리 여부(같은 머신에서 코어를 다투면 서버 수치가 낮게 나온다. `taskset` 등으로 나눌 수 있다)
- JDK 버전, netty 릴리스(`-PnettyVersion=<버전>`, 기본 4.1.101.Final), 커널·TLS 가속
- netty 서버는 이벤트 루프 스레드 2개로 독립 실행한다(이미 netty가 있는 호스트에 삽입하는 경우는 범위 밖). 값은 JSON의 `nettyEventLoops`에 기록된다
- 지연은 시계 해상도와 스케줄링에 민감하다. `loadgenBottleneck`이 true인 실행은 버린다

## CI

CI는 컴파일과 짧은 스모크 실행(`LoadCliTest`)만 돈다. 성능 수치는 단정하지 않는다. 러너의 수치는 잡음이 커서 근거로 쓸 수 없다.
