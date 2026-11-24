# strand-benchmark

`strand-core` 내부 비용의 증가율을 재는 JMH 벤치마크. 배포하지 않는다. #200에서 만들었다.

판정은 절대 시간이 아니라 증가율로 한다. 결과는 시간 단위(`AverageTime`, ms/op)이고 작을수록 좋다. 입력(바이트 수, 요청 수)이 커질 때 시간도 그만큼(선형)만 늘어야 한다. 대기 수를 100배(2000→200000) 키웠을 때의 변화로 판정한다. 이전 이슈(#216)의 처리량(ops/ms) 수치는 `1/x`로 ms/op로 환산해 비교한다. 유휴 PING 송신 경로는 연결당 30초에 한 번이라 재지 않는다. CI는 컴파일(`jmhClasses`)만 확인하고 측정하지 않는다. 러너의 수치는 잡음이 커서 근거로 쓸 수 없다.

`Frames` 같은 패키지 전용 클래스를 쓰려고 벤치마크 소스도 `io.github.ppzxc.strand.core.internal` 패키지에 둔다. 클래스패스로 실행하고 `module-info`가 없어서 가능하다.

## 실행

```sh
./gradlew :strand-benchmark:jmh                              # 전체
./gradlew :strand-benchmark:jmh -PjmhIncludes=OutboxBenchmark   # 정규식으로 일부만
```

결과는 `strand-benchmark/build/results/jmh/results.json`에 남는다.

| 벤치마크 | 보는 것 |
|---|---|
| `ReceiveManySmallFramesBenchmark` | 청크 하나에 작은 프레임이 가득 찬 입력의 `receive()` |
| `ReceiveBigFrameBenchmark` | 큰 프레임 하나가 작은 조각으로 도착하는 입력의 `receive()` |
| `ReceiveRequestFramesBenchmark` | 실제 요청 프레임 1024개(본문 64B/1KiB/64KiB)를 서버가 받는 비용. 파싱과 `REQUEST_RECEIVED` 이벤트 생성 포함. 위 두 Receive 벤치마크의 예약 프레임이 대조군이다 |
| `ReceivePingBenchmark` | PING 1000/10000/100000개를 받고 ACK를 쌓는 비용 |
| `OutboxBenchmark` | 오류로 닫기, 번호표 취소, 번호표 상태 조회, 기한 확인의 쓰기 대기열 비용 |
| `TicketLookupBenchmark` | 번호 없는 대기열 요청, 번호 받은 쓰기 전 요청, 쓰기 시작한 요청을 번호표로 상태 조회, 취소 후 재충전하는 비용. 대기 수 2000/20000/200000에서 퍼뜨린 표본과 이웃 표본을 비교한다. 대기 수와 무관해야 한다(#207) |
| `RespondBenchmark` | 서버가 받아 둔 요청 전부에 `respond`하는 비용. 대기 수 2000/20000/200000 × 응답 순서(먼저/나중 보낸 것부터). 응답 한 건당 비용이 대기 수와 무관해야 하므로, 한 호출이 n건 전부를 처리하는 시간은 대기 수에 선형이어야 한다 |
| `ResponseConsumeBenchmark` | 클라이언트가 응답 전부를 받아 내 요청 장부에서 소비하는 비용(`RESULT` 이벤트). 같은 대기 수·응답 순서 행렬. 응답 한 건당 번호표 조회가 대기 수와 무관해야 하므로(#207) 전체 시간은 대기 수에 선형이어야 한다 |
| `WriteCompletionBenchmark` | 프레임 하나를 다 쓰는 비용이 응답을 기다리는 열린 요청 수(100/2000/20000)에 비례하지 않는지(#382) |
| `DeadlineExpiryBenchmark` | 기한 있는 대기 요청이 많을 때 `advance`의 비용. 만료 수(10/100/1000)에만 비례하고 대기 수와 무관해야 한다(#207) |
