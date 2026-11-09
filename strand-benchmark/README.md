# strand-benchmark

`strand-core` 내부 비용의 증가율을 재는 JMH 벤치마크. 배포하지 않는다. #200에서 만들었다.

판정은 절대 시간이 아니라 증가율로 한다. 입력(바이트 수, 요청 수)이 8배, 10배가 될 때 시간도 그만큼(선형)만 늘어야 한다. CI는 컴파일(`jmhClasses`)만 확인하고 측정하지 않는다. 러너의 수치는 잡음이 커서 근거로 쓸 수 없다.

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
| `OutboxBenchmark` | 오류로 닫기, 번호표 취소, 번호표 상태 조회, 기한 확인의 쓰기 대기열 비용 |
