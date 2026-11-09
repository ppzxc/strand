---
ticket: 200
label: wayfinder:task
checked: 2026-11-09
---

# Q7. `Endpoint.receive()` 수신 버퍼 설계 조사

대상: `strand-core/src/main/java/io/github/ppzxc/strand/core/internal/Endpoint.java`, `Frames.java`
제약: 런타임 의존성 없음(JDK만 사용), `--release 8`(`build-logic/src/main/kotlin/StrandBuild.kt`: `JAVA_RELEASE = 8`, 툴체인 25), Error Prone와 NullAway 적용, 단일 스레드 sans-IO.

## 0. 결론 요약

**추천: "제자리 파싱과 경계에 걸친 프레임 하나만 버퍼링"하는 혼합 방식**

- 들어온 `byte[]` 안에 통째로 들어 있는 프레임은 호출자 배열을 `(array, offset, length)` 뷰로 바로 파싱하고 처리한다. 복사도 할당도 없다.
- 청크 경계에 걸친 프레임 하나만 내부 누적 버퍼에 모은다. 따라서 누적 버퍼에는 늘 "미완성 프레임 한 개"만 있고, compaction은 구조적으로 필요 없다.
- 누적 버퍼는 공격자가 적어 보낸 길이가 아니라 **실제로 받은 바이트에 비례해** 두 배씩 키운다. 상한은 `frameSize`다. 프레임을 다 처리해 버퍼가 비면 큰 버퍼는 놓는다.
- `handle()`부터 하위 핸들러까지 모두 `(buf, off, len)` 뷰를 받게 바꾼다. 그러면 프레임 복사와 payload 복사가 둘 다 사라진다. 복사는 꼭 필요한 두 곳에만 남는다. AUTH credentials(애플리케이션으로 나감)와 PING 8바이트(`--release 8`에는 범위 지정 `Arrays.equals`가 없음)다.

이 방식은 Netty `MERGE_CUMULATOR`의 "누적분이 비었으면 입력을 그대로 쓴다" 빠른 경로, Go `x/net/http2` Framer의 "다음 호출 전까지만 유효한 뷰" 계약, rustls `SliceInput`(호출자 버퍼에서 바로 deframe)을 sans-IO `byte[]` API에 맞게 합친 것이다(2절 참고).

## 1. 현재 코드 분석

### 1.1 비용

| 위치 | 동작 | 비용 |
|---|---|---|
| `receive()` L159 | `receiveBuffer = Frames.concat(receiveBuffer, bytes)` | 매 호출마다 누적분 전체와 새 청크를 새 배열로 복사 |
| `checkPreface()` L529 | `copyOfRange(preface.length, len)` | 나머지 전체 복사(연결당 1회) |
| `readFrame()` L604 | `copyOfRange(0, frameSize)` | 프레임 복사 |
| `readFrame()` L605 | `copyOfRange(frameSize, len)` | **프레임마다 나머지 전체 복사** |
| `handle()` L648 | `copyOfRange(frame, HEADER_SIZE, len)` | 제어 프레임 payload 복사 |
| `handleServerAuth()` L865 | credentials 복사 | 꼭 필요함(애플리케이션으로 나감) |

최악 경우는 두 가지다.

- **큰 청크 하나에 작은 프레임이 많은 경우**(N바이트, 프레임 크기 f): L605 때문에 복사량이 Σ(N − k·f) ≈ N²/(2f)이다. 예를 들어 N = 1 MiB, f = 20 B이면 약 26 GB를 복사하고 할당도 약 5만 번 일어난다.
- **큰 프레임 하나가 작은 조각으로 오는 경우**(프레임 크기 F, 조각 크기 c): L159 때문에 F²/(2c)를 복사한다. 예를 들어 F = 1 MiB, c = 1 KiB이면 약 512 MB다.

`maxFrameSize` 범위는 16 KiB~1 MiB다(`HelloParams.MAX_FRAME_SIZE_LOWER_LIMIT`/`UPPER_LIMIT`).

### 1.2 프레임과 payload 바이트가 밖으로 나가는가 (뷰를 써도 안전한가)

코드를 직접 확인한 결과는 다음과 같다.

- `Event.java`에는 `byte[]` 필드가 없다. `Event.requestReceived(streamId, type)`와 `Event.result(streamId, status, certainty)`에는 payload가 없다. 애플리케이션 프레임의 payload는 지금 애플리케이션으로 전달되지 않는다.
- `handleResponse`와 `handleRequest`는 헤더 필드만 읽는다. `scanExtensions`는 읽기만 한다.
- HELLO의 `peerLimitsOf(payload)`는 읽기만 한다.
- PING의 경우 `endOldestWait(payload)`는 `Arrays.equals`로 비교만 한다. `Frames.control(PING, ACK, payload)`는 `ByteArrayOutputStream`에 다시 쓰므로 복사본이 생긴다. 어느 쪽도 원본을 보관하지 않는다.
- AUTH의 credentials는 이미 `copyOfRange` 후 `Authenticator`로 넘긴다. mechanism은 `new String(...)`으로 복사된다.
- 어떤 필드에도 프레임 배열을 저장하지 않는다. 처리는 모두 `receive()` 호출 안에서 동기적으로 끝난다.

따라서 **`receive()`가 반환되기 전까지는 호출자 배열을 뷰로 읽어도 안전하다.** 호출자가 반환 후 배열을 재사용해도 문제가 없으려면 반환 시점에 아직 처리하지 못한 꼬리 바이트만 복사해 두면 된다.

**나중에 바뀔 수 있는 점:** 앞으로 이벤트에 payload를 싣게 되면 그 경계에서 복사 1회가 불가피하다. 피하려면 Go http2처럼 "다음 `receive`/`drainEvents` 호출 전까지만 유효"하다는 계약을 공개 API에 넣어야 한다. 그러나 이 계약은 sans-IO 이벤트 큐(`drainEvents()`가 나중에 꺼냄) 구조와 잘 맞지 않는다. 현실적으로는 "애플리케이션에 넘길 payload만 정확한 크기로 1회 복사"하게 된다. 그래도 지금보다 복사 횟수가 적다.

### 1.3 `--release 8`이 선택지를 좁히는 지점

- `VarHandle`(`MethodHandles.byteArrayViewVarHandle`)은 Java 9부터 있다. **사용할 수 없다.**
- `Arrays.equals(a, aFrom, aTo, b, bFrom, bTo)`와 `Arrays.compare`/`mismatch`의 범위 버전도 Java 9부터 있다. 사용할 수 없다.
- `ByteBuffer.slice(int, int)`는 Java 13부터 있다. 사용할 수 없다. `slice()`와 `duplicate()`는 Java 8에도 있다.
- `ByteBuffer.flip()`과 `position(int)` 등이 `ByteBuffer`를 반환하는 공변 오버라이드는 Java 9부터다. `--release 8`로 컴파일하면 올바르게 `Buffer`를 반환하는 메서드로 링크되므로 문제는 없다. 다만 코드에서 체이닝을 쓸 수 없다.
- 길이 필드 읽기에는 지금의 수동 시프트(`Frames.readUnsignedInt(byte[], at)`)를 그대로 쓰면 된다. 이 함수는 이미 offset을 받으므로 뷰 방식과 바로 맞는다. `ByteBuffer.wrap(arr).getInt(at)`(빅엔디언 기본값)도 쓸 수 있지만 얻는 게 없고 객체만 하나 더 생긴다.

## 2. 업계 사례 (커밋 고정 링크)

### Netty `ByteToMessageDecoder` (4.1, `42484cc`)

- `MERGE_CUMULATOR`([L83](https://github.com/netty/netty/blob/42484ccf42c1c7b6a4d2b2b1714181803fc37c2a/codec/src/main/java/io/netty/handler/codec/ByteToMessageDecoder.java#L83)): 하나의 `ByteBuf`에 memcpy로 합친다. [L92](https://github.com/netty/netty/blob/42484ccf42c1c7b6a4d2b2b1714181803fc37c2a/codec/src/main/java/io/netty/handler/codec/ByteToMessageDecoder.java#L92)의 주석은 "If cumulation is empty and input buffer is contiguous, use it directly"이다. 누적분이 비어 있으면 입력을 복사하지 않고 그대로 쓴다. **추천안의 빠른 경로와 같은 발상이다.** 이것이 기본값이다.
- `COMPOSITE_CUMULATOR`([L119-123](https://github.com/netty/netty/blob/42484ccf42c1c7b6a4d2b2b1714181803fc37c2a/codec/src/main/java/io/netty/handler/codec/ByteToMessageDecoder.java#L119)): "so do no memory copy whenever possible". Javadoc은 인덱싱이 복잡해 `MERGE_CUMULATOR`보다 느릴 수 있다고 경고한다. 업계 최대 프레임워크조차 기본값으로 체인 대신 연속 버퍼를 고른 셈이다.
- compaction 정책: `discardAfterReads = 16`([L191](https://github.com/netty/netty/blob/42484ccf42c1c7b6a4d2b2b1714181803fc37c2a/codec/src/main/java/io/netty/handler/codec/ByteToMessageDecoder.java#L191))이고 read 16회마다 `discardSomeReadBytes()`를 부른다([L316](https://github.com/netty/netty/blob/42484ccf42c1c7b6a4d2b2b1714181803fc37c2a/codec/src/main/java/io/netty/handler/codec/ByteToMessageDecoder.java#L316)). `AbstractByteBuf.discardSomeReadBytes()`는 `readerIndex >= capacity() >>> 1`일 때만 앞으로 당긴다([L245](https://github.com/netty/netty/blob/42484ccf42c1c7b6a4d2b2b1714181803fc37c2a/buffer/src/main/java/io/netty/buffer/AbstractByteBuf.java#L245)). 즉 "절반 이상 소비했을 때 compact"한다.
- 프레임 추출: `LengthFieldBasedFrameDecoder.extractFrame`은 `buffer.retainedSlice(index, length)`로 복사 없는 뷰를 만든다([L506](https://github.com/netty/netty/blob/42484ccf42c1c7b6a4d2b2b1714181803fc37c2a/codec/src/main/java/io/netty/handler/codec/LengthFieldBasedFrameDecoder.java#L506)). 참조 카운트가 있어서 가능한 방식이다. strand는 프레임이 밖으로 나가지 않으므로 참조 카운트 없이 같은 효과를 얻는다.

### Kafka `NetworkReceive` (trunk, `8f4cc1d`)

- 4바이트 길이를 `ByteBuffer.allocate(4)`에 먼저 읽는다([L62](https://github.com/apache/kafka/blob/8f4cc1d80036105ffc79388dc659a9b325996943/clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java#L62), [L91](https://github.com/apache/kafka/blob/8f4cc1d80036105ffc79388dc659a9b325996943/clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java#L91)). 그다음 `memoryPool.tryAllocate(requestedBufferSize)`로 정확한 크기의 버퍼를 할당한다([L103](https://github.com/apache/kafka/blob/8f4cc1d80036105ffc79388dc659a9b325996943/clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java#L103)).
- 정확한 크기로 할당하면 프레임당 할당 1회, 복사 0회(소켓에서 바로 읽음)다. 하지만 공격자가 적어 보낸 길이만큼 메모리를 미리 잡는다. Kafka는 `MemoryPool`이 전체 메모리 사용량을 막아 줘서 이 방식을 쓸 수 있다. **strand에는 풀이 없으므로** 그대로 가져오면 안 된다. 1 MiB라고 적은 헤더 뒤에 바이트를 조금씩만 보내면 연결마다 1 MiB가 묶인다.
- 또 Kafka는 소켓에서 직접 읽는다(pull). strand는 이미 채워진 `byte[]`를 받는다(push). 그래서 strand에서는 "소켓에서 정확한 크기 버퍼로 바로 읽기"가 "호출자 배열에서 제자리 파싱"에 해당한다.

### gRPC-java `MessageDeframer` (master, `1bb4732`)

- `unprocessed`(`CompositeReadableBuffer`)에서 `nextFrame.addBuffer(unprocessed.readBytes(toRead))`로 프레임을 모은다([L358](https://github.com/grpc/grpc-java/blob/1bb47327a04a684c4845db6d5e613386d351f6f4/core/src/main/java/io/grpc/internal/MessageDeframer.java#L358)). `CompositeReadableBuffer.readBytes`는 통째로 소비한 구성 버퍼를 `readableBuffers.poll()`로 객체째 옮기고([L223](https://github.com/grpc/grpc-java/blob/1bb47327a04a684c4845db6d5e613386d351f6f4/core/src/main/java/io/grpc/internal/CompositeReadableBuffer.java#L223)) 경계에 걸친 마지막 버퍼만 나눈다. 본문은 `ReadableBuffers.openStream(nextFrame, true)`로 전달한다([L430](https://github.com/grpc/grpc-java/blob/1bb47327a04a684c4845db6d5e613386d351f6f4/core/src/main/java/io/grpc/internal/MessageDeframer.java#L430)).
- 청크 체인이 성립하는 이유는 전송 계층(Netty, OkHttp)이 버퍼 소유권을 넘겨주기 때문이다. strand의 `receive(byte[])`는 호출자가 배열을 재사용할 수 있으므로 체인에 넣으려면 결국 복사해야 한다.

### Okio `Segment` (OkHttp 기반, `f0e3f47`)

- 8 KiB 세그먼트(`SIZE = 8192`, [L181](https://github.com/square/okio/blob/f0e3f479f83531332de076ed39bc001a5618d538/okio/src/commonMain/kotlin/okio/Segment.kt#L181))를 이중 연결 리스트로 잇고 풀에서 재사용한다. `SHARE_MINIMUM = 1024`([L184](https://github.com/square/okio/blob/f0e3f479f83531332de076ed39bc001a5618d538/okio/src/commonMain/kotlin/okio/Segment.kt#L184))이므로 1 KiB 이상일 때만 배열을 공유하고 그보다 작으면 복사한다. `compact()`는 꼬리와 그 앞 세그먼트가 둘 다 절반 미만으로 차 있을 때 합친다.
- "작은 복사는 공유보다 싸다"는 경험칙이 코드에 상수로 박혀 있다. strand의 최대 프레임은 1 MiB이고 제어 프레임은 수십 바이트이므로 세그먼트 체인이 얻는 이득은 작다.

### Go

- `bufio.Reader`(`be57ebb`): `r`/`w` 인덱스를 쓰는 단일 버퍼다. `fill()`에서만 "Slide existing data to beginning"(`copy(b.buf, b.buf[b.r:b.w])`)한다([L100-102](https://github.com/golang/go/blob/be57ebb0f3c3ec056ccf39107a5d1380a51ff1c2/src/bufio/bufio.go#L100)). 즉 **공간이 모자랄 때만 compact**한다. `Peek` 결과는 "stop being valid at the next read call"이다.
- `x/net/http2` Framer(`3de643d`): "The returned Frame is only valid until the next call to ReadFrame"([L561](https://github.com/golang/net/blob/3de643db506517b3e652a13cc0b56034cea6b8e6/http2/frame.go#L561)), "The caller must not retain the returned memory"([L640](https://github.com/golang/net/blob/3de643db506517b3e652a13cc0b56034cea6b8e6/http2/frame.go#L640)). 프레임 크기 이상의 `readBuf`를 재사용하고, 모자라면 `make([]byte, size)`로 정확한 크기로 다시 만든다([L459](https://github.com/golang/net/blob/3de643db506517b3e652a13cc0b56034cea6b8e6/http2/frame.go#L459)). 같은 파일의 TODO는 이 방식이 메모리를 묶어 둔다("less memory-pinning")는 점을 스스로 인정한다.

### Rust sans-IO

- tokio-util `LengthDelimitedCodec`(`99dbe54`; h2 등이 쓰는 `Decoder` 패턴): 헤더를 읽으면 `src.reserve(n.saturating_sub(src.len()))`로 **선언된 길이만큼 미리 확보**한다([L555](https://github.com/tokio-rs/tokio/blob/99dbe548b7046915cd56e570e31604e1f8fa83cd/tokio-util/src/codec/length_delimited.rs#L555)). 프레임은 `src.split_to(n)`으로 복사 없이 떼어 낸다([L567](https://github.com/tokio-rs/tokio/blob/99dbe548b7046915cd56e570e31604e1f8fa83cd/tokio-util/src/codec/length_delimited.rs#L567)). 미리 확보하는 방식은 `max_frame_length`(기본 8 MiB)에 기대므로 Kafka와 같은 위험이 있다.
- rustls(`0d7f9b3`) `conn/receive.rs`의 두 입력 방식:
  - `VecInput`: `used` 인덱스를 쓰는 단일 `Vec`이다. `discard(taken)`가 `copy_within(taken..used, 0)`로 앞으로 당긴다([L935](https://github.com/rustls/rustls/blob/0d7f9b323ec71c2f359c8e490a13a57e7b1a0f53/rustls/src/conn/receive.rs#L935)). discard 양은 deframer가 모아 두었다가 한꺼번에 적용한다. `prepare_read()`는 상한(64 KiB)을 두고 4 KiB씩 키우며, 비었거나 상한을 넘으면 `shrink_to`로 줄인다([L974-1001](https://github.com/rustls/rustls/blob/0d7f9b323ec71c2f359c8e490a13a57e7b1a0f53/rustls/src/conn/receive.rs#L974)). **받은 만큼 키우고 비면 줄인다**는 점이 추천안의 성장 정책과 같다.
  - `SliceInput<'a>`([L1044](https://github.com/rustls/rustls/blob/0d7f9b323ec71c2f359c8e490a13a57e7b1a0f53/rustls/src/conn/receive.rs#L1044)): 호출자 버퍼에서 바로 deframe하고 `into_used()`로 소비한 바이트 수만 돌려준다. 남은 꼬리는 호출자가 가진다. 내부 버퍼가 아예 없는 극단적인 sans-IO 형태다.

## 3. JDK만으로 가능한 선택지

| # | 방식 | 요지 |
|---|---|---|
| A | 현재 방식 | concat하고 프레임마다 앞과 뒤를 `copyOfRange` |
| B | `byte[]`와 r/w 인덱스, 매 `receive()` 끝에 compact | 남은 바이트를 receive마다 1회 당김 |
| C | B와 같되 `r ≥ cap/2`일 때만 compact | Netty `discardSomeReadBytes` 정책 |
| D | B와 같되 공간이 모자랄 때만 compact | Go `bufio.fill` 정책 |
| E | heap `ByteBuffer`의 `compact()`/`flip()`, `slice()`/`duplicate()` | 의미는 B~D와 같고 API만 다르다 |
| F | 링 버퍼 | compact는 없지만 경계를 넘는 헤더나 프레임은 둘로 갈라진다 |
| G | 청크 리스트 `Deque<byte[]>`/`Deque<ByteBuffer>` | gRPC와 Okio 방식. 호출자 배열은 복사해야 넣을 수 있다 |
| H | `ByteArrayOutputStream` 누적 | 앞에서 소비할 수 없어 `toByteArray()`가 매번 전체를 복사한다 |
| **I** | **제자리 파싱과 경계 프레임 하나만 누적(추천)** | 2절의 Netty 빠른 경로, Go 뷰 계약, rustls 성장 정책을 합친 것 |
| J | rustls `SliceInput` 방식: `int receive(...)`가 소비량을 반환하고 꼬리는 호출자가 보관 | 내부 버퍼 0. 대신 API를 깨고 부담을 호출자에게 넘긴다 |

공통 부속은 모든 방식에 붙일 수 있다. `handle(byte[] buf, int off, int len)` 뷰로 프레임 복사와 payload 복사를 없애는 것이다. 아래 표는 이 부속을 적용했다고 가정한다(A만 제외).

## 4. 비교

N = 한 청크의 바이트 수, f = 작은 프레임 크기, F = 큰 프레임 크기, c = 조각 크기.

| 방식 | 바이트당 복사(상각) | 할당 | 작은 프레임 다수가 큰 청크 하나로 올 때 | 큰 프레임이 작은 조각으로 올 때 | 메모리 보유 | 복잡도 | 호출자 배열 안전성 | sans-IO `byte[]` 적합성 |
|---|---|---|---|---|---|---|---|---|
| A 현재 | 2 + O(N/f) | 프레임당 3 + receive당 1 | **O(N²/f)** | **O(F²/c)** | 남은 바이트만큼 | 낮음 | 안전(전부 복사) | 그대로 |
| B 매번 compact | 1 + 남은 바이트 | 성장할 때만(두 배) | O(N) (receive마다 남은 꼬리 < 1프레임 당김) | 두 배 성장이면 O(F), 정확 크기 성장이면 **O(F²/c)** | 최대 용량 유지 | 낮음 | 안전 | 좋음 |
| C 절반 이상일 때 compact | ≈1 + 상각 O(1) | 성장할 때만 | O(N) | O(F) | 최대 용량, 이후 줄이기 필요 | 낮음 | 안전 | 좋음 |
| D 모자랄 때만 compact | ≈1 + 상각 O(1) | 성장할 때만 | O(N) | O(F) | 최대 용량, 이후 줄이기 필요 | 낮음 | 안전 | 좋음 |
| E ByteBuffer | B~D와 같음 | 같음 + 뷰 객체 | B~D와 같음 | 같음 | 같음 | 중간(position/limit 상태, Java 8 API 제약) | 안전 | 보통 |
| F 링 버퍼 | 1 + 경계를 넘는 프레임 재조립 | 고정(≥ maxFrameSize) | O(N) | O(F) | **항상 ≥ maxFrameSize(최대 1 MiB/연결)** | 높음(랩 처리, 헤더 분할) | 안전 | 보통 |
| G 청크 리스트 | 1(입력 방어 복사) + 경계 프레임 재조립 1 | 청크당 1 + 노드 | O(N) | O(F), 조각 수만큼 노드 | 받은 만큼 | 높음(교차 읽기) | 복사해야만 안전 | 나쁨(소유권 이전 모델용) |
| H BAOS | ≥2 | 매 `toByteArray` | O(N²/f) 가능 | O(F²/c) 가능 | — | 낮음 | 안전 | 나쁨 |
| **I 제자리+경계** | **청크 안 프레임 0, 경계 프레임만 1** | **0(정상 상태). 경계 프레임당 O(log) 두 배 성장** | **복사 0, 할당 0** | **O(F)**(두 배 성장이라 재복사 합 < 2F) | **미완성 프레임 하나만. 비면 반환** | 중간 | 안전(반환 전에 꼬리 복사) | **매우 좋음(API 유지)** |
| J SliceInput | 0 | 0 | 0 | 호출자 몫 | 0(호출자 몫) | 중간 + 호출자 부담 | 호출자 책임 | API 변경 |

메모:

- B~D의 큰 프레임 비용은 성장 정책이 결정한다: 큰 프레임이 끝날 때까지 r = 0이라 compact는 일어나지 않지만, 용량을 "정확히 필요한 만큼"만 키우면 매 receive가 재할당·전체 복사가 되어 지금의 concat과 같아진다. 두 배 성장이어야 O(F)다.
- I의 "경계 프레임만 1회 복사"는 C/D의 "모든 바이트 1회 복사"보다 엄밀히 좋다. 컨트롤러와 HELLO, 작은 RPC 위주라면 대부분 프레임이 청크 하나 안에 들어오므로 실제 복사량은 0에 가깝다.
- I는 누적 버퍼에 미완성 프레임 하나만 두므로 C/D처럼 "언제 compact할지" 정책 자체가 필요 없다. 프레임 하나를 완성하면 누적 버퍼는 반드시 비고, 그 즉시 남은 입력은 다시 제자리 경로로 간다.

## 5. 추천안 상세 (I)

### 5.1 데이터 구조

```java
// Endpoint 필드 (receiveBuffer 대체)
/** 청크 경계에 걸친 미완성 단위(서문 또는 프레임 1개)의 앞부분. [0, pendingLength)만 유효하다. */
private byte[] pending = EMPTY;          // 재사용. 비면 크기에 따라 EMPTY로 되돌림
private int pendingLength;               // 0이면 누적 없음 → 제자리 경로
private static final int RETAIN_LIMIT = 64 * 1024; // 비었을 때 이보다 크면 놓는다(rustls prepare_read와 같은 취지)
```

### 5.2 흐름

```java
public void receive(byte[] bytes) {           // (선택) receive(byte[] b, int off, int len) 오버로드 추가
    ...
    int at = 0, end = bytes.length;
    if (pendingLength > 0) {
        at = fillPending(bytes, at, end);      // "다음 결정에 필요한 바이트 수"만큼만 pending에 붙인다
        if (pendingLength < required(pending, 0, pendingLength)) return;   // 아직 모자람
        consumeUnit(pending, 0, pendingLength); // 서문 확인 또는 프레임 1개 처리
        releasePending();                       // pendingLength = 0, 용량 > RETAIN_LIMIT이면 EMPTY
    }
    while (!transportClosed && at < end) {
        int need = required(bytes, at, end - at); // 길이 4B → (첫 프레임이면 HEADER_SIZE / 길이 오류면 TYPE_END) → frameSize
        if (end - at < need) break;
        at += consumeUnit(bytes, at, need);     // 제자리 처리. 복사 없음
    }
    if (!transportClosed && at < end) stash(bytes, at, end); // 꼬리만 복사(미완성 단위 하나 분량 미만)
}
```

- `required(buf, off, len)`는 지금의 `checkPreface()`/`readFrame()` 분기를 그대로 옮긴 순수 함수다. 반환값은 "판정하려면 최소 몇 바이트가 필요한가"다. 4(길이 필드), `HEADER_SIZE`(첫 프레임 검사), `TYPE_END`(길이 오류지만 CLOSE인지 봐야 함), `frameSize`, 서문 6/8바이트 가운데 하나다. 판정이 즉시 닫기이면 그 신호를 따로 돌려준다. 이렇게 "결정 지점" 단위로 묶어야 도착 단위에 따라 결과가 달라지지 않는다. 이는 기존 L594 주석이 말하는 불변식과 같다.
- **pending 성장 정책**: `fillPending`은 `need`까지 한 번에 할당하지 않는다. `max(2 × 용량, pendingLength + 이번에 붙일 양)`으로 키우되 `need`(≤ `maxFrameSize`)를 상한으로 둔다. 1 MiB라고 적은 헤더 뒤에 1바이트씩 흘려 보내는 공격에도 메모리 사용량은 **실제로 받은 바이트에 비례**한다. 재복사 합은 두 배 성장이므로 2F 미만이다. Kafka와 tokio식 정확 크기 선할당은 풀이나 상한 없이는 쓰지 않는다.
- pending은 지금처럼 `closeTransport()`(L1087 `receiveBuffer = new byte[0]`)에서 함께 비운다(`pending = EMPTY; pendingLength = 0`).

### 5.3 시그니처 변경 목록 (빠뜨리면 조용한 버그가 되는 곳)

절대 인덱스는 모두 `off + k`로 바뀌어야 한다.

- `Frames`: `typeOf`, `flagsOf`, `statusOf`, `streamIdOf`, `extensionsLengthOf`를 `(byte[] b, int off)`로 바꾼다. `readUnsignedInt`/`readLong`/`readUnsignedShort`는 이미 `(bytes, at)`이므로 호출부에서 `off`를 더하면 된다. `concat`은 삭제한다(저장소 전체에서 L159만 씀).
- `Endpoint`:
  - `matchesPrefacePrefix(byte[] b, int off, int len)`, `isFirstFrameHeader(b, off)`, `rejectBadLength(b, off)`
  - `handle(byte[] b, int off, int len)`: 본문의 `frame.length`를 모두 `len`으로 바꾼다(L619 등).
  - `handleApplication(b, off, len, …)`, `handleResponse(b, off, …)`
  - `scanExtensions(b, off, extensionsLength)`: 시작 위치를 `off + HEADER_SIZE`로 바꾼다.
  - `handleClose(b, off, len, …)`: `payloadSize = len - HEADER_SIZE - extensionsLength`, `readLong(b, off + HEADER_SIZE + CLOSE_ERROR_CODE_SIZE)`, `readUnsignedInt(b, off + HEADER_SIZE)`
  - L648의 payload `copyOfRange`를 삭제한다. 하위 핸들러는 `(b, payloadOff, payloadLen)`을 받는다.
    - `handleHello(ack, b, po, pl)` / `peerLimitsOf(b, po, pl)`: 제자리에서 읽는다.
    - `handleReset(streamId, b, po, pl)`: `pl != 4` 검사 후 `readUnsignedInt(b, po)`
    - `handleAuth`/`handleServerAuth`: mechanism은 `new String(b, po+1, n, US_ASCII)`. **credentials는 `copyOfRange` 유지(애플리케이션으로 나감)**
    - `handleClientAuthAck(b, po, pl)`: 제자리에서 읽는다.
    - `handlePing`: 길이 8을 검사한 뒤 **8바이트만 `copyOfRange`**. `endOldestWait`의 `Arrays.equals(byte[], byte[])`는 Java 8에 범위 버전이 없고, 복사 비용이 무시할 만하다.
  - `receiveMessage()`(WebSocket): `handle(message, 0, message.length)`. 지금도 복사가 없고 바뀌는 것도 없다.

### 5.4 주의점

- **재진입 불가**: `Authenticator.authenticate()`는 `handle()` 안에서 동기로 불린다. 여기서 같은 `Endpoint`의 `receive()`를 부르면 지역 오프셋 `at`과 `pending`이 꼬인다. 현재 구현도 `receiveBuffer`가 꼬이므로 새로 생기는 위험은 아니다. 다만 뷰 방식에서는 증상이 더 미묘해진다. `receiving` 플래그로 `IllegalStateException`을 던지거나 javadoc에 명시하기를 권장한다.
- **(선택) `receive(byte[] b, int off, int len)` 오버로드 추가**: 전송 계층이 `ByteBuffer`나 큰 읽기 버퍼의 일부를 넘길 때 `copyOf`를 하지 않아도 된다. 기존 `receive(byte[])`는 이 오버로드에 위임한다. 하위 호환 추가다.
- **회귀 테스트(꼭 필요)**: 같은 바이트 스트림(서문, HELLO, AUTH, 여러 요청과 응답, PING, RESET, CLOSE, 길이 오류 CLOSE, 첫 프레임 오류)을 ① 통째로 ② **가능한 모든 단일 분할 지점**에서 둘로 ③ 1바이트씩 ④ 시드 고정 랜덤 청크로 넣는다. 그리고 `drainEvents()`와 outbox 시퀀스가 모두 같은지 단언한다. 서문 6/8바이트, `LENGTH_FIELD_SIZE`, `TYPE_END`, `HEADER_SIZE`, `frameSize` 결정 지점이 핵심이다. 반환 후 호출자 배열을 덮어써도 결과가 같은지 검사하는 케이스도 넣는다.

## 6. 기각한 대안과 이유

- **C/D(Netty, bufio식 단일 누적 버퍼)**: 충분히 좋고 단순하다. 하지만 모든 바이트를 1회 복사하며, I는 같은 복잡도 수준에서 정상 경로 복사를 0으로 만든다. I의 pending 버퍼가 사실상 "compact가 필요 없는 D"다.
- **E(ByteBuffer)**: 의미상 B~D와 같다. position/limit 상태와 Java 8 API 공백(`slice(int,int)`, 공변 반환) 때문에 손해다. `getInt`의 이점은 수동 시프트와 비교해 확인되지 않았다.
- **F(링 버퍼)**: 프레임 하나를 연속 바이트로 핸들러에 넘겨야 하므로 경계를 넘을 때 재조립이 필요하다. 용량도 `maxFrameSize`에 고정돼 연결마다 최대 1 MiB를 상시 보유한다.
- **G(청크 리스트)**: gRPC와 Okio는 소유권이 넘어오는 버퍼를 전제로 한다. strand는 호출자 배열을 재사용할 수 있어서 방어 복사가 필요하고, 그러면 이점이 사라진다. Netty 문서도 composite가 더 느릴 수 있다고 경고한다.
- **J(rustls SliceInput)**: 내부 복사가 완전히 0이지만 공개 API(`void receive(byte[])`)를 깨고 꼬리 관리 부담을 모든 전송 어댑터에 떠넘긴다. I가 같은 이점의 거의 전부를 API 변경 없이 얻는다.

## 7. 확인하지 못한 것과 한계

- **벤치마크를 돌리지 않았다.** "오버헤드 없음"은 프레임당 분기 수와 복사, 할당 횟수 논증에 근거한다. JMH 측정 결과가 아니다(repo에 JMH 설정 없음).
- Java 8 JIT가 수동 시프트 4회를 단일 로드로 합치는지(`ByteBuffer.getInt`와 대비)는 확인하지 않았다. 어느 쪽이든 프레임당 몇 회라서 영향은 미미할 것으로 본다.
- Jetty, Undertow, quiche는 조사하지 않았다.
- 인용은 2절에 적은 커밋 SHA의 원문을 `gh api`(raw)로 받아 grep해 줄 번호를 확인했다. 다만 Netty `MERGE_CUMULATOR`의 빠른 경로 세부(반환값이 입력 그 자체인지)와 gRPC `ReadableBuffers.openStream`이 내부에서 복사하는지는 요약 도구를 거친 내용이라 줄 단위로 끝까지 읽지는 않았다.
- rustls에서 discard를 언제 일괄 적용하는지(`take_discard()` 호출 시점)는 `receive.rs` L182-188 등 일부만 확인했다.
- `Frames.concat`의 호출처는 저장소 전체 grep으로 `Endpoint.java` L159 하나뿐임을 확인했다(테스트·conformance의 `concat`은 별개 헬퍼).
