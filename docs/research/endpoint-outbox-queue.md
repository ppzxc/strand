---
ticket: 200
label: wayfinder:task
checked: 2026-11-09
---

# Q8·Q9: Endpoint 쓰기 대기열(outbox) 자료구조 선택

대상: `strand-core/src/main/java/io/github/ppzxc/strand/core/internal/Endpoint.java`, `OutboundMessage.java`
빌드 제약: `--release 8` (`build-logic/src/main/kotlin/strand.java-conventions.gradle.kts:21`, `strand.library-conventions.gradle.kts:38`). 그래서 `SequencedMap.lastEntry()`(JDK 21)는 쓸 수 없다.

## 결론

**intrusive 이중 연결 리스트 하나로 바꾼다.** 내부 노드 `Outgoing`에 `prev/next`를 두고, 경계 포인터 `lastControl` 하나와 CLOSE 노드 포인터 하나를 둔다.

- `send`·취소·`removeFromOutbox`는 모두 O(1)이다.
- `drainMessages`는 O(n)이고 identity set을 쓰지 않는다.
- 취소된 항목을 붙잡아 두지 않는다.
- 현재 순서를 그대로 재현한다(아래 스케치의 `unlink` 끝 한 줄 포함).

두 큐 + tombstone 방식도 쓸 만하다. 다만 CLOSE 위치를 seq로 표시해야 하고, tombstone 압축(compaction)도 따로 넣어야 해서 더 복잡하다.

---

## 1. 현재 코드에서 확인한 사실

### 1.1 outbox의 모양(불변식)

outbox를 바꾸는 곳은 다음뿐이다.

| 줄 | 동작 |
|---|---|
| 142 | 서문 `send(Frames.preface())`. 서문은 `isApplication()==false`이므로 제어 쪽에 들어간다. |
| 1234 | 요청 프레임을 맨 뒤에 붙인다(`sendRequest`). |
| 1285 | 애플리케이션 프레임(응답 등)을 맨 뒤에 붙인다(`send`). |
| 1288-1292 | 제어 프레임을 `firstApplicationIndex()` 위치에 넣는다. CLOSE이면 `max(firstApp, afterLastUnwrittenRequest())` 위치다. |
| 1052 | WebSocket Close를 맨 뒤에 붙인다(`closeTransportAfterNoError`). |
| 419, 1183, 1330 | 요청 메시지만 지운다(`removeIf`). |
| 490-491 | drain이 전부 꺼내고 비운다. |

따라서 outbox는 언제나 다음 모양이다.
`[제어 런][애플리케이션 런 (그 안에 CLOSE가 하나 끼어 있을 수 있음)][WS Close (선택, 맨 끝)]`

- 제어 프레임끼리는 넣은 순서(FIFO)를 지킨다. 새 제어 프레임은 제어 런의 끝, 곧 첫 애플리케이션 프레임 앞에 들어간다.
- 애플리케이션 프레임끼리도 FIFO다.
- CLOSE는 쓰기 전 요청 가운데 살아 있는 마지막 것 바로 뒤에 들어간다. 그런 요청이 없으면 제어 런 끝에 들어간다.

### 1.2 CLOSE는 연결당 한 번만 보낸다

`close()`(450)와 `closeWithError()`(1071)는 둘 다 `closeExchanged`로 막혀 있다. 따라서 CLOSE를 넣는 데 O(n)이 한 번 들어도 문제가 되지 않는다.

Q8의 진짜 비용은 다른 데 있다. `refuseUnhandedRequests()`(1020-1028)가 RESET을 보낼 때마다 `firstApplicationIndex()` 선형 탐색과 `ArrayList.add(i)` 원소 밀기가 일어난다. 이것이 O(E·n)이다. `afterLastUnwrittenRequest()`의 identity set은 연결당 한 번만 만든다.

### 1.3 `undrainedRequests`는 outbox 안의 요청 프레임 집합과 정확히 같다

반례를 찾지 못했다.

- **들어가는 곳:** 요청 프레임은 `sendRequest`(1233-1235)로만 outbox에 들어가고, 같은 자리에서 map에도 들어간다. 응답(368, 733, 735)은 `send()`를 거쳐 outbox에만 들어간다. 응답은 요청이 아니므로 map에 없는 것이 맞다.
- **빠지는 곳:** 모든 경로가 map과 outbox에서 함께 지운다.
  - 417-419 (`endOwnRequest`)
  - 673→1181-1183 (`handleReset`→`dropUnwrittenRequest`)
  - 787→1181 (`handleResponse`)
  - 993-1000 (`endRequestsAfter`)
  - 1095-1103 (`closeTransport`). `ownPendingStreams`의 키를 돌며 지운다. 그런데 `undrainedRequests`의 키는 언제나 `ownPendingStreams`의 부분집합이다. `endOwnStream`을 부르는 세 곳(414, 672, 778)이 모두 이어서 map에서도 지우기 때문이다. 그래서 빠지는 요청이 없다.
- **비우는 곳:** map은 drain(504)에서 outbox와 함께 비우고, `closeTransport`(1103)에서 비운다.

주의할 점이 하나 있다. 이 집합은 outbox 안 애플리케이션 프레임 전체가 아니다. 응답은 빠진다. 그래서 "첫 애플리케이션 프레임"과 "마지막 쓰기 전 요청"은 서로 다른 기준이다.

### 1.4 CLOSE와 그 뒤에 넣는 제어 프레임의 순서(현재 동작)

| 상황 | 현재 결과 |
|---|---|
| CLOSE를 쓰기 전 요청 R 뒤에 넣은 뒤 PING이나 RESET을 보냄 | 첫 애플리케이션 프레임(R 이하) 앞에 들어간다. 즉 CLOSE보다 앞이다. |
| CLOSE를 넣을 때 쓰기 전 요청이 없었음 | CLOSE는 제어 런의 끝에 있다. 뒤에 보낸 제어 프레임은 CLOSE 뒤에 들어간다. |
| CLOSE 앞의 요청이 모두 취소되어, CLOSE가 제어 런에 붙게 됨 | `firstApplicationIndex()`가 CLOSE를 지나간다. 그래서 뒤에 보낸 제어 프레임은 CLOSE 뒤에 들어간다. |

규격과 대조하면 다음과 같다.

- 3.6절은 SHOULD이고 제어 프레임끼리의 순서를 정하지 않는다.
- 3.5.1절의 "돌려보내는 RESET이 오류 CLOSE보다 먼저"는 `refuseUnhandedRequests()`를 CLOSE보다 먼저 부르는 호출 순서가 지킨다.
- 3.2.2절의 HELLO 먼저는 제어 FIFO가 지킨다.

따라서 위 표의 세 번째 줄은 규격상 어느 쪽이어도 된다. 오류 CLOSE이면 곧바로 `closeTransport()`가 쓰기 전 요청을 거둔다. 그래서 "CLOSE를 요청 뒤에"라는 규칙이 실제로 효과를 내는 것은 `NO_ERROR` CLOSE일 때다.

### 1.5 확인 대기(ackWaits)

`sendAwaitingAck`(1335-1337)는 `send()`로 넣은 메시지를 대기 항목에 함께 기록한다. 제어 메시지는 개별로 지워지지 않는다. `closeTransport`는 `ackWaits.clear()`를 한다. 따라서 "시작하지 않은 대기의 메시지는 언제나 현재 outbox에 있다"가 성립한다.

drain의 identity set(494)은 이 불변식만으로도 없앨 수 있다. 노드가 대기를 가리키는 역참조를 두면 더 직접적이다.

### 1.6 Q9: outbox를 고쳐도 남는 O(n)

outbox를 바꾸는 것만으로는 취소가 O(1)이 되지 않는다.

- `cancel(ticket)`(333-346)은 번호표로 `deferredSends`와 `ownPendingStreams`를 선형으로 훑는다. 그래서 취소 하나에 O(n)이 들고, n건 취소는 여전히 O(n²)이다.
- `requestState(ticket)`(1160-1172)도 같다.
- `expireRequests()`는 `advance()`마다 대기 스트림 전체를 훑는다(397-402).

이 비용들은 이번 범위 밖이다. 고치려면 별도 후속 작업으로 번호표→streamId 색인(`HashMap<Long ticket, Long streamId>`)을 두어야 한다.

이 설계로 O(1)이 되는 것은 기한 만료나 RESET·응답 수신처럼 **streamId로 들어오는 경로**의 outbox 제거다.

---

## 2. 업계 사례

| 구현 | 제어 프레임 우선 | 취소·닫힌 스트림의 대기 프레임 | 출처 |
|---|---|---|---|
| **nghttp2** | 큐를 종류별로 나눈다: `ob_urgent`(SETTINGS, PING) → `ob_reg`(RST_STREAM, GOAWAY, 기타) → `ob_syn`(요청 HEADERS, 동시 스트림 한도에 걸림) → DATA(우선순위 큐). pop은 이 순서로 확인한다. | **지연 삭제가 기본이다.** `nghttp2_session_close_stream`의 주석: "If item is queued, it will be deleted when it is popped (nghttp2_session_prep_frame() will fail)". 큐에 있던 요청 HEADERS를 취소하면 `aux_data.headers.canceled = 1`로 표시하고 `ob_reg`로 옮긴다. 옮기는 이유는 "so that it is disposed of ... on the next send without waiting for a stream slot"이다. 큐 자체는 `qnext`로 잇는 intrusive 단일 연결 리스트다. | `lib/nghttp2_session.c` 1052-1112, 1153-1191, 1347, 1483-1497, 2331-2373 (https://github.com/nghttp2/nghttp2/blob/master/lib/nghttp2_session.c) |
| **nghttp2의 GOAWAY와 대기 요청** | GOAWAY는 `ob_reg`에 들어가므로 `ob_syn`에 대기 중인 요청 HEADERS보다 먼저 나간다. 종료 중이면 대기 요청은 pop 시점에 `NGHTTP2_ERR_START_STREAM_NOT_ALLOWED`로 버린다(`session_predicate_request_headers_send`). | 위와 같음 | 같은 파일 1477-1497 |
| **Go x/net/http2** | `WriteScheduler`가 스트림과 무관한 프레임과 RST_STREAM을 `zero` 큐에 넣고 먼저 pop한다. 문서: "Control frames like SETTINGS and PING are written before DATA frames." `Pop` 문서: "Frames with a given wr.StreamID() are Pop'd in the same order they are Push'd, except RST_STREAM frames." | `CloseStream`: "Any frames queued on this stream should be discarded." 스트림별 큐를 map에서 떼어 통째로 버린다. 서버의 `writeFrame`도 닫힌 스트림의 프레임을 무시한다("we simply ignore the frame", RST_STREAM은 예외). 즉 스트림별 큐 일괄 폐기와 쓰기 시점 무시를 함께 쓴다. `writeQueue`는 slice 두 개로 만든 FIFO다. | `http2/writesched_common.go`, `writesched_random.go`, `writesched.go`, `server.go` (https://github.com/golang/net/tree/master/http2) |
| **Netty HTTP/2** | SETTINGS와 PING은 `frameWriter`로 곧바로 쓴다. RST_STREAM과 GOAWAY는 `lifecycleManager`→`frameWriter().writeRstStream/writeGoAway`로 곧바로 쓴다. 흐름 제어기를 거치지 않는다. DATA와 그 뒤의 HEADERS만 `Http2RemoteFlowController.addFlowControlled`로 보낸다. 주석: "Pass headers to the flow-controller so it can maintain their sequence relative to DATA frames." | 스트림마다 `ArrayDeque<FlowControlled> pendingWriteQueue`를 둔다. 스트림이 닫히면 `onStreamClosed`→`FlowState.cancel()`이 그 스트림 큐만 **즉시** 비운다. 전역 큐를 훑지 않는다. | `DefaultHttp2ConnectionEncoder.java`, `DefaultHttp2RemoteFlowController.java` 118-121, 291, 307, 501-518, `Http2ConnectionHandler.java` 849, 895, 931 (netty 4.1) |
| **hyper h2 (Rust)** | `prioritize.rs`의 `pending_send`는 스트림을 담는 큐이고, 스트림마다 `pending_send` 프레임 큐가 공용 `Buffer`를 쓴다. GOAWAY, SETTINGS, PING의 순서는 이 파일에 없다(미확인). | 로컬 리셋이면 `clear_queue`로 그 스트림 큐를 즉시 비운다(주석 "TODO: make this more efficient?"). 상대가 리셋했으면 지연 처리한다. 주석: "Instead of doing O(N) traversal through queue to remove, lets just ignore the stream here." | `src/proto/streams/prioritize.rs` (https://github.com/hyperium/h2) |
| **OkHttp `Http2Writer`** | 출력 큐가 없다. `rstStream`, `ping`, `goAway` 모두 `withLock { ... sink ... }`로 곧바로 쓴다. 순서는 호출 순서로 정해진다. | 해당 없음 | `okhttp/src/commonJvmAndroid/kotlin/okhttp3/internal/http2/Http2Writer.kt` 136-251 |
| **JDK `ScheduledThreadPoolExecutor`** | 해당 없음 | 기본은 지연 삭제다. Javadoc: "such a cancelled task is not automatically removed from the work queue until its delay elapses ... it may also cause unbounded retention of cancelled tasks. To avoid this, use setRemoveOnCancelPolicy". 기본값은 false다. | https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ScheduledThreadPoolExecutor.html |
| **Netty `HashedWheelTimer`** | 해당 없음 | `cancel()`은 상태만 바꾸고 `cancelledTimeouts`에 넣는다. 다음 tick에 `processCancelledTasks()`가 버킷(intrusive 이중 연결 리스트 `prev/next`)에서 O(1)로 떼어 낸다. 주석: "we will have a GC latency of max. 1 tick duration which is good enough". | `common/src/main/java/io/netty/util/HashedWheelTimer.java` 669-687 |

### 업계 사례에서 얻는 것

1. **큐를 나눈다.** 제어 프레임 우선은 큐를 종류별로 나누거나(nghttp2, Go) 큐를 거치지 않는 방식(Netty, OkHttp)으로 구현한다. 하나의 리스트에서 위치를 찾아 끼워 넣는 구현은 찾지 못했다.
2. **취소는 O(1)로 처리한다.** 방법은 둘 중 하나다.
   - 표시만 하고 꺼낼 때 버린다: nghttp2, h2의 상대 리셋, STPE 기본값.
   - 스트림별 큐나 intrusive 리스트에서 O(1)로 떼어 낸다: Netty, Go `CloseStream`, HashedWheelTimer.
   - 전역 큐를 선형으로 훑어 지우는 구현은 h2의 `clear_queue` 정도이고, 그것도 TODO로 남아 있다.
3. **지연 삭제에는 대가가 있다.** STPE Javadoc이 명시하듯 취소된 항목이 무한히 남을 수 있다. 그래서 Netty 타이머는 tick마다 실제로 떼어 내고, Go는 `maxQueuedControlFrames = 10000`으로 큐 크기를 제한한다.
4. **GOAWAY 위치는 strand와 다르다.** HTTP/2 구현은 GOAWAY를 대기 요청 앞에 보낸다(nghttp2 `ob_reg`가 `ob_syn`보다 먼저, Go는 `scheduleFrameWrite`에서 `needToSendGoAway`(server.go 1291)를 `writeSched.Pop`(1307)보다 먼저 확인). 대기 요청은 보내지 않고 버린다. 반면 strand 3.5절은 CLOSE 뒤에 온 요청을 상대가 버린다. 그래서 strand는 CLOSE를 "쓰기 전 요청 뒤"에 둔다. 이 규칙은 strand에만 있으므로 업계 구조를 그대로 가져올 수 없고, CLOSE 위치를 따로 처리해야 한다.

---

## 3. 후보 비교

n = outbox 길이, E = 돌려보낼 요청 수, k = 취소 건수다.

| 설계 | send(제어) | send(CLOSE) | 취소 1건(outbox 쪽) | drain | 취소 항목 유지 | 코드 복잡도 | 순서 규칙 위험 |
|---|---|---|---|---|---|---|---|
| **현재** ArrayList + 위치 탐색 + `removeIf` | O(n) 탐색 + O(n) 밀기 | O(n) | O(n) | O(n) + identity set | 없음 | 낮음 | 기준 |
| A. 두 `ArrayDeque`(제어/앱) + 즉시 `removeIf` | O(1) | O(1) + CLOSE 위치 표시 | O(n) | O(n) | 없음 | 낮음 | CLOSE 위치를 따로 표현해야 함 |
| B. 두 `ArrayDeque` + **tombstone**(노드에 `removed` 표시) + seq 기준 CLOSE 위치 | O(1) | O(r) 1회 | O(1) | O(n + 남은 tombstone) | **있음.** drain 없이 요청→취소를 반복하면 끝없이 쌓인다(`ownPendingStreams` 한도는 살아 있는 요청만 센다). 압축을 넣어야 막힌다. | 중간(seq, anchor, 압축 기준값) | 압축할 때 anchor 처리가 틀리기 쉽다 |
| C. `LinkedHashSet<OutboundMessage>` 두 개(동등성은 객체 동일성) | O(1) | 중간 위치에 넣을 수 없어 seq가 필요함 → 결국 map 값으로 seq를 둬야 함 | O(1) | O(n) | 없음 | 중간. 공개 클래스의 `equals` 미재정의에 기대므로 약하다 | anchor 요청이 지워지면 앞 원소를 O(1)로 찾을 수 없음 |
| **D. intrusive 이중 연결 리스트 하나 + `lastControl` 경계 + CLOSE 노드** | **O(1)** | O(r) 1회(연결당 한 번) | **O(1) 떼어 내기** | **O(n)**, identity set 없음 | **없음** | 중간(리스트 연산 약 40줄) | **가장 낮다.** 현재 모양을 그대로 표현한다(1.1절) |
| E. 제어 프레임이 앞에 몇 개인지 세는 카운터만 추가 | O(1) 탐색 + O(n) 밀기 | O(n) | O(n) | O(n) | 없음 | 매우 낮음 | 요청을 지울 때 카운터 갱신이 틀리기 쉽음. 원소 밀기 비용은 그대로 |

D를 고르는 이유는 세 가지다.

- B와 D는 둘 다 O(1)이다. 하지만 B는 지연 삭제의 대가(STPE의 경고)를 막으려고 압축을 따로 넣어야 한다. D는 HashedWheelTimer처럼 노드를 곧바로 떼어 내므로 그 문제가 처음부터 없다.
- D는 CLOSE를 실제 노드로 끼워 넣는다. 그래서 seq나 anchor 개념이 필요 없다.
- 노드 하나에 참조 2개(약 16바이트)가 더 들지만, 프레임 바이트 배열에 비하면 무시할 만하다.

D는 2절의 업계 결론(큐를 종류별로 나눈다)과 어긋나지 않는다. `lastControl` 경계가 있으므로 D는 논리적으로 제어 구간과 애플리케이션 구간이라는 두 큐를 한 리스트에 이어 붙여 둔 것이다. nghttp2와 Go가 쓰는 분리와 같다. 리스트 중간에 끼워 넣는 것은 CLOSE 하나뿐이고 연결당 한 번이다. 이 삽입은 HTTP/2에는 없고 strand 3.5절에만 필요한 것이다(2절 4번).

---

## 4. 권장 설계 D 스케치

```java
/** 쓰기 대기열 노드. OutboundMessage는 공개 타입이므로 불변으로 두고 감싼다. */
private static final class Outgoing {
    final OutboundMessage message;
    @Nullable AckWait wait;          // sendAwaitingAck가 채운다
    @Nullable Outgoing prev, next;
    Outgoing(OutboundMessage m) { message = m; }
}

private @Nullable Outgoing head, tail;
/** 제어 런의 마지막 노드. 없으면 null이다(새 제어 프레임은 head에 들어간다). */
private @Nullable Outgoing lastControl;
/** 대기열에 있는 CLOSE 노드. 없으면 null이다. */
private @Nullable Outgoing closeNode;
private int outboxSize;
private final Map<Long, Outgoing> undrainedRequests = new LinkedHashMap<>(); // 값 타입만 바뀐다

private Outgoing send(byte[] frame) {
    Outgoing node = new Outgoing(OutboundMessage.binary(frame));
    if (node.message.isApplication()) {
        linkLast(node);                                   // 애플리케이션 FIFO
    } else if (Frames.typeOf(frame) == FrameType.CLOSE && !undrainedRequests.isEmpty()) {
        linkAfter(lastValue(undrainedRequests), node);    // 3.5절: 마지막 쓰기 전 요청 바로 뒤. 연결당 1회 O(r)
        closeNode = node;
    } else {
        linkAfter(lastControl, node);                     // 3.6절: 제어 런의 끝 = 첫 애플리케이션 프레임 앞
        lastControl = node;
        if (Frames.typeOf(frame) == FrameType.CLOSE) closeNode = node;
    }
    return node;
}

// linkAfter(null, n)은 head에 넣는다. linkLast/linkAfter는 outboxSize++, unlink는 outboxSize--.
private void unlink(Outgoing n) {
    // prev/next 이어 붙이기, head/tail 갱신, n.prev = n.next = null, outboxSize--
    // 요청 노드만 떼어 내므로 n은 lastControl이나 closeNode가 아니다.
    // 현재 동작과 같게: CLOSE 앞의 애플리케이션 프레임이 모두 빠져 CLOSE가 제어 런에 붙으면
    // CLOSE를 제어 런의 끝으로 본다(현재 firstApplicationIndex가 CLOSE를 지나가는 것과 같다).
    if (closeNode != null && closeNode.prev == lastControl) lastControl = closeNode;
}
// sendRequest:  Outgoing n = new Outgoing(binary(frame)); linkLast(n); undrainedRequests.put(id, n);
// WS Close(1052): linkLast(new Outgoing(normalWebSocketClose()));  // 맨 끝. transportClosed 뒤에는 아무것도 붙지 않는다
// sendAwaitingAck: Outgoing n = send(frame); AckWait w = new AckWait(payload, n); n.wait = w; ackWaits.add(w);

private void unlinkRequest(long streamId) {               // 417-419, 1181-1183, 993-1000, 1095-1103를 대체
    Outgoing n = undrainedRequests.remove(streamId);
    if (n != null) unlink(n);                              // O(1)
}

public List<OutboundMessage> drainMessages() {
    List<OutboundMessage> drained = new ArrayList<>(outboxSize);
    long startAt = now + config.ackTimeoutMs();
    for (Outgoing n = head; n != null; n = n.next) {
        drained.add(n.message);
        if (n.wait != null && !n.wait.started) n.wait.start(startAt); // identity set을 쓰지 않는다
    }
    head = tail = lastControl = closeNode = null; outboxSize = 0;
    for (long id : undrainedRequests.keySet()) highestSentOwnStreamId = Math.max(highestSentOwnStreamId, id);
    undrainedRequests.clear();
    return drained;
}
```

- `lastValue(map)`는 `--release 8`이라 `values()`를 끝까지 돌아 마지막 값을 얻는다. CLOSE는 연결당 한 번이므로 비용 문제가 없다.
- `AckWait.message`는 `Outgoing`에서 `wait`를 거꾸로 가리키는 참조로 바뀐다. `AckWait`에는 메시지 필드가 필요 없어진다.
- ACK가 drain보다 먼저 와서 `endOldestWait`가 대기를 이미 목록에서 뺐다면, drain이 그 대기를 `start`해도 아무 효과가 없다. `ackWaits`에 없는 객체이기 때문이다.

### 순서 규칙이 지켜지는 방식

| 규칙 | 지키는 방식 |
|---|---|
| 제어 프레임은 쓰기 전 애플리케이션 프레임보다 먼저(3.6절) | `lastControl` 뒤에 넣는다. 제어 런 바로 뒤가 첫 애플리케이션 프레임이므로 `firstApplicationIndex()`와 같은 위치다. |
| 제어 프레임끼리 FIFO, HELLO 먼저(3.2.2절) | 넣을 때마다 `lastControl`을 앞으로 옮기므로 제어 런이 FIFO다. 서문과 HELLO가 제어 런의 맨 앞에 남는다. |
| 애플리케이션 프레임끼리 FIFO | `linkLast`를 쓰고, 지울 때는 그 노드만 떼어 낸다. |
| CLOSE는 마지막 쓰기 전 요청 뒤(3.5절) | 살아 있는 마지막 요청 노드 바로 뒤에 넣는다. 현재의 `max(firstApp, afterLastReq)`에서 요청은 애플리케이션 프레임이므로 `afterLastReq ≥ firstApp`이 성립한다. 따라서 둘은 같은 위치다. 쓰기 전 요청이 없으면 제어 런 끝에 넣는다. 이것도 현재와 같다. |
| CLOSE 뒤에 보낸 PING이나 RESET | `lastControl` 뒤, 곧 CLOSE 앞의 애플리케이션 프레임보다 앞에 들어간다. 현재와 같다. |
| 돌려보내는 RESET이 오류 CLOSE보다 먼저(3.5.1절) | `closeWithError`와 `close`가 RESET을 먼저 넣는 호출 순서가 그대로 지킨다. 두 프레임 모두 제어 런에 FIFO로 들어가거나, CLOSE가 그보다 뒤인 요청 뒤에 들어간다. |
| WS Close는 맨 끝 | `linkLast` 뒤 곧바로 `closeTransport()`가 실행되고, 그 뒤에는 송신이 없다. 제어 프레임은 `lastControl` 뒤에 들어가므로 WS Close 뒤로 가지 않는다. |
| drain한 메시지는 쓰기 시작한 것 | 리스트 전체를 꺼내고 비운다. 현재와 같다. |
| CLOSE 앞의 요청이 모두 취소된 뒤 보낸 제어 프레임(1.4절 표 셋째 줄) | `unlink` 끝의 한 줄이 `lastControl`을 CLOSE로 옮긴다. 그래서 현재처럼 CLOSE 뒤에 들어간다. 이 줄이 없으면 CLOSE 앞에 들어간다. 그것도 규격상 허용되지만, 현재 출력과 똑같이 맞추려고 넣는다. |

따라서 D는 모든 경우에 현재와 같은 순서를 낸다.

### 복잡도 변화

- **`refuseUnhandedRequests()`:** `events.removeIf` O(E) 위에 RESET마다 O(1)이므로 **O(E)**다. 현재는 O(E·n)이다.
- **`close()`, `closeWithError()`:** CLOSE를 넣는 데 O(r)이 한 번 든다.
- **`closeTransport()`, `endRequestsAfter()`:** 대기 스트림 하나당 O(1) 떼어 내기이므로 **O(P)**다. `newIdentitySet`, `identitySet`, `removeFromOutbox`, `firstApplicationIndex`, `afterLastUnwrittenRequest`가 모두 없어진다.
- **기한 만료, RESET·응답 수신에 따른 outbox 제거:** O(1)이다.
- **`drainMessages()`:** O(n)이고 할당은 결과 리스트 하나뿐이다.
- **남는 O(n) 경로:** `cancel(ticket)`과 `requestState(ticket)`의 번호표 탐색, `advance()`마다 도는 `expireRequests()` 전체 순회. 1.6절에서 말했듯 별도 작업이다.

---

## 5. 확인하지 못한 것

- **h2 (Rust):** `store::Queue`와 `Buffer`가 slab 색인 기반의 연결 큐인지 원문으로 확인하지 못했다. GOAWAY, SETTINGS, PING의 순서도 `prioritize.rs` 밖에 있어 보지 않았다.
- **quiche, msquic:** 조사하지 않았다.
- **nghttp2:** GOAWAY가 `default` 분기로 `ob_reg`에 들어간다는 것은 원문(1108-1112)으로 확인했다. GOAWAY를 보낸 뒤의 처리(`session_after_frame_sent`)는 보지 않았다.
- **테스트:** 이번 설계를 구현해 기존 테스트를 돌려 보지 않았다. 저장소 파일은 하나도 고치지 않았다.
