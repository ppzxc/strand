# Strand 적합성 벡터 형식

* **문서 상태 (Status)**: 초안 (Draft)
* **벡터 형식 버전 (Format version)**: 1
* **작성일자 (Date)**: 2026-10-28
* **편집자 (Editor)**: 조정하 (@ppzxc)
* **저장소 (Repository)**: ppzxc/strand

---

이 문서는 규범이다. 와이어 규격(`STRAND_SPECIFICATION.md`)의 규범 참조이며, 벡터 형식 버전 1을 정의한다. 다른 언어로 구현하는 사람이 이 문서와 JSON Schema 파일(`conformance/schema/vector-format-1.schema.json`)만 보고 벡터를 쓰고 러너를 만들 수 있도록, 벡터의 의미를 여기서 정의한다. JSON Schema는 이 문서의 구조를 기계가 검사하게 만든 것이다. 둘이 어긋나면 이 문서가 우선한다. id 유일성, 일련번호 할당 전제, 가상 시계처럼 스키마로 표현할 수 없는 규칙은 이 문서에만 있다.

이 문서에서 **러너**는 벡터를 읽어 테스트 대상 구현에 입력을 넣고 출력을 기대 결과와 비교하는 프로그램이고, **단계**는 시나리오의 한 항목이다. 두 말은 이 문서 안에서만 쓴다.

절 번호 표기: "이 문서 n절"은 이 문서의 절이고, 그냥 "n.n절"은 와이어 규격(`STRAND_SPECIFICATION.md`)의 절이다. 벡터의 `spec` 필드도 와이어 규격의 절만 가리킨다.

## 1. 모델

모든 벡터는 시나리오 하나다. 시나리오는 초기 상태(`setup`)와 단계(`steps`) 목록으로 이뤄진다. 프레임 하나를 디코딩해 보는 벡터도 단계가 하나뿐인 시나리오다. 그래서 러너와 구현의 접점도 하나다.

러너가 관측하는 것은 **구현이 송신한 와이어 프레임**과 **애플리케이션 경계 이벤트**, 두 가지뿐이다. 스트림 상태나 미응답 바이트 같은 내부 상태는 직접 단언하지 않는다. 대신 다음처럼 간접 관측으로 확인한다.

| 확인하려는 내부 상태 | 간접 관측 |
| --- | --- |
| 유예 상태 | 끝난 스트림으로 늦게 도착한 프레임이 조용히 버려지는지(`send: []`) |
| 유예 보관에서 빠짐 | 같은 늦은 프레임에 `RESET(STREAM_CLOSED)`이 오는지 |
| 미응답 바이트 반환 | 반환 사건 직후 같은 크기의 요청이 다시 수락되는지 |
| 거절된 요청이 미응답 바이트에 들지 않음 | 거절 직후 한도 경계 크기의 요청이 수락되는지 |

## 2. 파일 구조와 진화 규칙

벡터 파일의 최상위는 아래 두 필드다.

| 필드 | 의미 |
| --- | --- |
| `format` | 형식 버전. 정수. 이 문서는 `1`을 정의한다 |
| `vectors` | 벡터 배열 |

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "ping-is-acknowledged",
      "description": "PING을 받으면 같은 8바이트에 ACK를 붙여 돌려준다",
      "spec": ["3.4"],
      "setup": { "role": "server", "established": true },
      "steps": [
        {
          "kind": "recv",
          "hex": "00000014 00020000 00000000 00000000 0102030405060708",
          "send": [{ "type": "PING", "flags": ["ACK"], "payload": "0102030405060708" }],
          "events": []
        }
      ]
    }
  ]
}
```

진화 규칙은 다음과 같다.

1. 같은 `format` 안에서는 필드를 추가만 할 수 있다. 추가하는 필드에는 기본값이 있어야 한다. 그러므로 기본값을 쓰는 옛 벡터는 새 로더가 계속 읽을 수 있다.
2. 로더는 자기가 아는 것보다 높은 `format`과 모르는 필드를 거부한다(MUST).

## 3. 벡터의 공통 필드

| 필드 | 필수 | 의미 |
| --- | --- | --- |
| `id` | 예 | 같은 파일 안에서 유일한 kebab-case 이름(`[a-z0-9]+(-[a-z0-9]+)*`) |
| `description` | 예 | 무엇을 확인하는지 적은 한 문장 이상의 설명 |
| `spec` | 예 | 근거가 되는 규격 절 번호 문자열의 배열. 예: `["2.4.2", "5.1.3"]`. 가장 구체적인 하위 절 번호를 쓴다(예: `5.1`이 아니라 `5.1.3`). 비어 있으면 안 된다. 이슈 번호는 쓰지 않는다 |
| `setup` | 예 | 초기 상태(이 문서 4절) |
| `steps` | 예 | 단계 목록(이 문서 5절). 하나 이상 |

벡터 파일은 규격 영역별로 나눈다. `framing`, `handshake`, `streams`, `backpressure`, `extensions`, `close` 같은 이름을 쓴다. 테스트 이름은 `파일명/id`다(파일명에서 `.json`은 뺀다).

## 4. 초기 상태: `setup`

| 필드 | 필수 | 의미 | 기본값 |
| --- | --- | --- | --- |
| `role` | 예 | 테스트 대상 구현의 역할. `client` 또는 `server` | — |
| `local_hello` | 아니오 | 구현이 보내는 HELLO의 파라미터(3.2.1절의 이름) | 규격 기본값 |
| `peer_hello` | 아니오 | 러너가 상대 역할로서 보내는 HELLO의 파라미터 | 규격 기본값 |
| `established` | 예 | 연결을 수립한 상태에서 시작하는지 | — |
| `config` | 아니오 | 설정 항목(이 문서 8.2절) | 항목별 기본값 |

`established`가 `true`이면 러너가 표준 수립 순서(연결 서문, HELLO, `ACK`, 3.3절)를 실행하고, 성공을 확인한 뒤 구현의 출력과 이벤트를 비운 다음 첫 단계를 시작한다. 이때 `local_hello`와 `peer_hello`로는 수립이 성공하는 값만 쓴다. `false`이면 연결이 막 열린 맨 바이트 상태에서 시작한다. 연결 서문과 첫 프레임 검사를 확인하는 벡터가 이 쪽이다. 이 경우 구현이 연결이 열리자마자 내보내는 출력(클라이언트 역할의 연결 서문 등)은 첫 단계의 `send`에 합쳐 비교한다.

"이미 열린 스트림" 같은 중간 상태는 선언하지 않는다. 필요하면 앞 단계에서 `request`나 `recv`로 만든다.

## 5. 단계

단계는 `kind`로 종류를 구분하며, 모든 단계에 기대 결과 `send`와 `events`(이 문서 6절)가 반드시 있다.

| `kind` | 필드 | 의미 |
| --- | --- | --- |
| `recv` | `hex` | 구현에 바이트를 넣는다. 소문자 hex이고 공백·줄바꿈은 허용하며, 모두 지운 뒤 자릿수가 짝수여야 한다. 프레임 경계와 무관하게 아무 위치에서 잘라도 된다. 형식이 깨진 바이트도 쓸 수 있다 |
| `request` | `type`, `extensions`, `payload` | 애플리케이션이 요청을 보내게 한다. `type`은 256 이상 정수, `extensions`는 `{tag, value}` 배열(기본값 빈 배열), `payload`는 hex(기본값 빈 값) |
| `respond` | `stream_id`, `status`, `extensions`, `payload` | 애플리케이션이 받은 요청에 응답하게 한다. `status`는 2.5.1절의 이름 |
| `cancel` | `stream_id` | 애플리케이션이 자기가 보낸 요청을 취소하게 한다 |
| `close` | `error_code` | 애플리케이션이 연결을 닫게 한다. `error_code`는 2.5.2절의 이름 |
| `advance` | `ms` | 가상 시계를 `ms`밀리초 앞으로 보낸다(이 문서 8.3절). 1 이상 정수 |

`stream_id`는 개시자 비트를 포함한 32비트 정수이고, 10진수로 쓴다(서버가 연 첫 스트림은 `2147483649`).

## 6. 기대 결과

### 6.1 `send`: 송신 프레임

`send`는 단계가 실행되는 동안 구현이 보낸 프레임의 목록이다. 프레임은 필드 단위 구조로 쓰고, 아래 표의 규범 필드만 비교한다. 아무것도 보내지 않으면 `send: []`다.

| 프레임 (`type`) | 비교하는 필드 | 비교하지 않는 필드 |
| --- | --- | --- |
| `PREFACE` | 8바이트 연결 서문 전체 | — |
| `HELLO` | `flags`(`ACK` 여부), `stream_id`(0), `params`(빠진 값은 규격 기본값으로 채워 비교. `ACK`이면 payload가 비어 있어야 함), 확장 영역(비어 있어야 함) | — |
| `PING` | `flags`(`ACK` 여부), `stream_id`(0), `payload`(공백 없는 16자리 hex), 확장 영역(비어 있어야 함) | — |
| `CLOSE` | `stream_id`(0), `last_stream_id`, `error_code`, `flags`(0), 확장 영역(비어 있어야 함) | `reason` |
| `RESET` | `stream_id`, `error_code`, `flags`(0), 확장 영역(비어 있어야 함) | — |
| 애플리케이션 프레임 (`type` 256 이상 정수) | `type`, `flags`(`RESPONSE`, `END`의 집합), `stream_id`, `status`(`RESPONSE` 프레임만. 생략하면 `OK`), `extensions`(tag별 값. 순서는 비교하지 않음), `payload`(생략하면 빈 값) | 요청 프레임의 `status` |

모든 프레임에서 `length`와 `reserved`는 비교하지 않는다. 비교 대상이지만 쓰지 않은 필드는 위 표의 기본값(빈 `flags`, 빈 확장 영역, 빈 `payload`)과 비교한다. 애플리케이션 프레임은 `flags`를 반드시 쓴다.

### 6.2 `events`: 애플리케이션 경계 이벤트

| `event` | 필드 | 뜻 |
| --- | --- | --- |
| `request_received` | `stream_id`, `type` | 상대가 연 스트림의 새 요청이 애플리케이션에 전달됨 |
| `result` | `stream_id`, `status` | 자기가 보낸 요청의 결과가 애플리케이션에 전달됨. 상대가 보낸 `status`인지 로컬에서 만든 `status`인지는 구분하지 않는다 |
| `closed` | — | 연결이 닫혔음이 애플리케이션에 전달됨 |

`status`는 2.5.1절의 이름이다. 17~255의 예약 값은 정수로 쓴다.

## 7. 비교 규칙

1. 단계마다 `send` 목록과 `events` 목록은 각각 순서까지 포함해 완전히 일치해야 한다. 기대 목록에 없는 출력이나 이벤트가 있으면 실패다.
2. `send`와 `events` 두 목록 사이의 순서는 비교하지 않는다.
3. `send: []`는 구현이 아무것도 보내지 않았다는 뜻이다. 그러므로 조용한 폐기와 와이어 무응답을 검증할 수 있다.
4. 아무것도 보내지 않고 연결을 닫는 경우(예: 연결 서문 불일치)는 `send: []`와 `events: [{ "event": "closed" }]`로 쓴다.

## 8. 전제 조건과 설정

### 8.1 전제 조건

벡터를 실행하는 동안 테스트 대상 구현은 다음을 만족한다. 벡터는 이를 전제로 쓴다.

1. 구현은 자기 번호 공간에서 일련번호를 1부터 연속으로 할당한다. 클라이언트는 1, 2, 3, ...이고 서버는 `2147483649`, `2147483650`, ...이다. 번호 건너뛰기를 허용하는 2.2.5절은 바뀌지 않는다. 이 전제 덕분에 벡터 작성자는 응답을 담은 `recv`의 `stream_id`를 미리 적을 수 있다.
2. 구현은 256 이상의 모든 애플리케이션 `type`을 알려진 것으로 다루고 payload를 해석하지 않은 채 전달한다. `type` 등록과 payload 코덱은 애플리케이션 계층의 몫이므로, 벡터는 5.1.3절의 모르는 `type`과 payload 해석 실패를 검증하지 않는다.

### 8.2 설정 항목

적합한 구현은 적합성 실행 중에 아래 항목을 모두 설정할 수 있어야 한다(MUST). 건너뛰는 항목은 없다. 설정은 `setup`의 `local_hello`와 `config`로 한다. `local_hello`의 파라미터는 구현이 보내는 HELLO의 값으로 설정할 수 있어야 한다.

| 항목 | 위치 | 의미 | 기본값 |
| --- | --- | --- | --- |
| `max_frame_size`, `max_pending_bytes`, `max_concurrent_streams`, `capabilities`, `content_types`, `content_type` | `local_hello` | 3.2.1절의 파라미터 | 3.2.1절의 기본값 |
| `draining_retention_limit` | `config` | 유예 보관 한도. 유예 상태로 최근 넘어간 스트림을 몇 개까지 보관하는지(스트림 개수). 넘치면 보관에서 빠진다. 보관에서 빠진 스트림은 "끝난 스트림"이다 | 1024 |
| `hello_timeout_ms` | `config` | HELLO를 보내고 `ACK`를 기다리는 시간(`HELLO_TIMEOUT`, 3.3절) | 5000 |

설정 값이 규격의 권고(예: 유예 보관 한도 1,024개 이상)보다 작아도, 적합성 실행 중에는 구현이 설정값을 따른다. 보관에서 가장 먼저 유예 상태로 넘어간 스트림부터 빠지는지(FIFO), 늦은 프레임이 닿으면 갱신되는지(LRU)에 따라 결과가 갈리는 벡터는 쓰지 않는다.

### 8.3 가상 시계

시간은 `advance` 단계로만 흐른다(MUST). 구현은 시계를 주입받을 수 있어야 하며, 적합성 실행 중에는 실제 시간을 보지 않는다. 시계는 `setup` 시작 시점에 0이다. 표준 수립 순서와 단계 안의 처리는 시간이 걸리지 않는다. 시간 한도에 정확히 닿은 순간의 동작은 규격이 정하지 않았으므로, 벡터는 한도 직전과 한도를 지난 뒤만 쓴다.

## 9. 검증 범위

벡터는 MUST와 MUST NOT 동작만 검증한다. SHOULD 동작은 다루지 않는다. 예를 들어 `timeout` 초과 시 `DEADLINE_EXCEEDED` 응답, 유예 보관 한도 권고, 배압을 지키는 송신 측 회계(이 문서 4.5절)는 벡터가 없다. 선택 동작 때문에 적합성 판정이 갈리지 않게 하려는 원칙이다.

## 10. 예시 (비규범)

아래 벡터는 형식을 보여 주는 예시다. 규격 본문이 요구하는 동작은 본문이 정하며, 이 예시는 그 일부를 어떻게 관측하는지를 보여 줄 뿐이다. 긴 반복 바이트는 줄여 쓰는 표기 `<00x16352>`(`00`을 16,352번 반복)로 나타냈다. 실제 벡터 파일에는 풀어서 쓴다.

### 10.1 조용한 폐기와 유예 보관에서 빠진 뒤의 `STREAM_CLOSED`

응답이 도착한 스트림은 유예 상태가 되어 늦은 프레임을 조용히 버린다. 유예 보관 한도가 1이면 다음 스트림이 끝날 때 앞 스트림이 보관에서 빠져, 같은 늦은 프레임에 `RESET(STREAM_CLOSED)`이 나간다.

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "draining-discard-then-evicted",
      "description": "유예 중인 스트림의 늦은 응답은 조용히 버리고, 유예 보관에서 빠진 스트림의 늦은 응답에는 RESET(STREAM_CLOSED)을 보낸다",
      "spec": ["4.3", "4.4", "5.1.2"],
      "setup": {
        "role": "client",
        "established": true,
        "config": { "draining_retention_limit": 1 }
      },
      "steps": [
        {
          "kind": "request",
          "type": 257,
          "send": [{ "type": 257, "flags": ["END"], "stream_id": 1 }],
          "events": []
        },
        {
          "kind": "request",
          "type": 257,
          "send": [{ "type": 257, "flags": ["END"], "stream_id": 2 }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010300 00000001 00000000",
          "send": [],
          "events": [{ "event": "result", "stream_id": 1, "status": "OK" }]
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010300 00000001 00000000",
          "send": [],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010300 00000002 00000000",
          "send": [],
          "events": [{ "event": "result", "stream_id": 2, "status": "OK" }]
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010300 00000001 00000000",
          "send": [{ "type": "RESET", "stream_id": 1, "error_code": "STREAM_CLOSED" }],
          "events": []
        }
      ]
    }
  ]
}
```

네 번째 단계가 조용한 폐기이고, 마지막 단계가 보관에서 빠진 뒤의 `STREAM_CLOSED`다.

### 10.2 미응답 바이트의 경계, 거절 배제, 반환 후 재수락

`max_frame_size`와 `max_pending_bytes`를 모두 16,384로 두고, 서버 역할 구현이 상대의 요청을 받는다. 요청 하나의 크기는 `4 + length`이다. 첫 요청은 16,368바이트이고, 이어지는 요청은 17바이트(합계가 한도보다 1 큼), 16바이트(합계가 한도와 같음)다.

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "pending-bytes-boundary-refuse-and-return",
      "description": "합계가 한도와 같으면 수락하고 1 크면 REFUSED로 거절한다. 거절된 요청은 미응답 바이트에 들지 않고, 응답을 보내면 그 크기만큼 다시 수락한다",
      "spec": ["4.5", "5.1.2"],
      "setup": {
        "role": "server",
        "established": true,
        "local_hello": { "max_frame_size": 16384, "max_pending_bytes": 16384 }
      },
      "steps": [
        {
          "kind": "recv",
          "hex": "00003fec 01010200 00000001 00000000 <00x16352>",
          "send": [],
          "events": [{ "event": "request_received", "stream_id": 1, "type": 257 }]
        },
        {
          "kind": "recv",
          "hex": "0000000d 01010200 00000002 00000000 00",
          "send": [{ "type": "RESET", "stream_id": 2, "error_code": "REFUSED" }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010200 00000003 00000000",
          "send": [],
          "events": [{ "event": "request_received", "stream_id": 3, "type": 257 }]
        },
        {
          "kind": "respond",
          "stream_id": 3,
          "status": "OK",
          "send": [{ "type": 257, "flags": ["RESPONSE", "END"], "stream_id": 3, "status": "OK" }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010200 00000004 00000000",
          "send": [],
          "events": [{ "event": "request_received", "stream_id": 4, "type": 257 }]
        }
      ]
    }
  ]
}
```

두 번째 단계가 `+1` 거절이다. 세 번째 단계는 거절된 요청이 미응답 바이트에 들지 않았음을 보이는 동시에 `==` 경계 수락이다. 네 번째 단계에서 응답으로 16바이트가 반환됐으므로 다섯 번째 단계가 다시 수락된다.

### 10.3 응답 해석 실패

응답에 모르는 필수 확장 tag가 붙어 오면 구현은 와이어로 아무것도 보내지 않고, 호출자에게만 로컬에서 만든 `UNIMPLEMENTED` 결과를 넘긴다. 스트림은 정상 응답을 받은 것처럼 유예 상태로 넘어가므로 같은 스트림의 늦은 프레임은 조용히 버려진다. 응답 `type` 불일치와 필수 확장이 겹치면 `type` 불일치가 우선해 `INTERNAL`이다.

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "response-unknown-required-extension",
      "description": "응답에 모르는 필수 확장 tag가 붙어 오면 아무것도 보내지 않고 호출자에게 UNIMPLEMENTED를 넘기며, 이후 같은 스트림의 늦은 프레임은 조용히 버린다",
      "spec": ["2.4.2", "5.1.3"],
      "setup": { "role": "client", "established": true },
      "steps": [
        {
          "kind": "request",
          "type": 257,
          "send": [{ "type": 257, "flags": ["END"], "stream_id": 1 }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "00000010 01010300 00000001 00040000 80010000",
          "send": [],
          "events": [{ "event": "result", "stream_id": 1, "status": "UNIMPLEMENTED" }]
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010300 00000001 00000000",
          "send": [],
          "events": []
        }
      ]
    },
    {
      "id": "response-type-mismatch-beats-required-extension",
      "description": "응답 type 불일치와 모르는 필수 확장이 겹치면 type 불일치가 우선해 호출자에게 INTERNAL을 넘긴다. 와이어로는 아무것도 보내지 않는다",
      "spec": ["2.2.2", "2.4.2", "5.1.3"],
      "setup": { "role": "client", "established": true },
      "steps": [
        {
          "kind": "request",
          "type": 257,
          "send": [{ "type": 257, "flags": ["END"], "stream_id": 1 }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "00000010 01020300 00000001 00040000 80010000",
          "send": [],
          "events": [{ "event": "result", "stream_id": 1, "status": "INTERNAL" }]
        }
      ]
    }
  ]
}
```

### 10.4 가상 시계와 `HELLO_TIMEOUT`

서버 역할 구현이 자기 HELLO와 `ACK`를 보낸 뒤 상대의 `ACK`가 오지 않는다. 대기 시간(1,000밀리초) 직전에는 아무것도 보내지 않고, 지나면 `CLOSE(HELLO_TIMEOUT)`을 보낸다. CLOSE의 `last_stream_id` 0은 처리한 스트림이 없다는 뜻이다.

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "hello-ack-timeout",
      "description": "HELLO에 대한 ACK가 오지 않은 채 대기 시간이 지나면 CLOSE(HELLO_TIMEOUT)을 보내고 닫는다",
      "spec": ["3.3"],
      "setup": {
        "role": "server",
        "established": false,
        "config": { "hello_timeout_ms": 1000 }
      },
      "steps": [
        {
          "kind": "recv",
          "hex": "c0de5354 52300000 0000000c 00010000 00000000 00000000",
          "send": [
            { "type": "HELLO", "params": { "content_type": 1 } },
            { "type": "HELLO", "flags": ["ACK"] }
          ],
          "events": []
        },
        {
          "kind": "advance",
          "ms": 999,
          "send": [],
          "events": []
        },
        {
          "kind": "advance",
          "ms": 2,
          "send": [{ "type": "CLOSE", "last_stream_id": 0, "error_code": "HELLO_TIMEOUT" }],
          "events": [{ "event": "closed" }]
        }
      ]
    }
  ]
}
```
