---
ticket: 147
label: wayfinder:research
checked: 2026-11-07
---

# 금융·문자 전송 프로토콜의 신뢰성 메커니즘 비교

확인일: 2026-11-07. 주 비교 대상은 SMPP 3.4/5.0, FIX 세션 계층(FIXT 프로필), ISO 8583이다. MQTT 5.0 QoS 2와 AMQP 1.0은 참고용으로 짧게 본다. 근거는 규격 원문에서만 가져왔고, 각 주장 옆에 절 번호(필요하면 쪽수)를 달았다. 원문 대신 추론한 내용은 **(추론)**, 1차 출처를 확인하지 못한 내용은 **(1차 출처 미확인)**으로 표시했다.

## 출처와 신뢰도

| 약칭 | 문서 | 입수 경로 | 비고 |
|---|---|---|---|
| SMPP34 | Short Message Peer to Peer Protocol Specification v3.4, Issue 1.2, 12-Oct-1999, SMPP Developers Forum | https://www.world-text.com/docs/SMPPV3.4.pdf (미러) | 공식 배포처(SMPP Developers Forum, 이후 SMS Forum)는 해산했다. 미러본 PDF 표지에 "Issue 1.2"와 Forum 저작권 표기가 있다. 원본과의 해시 대조는 하지 않았다 |
| SMPP50 | Short Message Peer-to-Peer Protocol Specification Version 5.0, 19-February-2003, SMS Forum | https://smpp.org/smppv50.pdf | 166쪽 |
| FIXSL | FIX Session Layer Technical Specification, Technical Standard – June 2020, FIX Protocol Ltd. | https://www.fixtrading.org/wp-content/uploads/download-manager-files/FIX_Session_Layer_June_2020.pdf | FIX4·FIXT·LFIXT 세션 프로필을 함께 정의한다. FIXT의 BeginString은 `FIXT.1.1`(5.3절, 4.2.1절 표 1) |
| FIXORCH | FIX Orchestra 저장소의 FIXT 세션·FIX Latest 정의 | https://github.com/FIXTradingCommunity/orchestrations (`FIX Standard/FIXTSession.xml`, `FIX Standard/OrchestraFIXLatest.xml`) | 필드·자료형 정의(MsgSeqNum, SeqNum, ClOrdID) 확인용 |
| ISO8583 | ISO 8583:2023, Edition 3 | https://committee.iso.org/standard/79451.html (초록) | **본문은 유료(18쪽)라 읽지 못했다.** 초록에 따르면 메시지·필드·값 정의는 유지 기관(MA)이 제공하고, "메시지 전송 방식이나 정산 방식은 이 문서의 범위가 아니다" |
| ISO8583-ANX | ISO 8583 부속서 C(데이터 요소), D(코드 목록), E(메시지 클래스), F(메시지·거래 매칭), J(이전 판) | https://standards.iso.org/iso/8583/ed-3/en/ | ISO가 공개한 전자 부록이다. **경로는 ed-3인데 각 파일 머리말은 `ISO/DIS 8583:2021(E)`**다. 부속서 J는 현행판을 "fourth edition"이라 부르지만 iso.org는 ISO 8583:2023을 Edition 3으로 적는다. 그러니 발행본과 문구가 다를 수 있다 |
| MQTT5 | MQTT Version 5.0, OASIS Standard, 2019-03-07 | https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html | 규범 문장 번호 `[MQTT-x.y.z-n]`로 인용 |
| AMQP1 | OASIS AMQP Version 1.0, Part 2 Transport / Part 5 Security | http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html , http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html | |

## 결론: 7개 항목 비교표

"규격 범위 밖"은 해당 규격이 그 계층을 아예 다루지 않는다는 뜻이고, "규정 없음"은 다루는 계층인데 그 항목에 대한 문장이 없다는 뜻이다.

| 항목 | SMPP 3.4 / 5.0 | FIX 세션 계층 (FIXT) | ISO 8583 | (참고) MQTT 5 QoS 2 | (참고) AMQP 1.0 |
|---|---|---|---|---|---|
| **1. 결과 확정** (응답 없는 요청) | 응답이 올 때까지 "받지 않은 것으로 간주"(3.4 §2.8). `response_timer` 만료 시 "처리되지 않았다고 간주하고 적절한 조치"(3.4 §7.2). 끊긴 세션의 미응답 PDU는 새 세션에서 응답되지 않는다(5.0 §2.6.3). 사후 조회는 `query_sm`인데 SMSC가 준 `message_id`로 찾는다(3.4 §4.8) | 세션 계층은 개별 메시지를 확인하지 않는 낙관적 모델(§4). 유실은 번호 간격으로 감지하고 `ResendRequest`로 복구(§4.8). 업무 응답이 없을 때 다시 보내는 것은 애플리케이션 책임(§4.9) | 요청/반복(repeat)에 응답이 없으면 보낸 쪽이 **반드시 reversal을 보낸다**. 늦게 온 응답은 무시(부속서 E 예외 흐름). reversal advice는 정해진 사유 외에는 거절할 수 없다(E, Reversal message rules) | 수신자가 PUBREC으로 소유권을 받고, PUBREL/PUBCOMP로 끝낸다(§4.3.3). 재접속 후 세션이 남아 있으면 원래 Packet Identifier로 재전송(§4.4) | 수신자 정산 모드 `second`: 송신자가 정산할 때까지 수신자가 정산을 미룬다(Part 2 §2.8.3). 미정산 상태를 기준으로 링크를 재개(§2.6) |
| **2. 중복 제거** (식별자·판단 주체·보관 기간) | **규정 없음.** 두 판 모두 본문에 "duplicate"가 나오지 않는다. 비슷한 기능은 `replace_if_present_flag`뿐이다. 이는 같은 주소·`service_type`의 미배달 메시지를 교체하는 기능이지 중복 제거가 아니다(3.4 §5.2.18) | 세션 계층: `PossDupFlag(43)=Y` 재전송분 중 같은 `MsgSeqNum`을 이미 처리했으면 무시(§4.8.7). 애플리케이션 계층: 중복 판단은 애플리케이션 책임이고 세션 계층은 하지 않는다(§3.1.14, §4.9). 단서는 `PossResend(97)`이고, 상호 합의로 "전역 유일 영속 식별자"를 대신 쓸 수 있다(§4.9). `ClOrdID(11)`의 유일성 범위는 "단일 거래일"(FIXORCH) | STAN(`Systems trace audit number`, bit 11)은 요청·반복·응답의 한 교환 안에서 바뀌지 않는다(부속서 C, F). 반복 메시지는 MTI 셋째 자리로 표시한다(xx1, xx3, xx5. 부속서 D 표 D.17). 보관 기간은 **규정 없음**(부속서 범위에서). 관련 응답 코드: 9113 "Duplicate transmission", 9123 "Request in progress"(부속서 D 표 D.3) | Packet Identifier 기준으로 수신자가 판단. PUBREL을 받기 전까지 같은 ID의 PUBLISH에 PUBREC만 다시 보내고 중복 전달 금지(4.3.3-10). PUBCOMP를 보낸 뒤 같은 ID는 새 메시지(4.3.3-12). 보관은 세션 수명(마지막 연결 + Session Expiry Interval, §4.1) 동안 | `delivery-tag`(최대 32옥텟)는 미정산 delivery 사이에서 유일해야 한다(§2.6.12, §2.8.7). 정산되면 잊는다(§2.6) |
| **3. 번호 공간** (크기·순환·소진) | `sequence_number` 4바이트, `0x00000001`~`0x7FFFFFFF`(3.4 §5.1.4, 5.0 §4.7.24). 3.4는 순환 규정 없음. **5.0은 다 쓰면 `0x00000001`로 돌아간다**(§4.7.24). 단조 증가는 권고일 뿐이고, 응답을 받은 번호를 바로 재사용하는 방식도 허용한다(5.0 §2.6.2) | `MsgSeqNum(34)`은 `SeqNum` 자료형이며 "양수, 선행 0 없음"(FIXORCH)이고 **상한·순환 규정 없음**. 세션은 1에서 시작하고 1로 리셋하면 새 세션이다(§4.1). 24시간 운영에서는 **연결을 유지한 채** `ResetSeqNumFlag(141)`로 1부터 다시 시작한다(§4.4.2) | STAN은 `n 12`(부속서 C, ed-3), 1987·1993판은 `n 6`(부속서 J). 순환·소진 규칙은 부속서에 없다. 본문(유료)에 있는지는 **1차 출처 미확인** | Packet Identifier 2바이트, 0이 아니고 "현재 쓰이지 않는" 값. 확인을 받으면 재사용 가능(§2.2.1) | `transfer-id`/`delivery-id`/`delivery-count`는 32비트 RFC 1982 순환 산술(§2.5.6, §2.6.7, §2.8.10) |
| **4. 연결을 넘는 세션** | **없음.** 번호는 한 세션용이고, 끊기면 새 세션은 1부터 다시 시작한다(5.0 §2.6.3) | **있음.** 한 FIX 세션은 순차적인 여러 FIX 연결에 걸칠 수 있다. 세션 식별은 BeginString + 양쪽 CompID(§4.2). `NextNumIn`/`NextNumOut`은 영속 저장해야 한다(§4.1). 재접속 때 `NextExpectedMsgSeqNum(789)`로 빠진 부분을 재전송(§4.4.1). 같은 CompID의 두 번째 동시 접속은 Logout 없이 끊는다(§4.6.4). 경량 프로필 LFIXT는 세션이 연결 하나와 같다(§5.4.3) | 규격 범위 밖(전송 제외, ISO8583 초록) | Client Identifier 기준 세션. 마지막 연결 + Session Expiry Interval까지 유지. 서버는 미확인 QoS 1/2 메시지와 구독 등을 저장(§4.1) | 링크 이름으로 링크를 재개. 끝점 상태 대신 터미널의 미정산 delivery 상태만 있으면 된다(§2.6, §2.6.1, §2.6.3) |
| **5. 생존 확인** (협상·판정) | `enquire_link`/`_resp`(3.4 §4.11). `enquire_link_timer`, `inactivity_timer`, `response_timer`는 권고 타이머이고 **값은 규격 범위 밖, 협상 없음**(3.4 §7.2, 5.0 §2.7) | Logon의 `HeartBtInt(108)`로 **협상**(필수 필드). 수락 측은 고정값, 범위, 상대 값 수용 중 하나를 고른다(§4.3.4~4.3.5). 간격 동안 받은 것이 없으면 `TestRequest`를 보내고, `TestRequestThreshold`(1.2~2.0) × 간격 안에 응답이 없으면 Logout 후 끊는다(§4.5.1, §4.5.5) | 네트워크 관리 메시지 800 클래스에 기능 코드 831 "echo test", 801/802 sign-on/off가 있다(부속서 D 표 D.14, E). 주기·판정 규칙은 부속서에 없다 | CONNECT Keep Alive, 서버가 CONNACK Server Keep Alive로 덮어쓸 수 있다. 서버는 1.5배 동안 받은 것이 없으면 끊는다(3.1.2-21, -22) | `open`의 `idle-time-out`. 각자 독립적으로 정하고, 실제 임계값의 절반을 알리라고 권고. 빈 프레임으로 유지(§2.4.5) |
| **6. 과부하·속도 제어** | 3.4: 오류 코드 `ESME_RTHROTTLED`(0x58, 허용 메시지 한도 초과), `ESME_RMSGQFUL`(0x14)(§5.1.3 표 5-2). 창(window)은 미응답 요청 수의 상한일 뿐 흐름 제어가 아니다(5.0 §2.9). 5.0은 응답 PDU에 `congestion_state` TLV(0~100, 최적 80~90)를 추가했다(§2.9, §4.8.4.18) | 세션 계층 규격에 throttle이나 흐름 제어 문장이 **없다**(본문 검색). 최대 메시지 크기 `MaxMessageSize(383)` 협상만 있다(§4.3.6) | 메시지 단위 응답 코드로 처리. 예: 9106 "Cutoff is in process … 몇 분 뒤 다시 보낼 수 있음", 9103 "Re-enter transaction", 9123 "Request in progress"(부속서 D 표 D.3). 연결 단위 흐름 제어는 규격 범위 밖 | Receive Maximum로 미확인 QoS>0 PUBLISH 수를 제한하는 송신 할당량(§4.9). 이유 코드 0x89 Server busy, 0x96 Message rate too high, 0x97 Quota exceeded | 세션 창(`incoming-window`/`outgoing-window`)과 링크 크레딧(`link-credit`)(§2.5.6, §2.6.7) |
| **7. 인증 위치** | **프로토콜 안**: `bind_*`의 `system_id`/`password`(3.4 §5.2.1~5.2.2). password를 NULL로 두는 "insecure access"도 SMSC가 허용하면 가능(3.4 §4.1.1 각주). 암호화는 자체 기능이 없고 전용선, SSL/TLS, VPN을 권고(5.0 §2.10) | **둘 다**: Logon에 `Username(553)`, `Password(554)`(평문, 비권장), `EncryptedPassword(1402)`, `RawData(96)` 등(§4.3.10 표 2). TCP 위에서는 FIX-over-TLS(FIXS) **필수**(§4.3.1). 애플리케이션 계층 암호화는 비권장(§4.3.3) | **메시지 단위**: 각 비트맵 마지막 비트를 MAC 필드(bit 64/128)로 예약(부속서 C). 키 교환은 네트워크 관리 메시지(부속서 E). 채널 보안은 규격 범위 밖 | CONNECT Username/Password, AUTH 패킷을 쓰는 확장 인증(§4.12) | 별도 SASL 계층과 TLS 계층(Part 5 §5.2, §5.3) |

## 1. SMPP 3.4 / 5.0

### 1.1 결과 확정

- 3.4 §2.8: "receiving entity must return the associated SMPP response PDU … Until such a response is received by the originator, it must be assumed that the PDU has not been received at the destination." (SMPP34 29쪽)
- 3.4 §7.2 표, `response_timer`: 만료되면 "The entity which originated the SMPP Request may assume that Request has not been processed and should take the appropriate action for the particular SMPP operation in question." 타이머 값은 "outside the scope of this SMPP Protocol Specification"이다. (SMPP34 165쪽)
- 5.0 §2.6.3: "All unacknowledged PDUs from the lost session will not be acknowledged in the new recovery session … the ESME is expected to begin numbering PDUs from 1." (SMPP50 37쪽)
- 사후 조회는 `query_sm`이다. 3.4 §4.8: "The matching mechanism is based on the SMSC assigned message_id and source address." (SMPP34 94쪽)
- 최종 배달 결과는 배달 영수증으로 확인한다. 3.4 §2.10.1: "To determine the eventual outcome of the SMS delivery, the ESME must request an SMSC Delivery Receipt in the submit_sm or data_sm operation." (SMPP34 30쪽)
- **(추론)** `message_id`는 `submit_sm_resp`에 실려 오므로, 그 응답이 유실되면 `query_sm`으로 조회할 열쇠가 없다. §2.8의 "받지 않은 것으로 간주" 규칙을 따라 다시 제출하면 SMSC가 중복을 거르는 장치가 규격에 없으니(1.2절) 중복 발송 위험이 남는다. 응답 유실 시 결과를 알 수 없는 상태를 프로토콜이 닫지 못하는 구조다.

### 1.2 중복 제거

- 3.4와 5.0 본문 모두 "duplicate"라는 단어가 없다(텍스트 추출본 전체 검색).
- `replace_if_present_flag`(3.4 §5.2.18): "used to request the SMSC to replace a previously submitted message, that is still pending delivery … provided that the source address, destination address and service_type match". 같은 절은 이 기능의 목적을 "This ensures that the SMSC has at most one message pending per destination SME for a particular application"이라고 적는다(125쪽). 목적지당 미배달 메시지를 하나로 묶는 기능이지 요청 단위 멱등성이 아니다. `submit_sm`에서 쓰는 방법은 §4.4.1.2에 있다.

### 1.3 번호 공간

- 3.4 §3.2 표 3-3: "The sequence_number should be increased monotonically for each submitted SMPP request PDU … may range from: 0x00000001 to 0x7FFFFFFF." (39쪽) §5.1.4도 같은 범위다(115쪽). 3.4 본문에는 "wrap"이 없다.
- 5.0 §4.7.24: "In the event of a session using the full range of values for the sequence_number, the ESME or MC should wrap around to 0x00000001. The value 0x00000000 is recommended for use when issuing a generic_nack where the original PDU was deemed completely invalid". (133쪽)
- 5.0 §2.6.2: 단조 증가는 "not a mandatory requirement". "Another approach … is to use a selected range of sequence numbers reusing values as soon as the associated response has been received." (37쪽)
- 5.0 §2.6.1: 번호 공간은 방향마다 따로 있다. "each SMPP session involves two sets of sequence numbers; the set used by the ESME … and the set used by the MC". (37쪽)

### 1.4 연결을 넘는 세션

- 없다(1.1절의 5.0 §2.6.3 인용). 5.0 §2.8.1은 연결이 끊기면 "continually try to connect or reconnect again at intervals"(예: 5초)라고 권고한다. (41쪽)

### 1.5 생존 확인

- 3.4 §4.11: `enquire_link`는 "confidence-check of the communication path"이고, 받는 쪽은 `enquire_link_resp`로 답한다. (105쪽)
- 3.4 §2.9와 §7.2, 5.0 §2.7 표 2-2: session init, enquire link, inactivity, response 타이머를 권고한다. inactivity 만료 시 "The SMPP session should be dropped." 값은 협상하지 않으며 규격 범위 밖이다.

### 1.6 과부하·속도 제어

- 3.4 §5.1.3 표 5-2: `ESME_RMSGQFUL 0x00000014 Message Queue Full`, `ESME_RTHROTTLED 0x00000058 Throttling error (ESME has exceeded allowed message limits)`. (113쪽) 5.0은 RTHROTTLED에 "usually returned where an ESME has exceeded a predefined messaging rate restriction applied by the operator"를 덧붙였다.
- 3.4 §2.5.2: 요청을 비동기로 여러 개 보낼 수 있고, 응답 순서는 "not mandatory"다. (21쪽)
- 5.0 §2.9: "It is a common misconception that windowing provides full flow control … A maxed out window prevents the originator from issuing more requests until responses arrive, but this is not flow control." 대안으로 응답 PDU에 붙이는 `congestion_state` TLV를 제시한다. 값은 0 Idle, 80-90 Optimum, 100 Congested이고 "the Window of N can actually be discarded". (43~44쪽, §4.8.4.18 147쪽)
- 3.4 §2.4: SMPP는 전송 계층이 "windowing, flow control and error handling"을 제공한다고 가정한다. (19쪽)

### 1.7 인증 위치

- 3.4 §5.2.2: "The password parameter is used by the SMSC to authenticate the identity of the binding ESME." `bind_transmitter` 표 각주: "The ESME may set the password to NULL to gain insecure access (if allowed by SMSC administration)." (116쪽, 46쪽)
- 5.0 §2.10: "SMPP itself, does not define any native encryption mechanisms." 권고 수단은 전용선, SSL/TLS("first establishing a secure and authenticated connection before commencing the SMPP session"), VPN, 보안 터널이다. (44~46쪽)

## 2. FIX 세션 계층 (FIXT)

모든 절 번호는 FIXSL(2020년 6월판)이다.

### 2.1 결과 확정

- §4: "The FIX session is based on an optimistic model. Normal delivery of data is assumed (i.e. no session layer acknowledgement of individual messages) with errors in delivery identified by message sequence number gaps." (12쪽)
- §4.8.2~4.8.3: 간격을 발견하면 `ResendRequest(35=2)`(BeginSeqNo, EndSeqNo, 0은 "이후 전부")를 보낸다. 재전송하는 쪽은 오래된 주문 등을 재전송하지 않기로 할 수 있고, 그때는 `SequenceReset(35=4)` GapFill로 건너뛴다. "the resender assumes responsibility for the results of the decision to retransmit or not retransmit messages." (34~36쪽)
- §4.6.3: "The impact of a FIX connection termination on the application layer is left to the application layer; it is undefined in the FIX session layer." (32쪽)
- §4.9: "The application layer may resend a message if it has not received an application layer acknowledgement for that message." 다시 보낸 메시지는 새 `MsgSeqNum`을 쓴다. (40쪽)

### 2.2 중복 제거

- §3.1.13 retransmission: 원래 `MsgSeqNum(34)`에 `PossDupFlag(43)=Y`를 붙인다. §3.1.14 resend: "The determination if the message was previously received is the responsibility of the application layer, not the session layer." (10쪽)
- §4.8.4: "It is the responsibility of the receiving firm to properly respond to the message with PossDupFlag(43) set to "Y" by determining if the message was previously processed." (36쪽)
- §4.8.7: "A message received with the PossDupFlag(43) set to "Y" should be ignored when a message with the same MsgSeqNum(34) was previously processed." (39쪽)
- §4.9: "The application layer is responsible for the detection and proper handling of duplicate application messages. The FIX session layer does not initiate a resend of an application message nor does the FIX session layer detect duplication application messages." 대안으로 "The use of a globally unique persistent identifier that enforces idempotent behavior at the application layer may be used as an alternative to sending a PossResend(97) field by counterparty agreement." (40쪽)
- 애플리케이션 식별자의 보관 범위 예: `ClOrdID(11)` 정의 "Uniqueness must be guaranteed within a single trading day … should ensure uniqueness across days, for example by embedding a date". (FIXORCH `OrchestraFIXLatest.xml`)
- 세션 번호의 보관 기간은 세션 기간과 같다. §4: "Connecting parties shall bilaterally agree upon the time the FIX session is started and the duration of a FIX session … such as a trading day". (12쪽)

### 2.3 번호 공간

- §4.1: "A FIX session shall start with … NextNumOut of 1 and … NextNumIn of 1." "Resetting NextNumOut to 1 and NextNumIn to 1, for whatever reason, shall constitute the beginning of a new FIX session." 세션 메시지와 애플리케이션 메시지가 같은 번호 공간을 쓴다. (12~13쪽)
- 자료형: `MsgSeqNum` type `SeqNum`, `SeqNum` = "Sequence of digits without commas or decimals. Value must be positive and may not contain leading zeros." XML 매핑은 `xs:positiveInteger`다. (FIXORCH `FIXTSession.xml`) 세션 계층 본문에는 상한, 순환, 소진 규정이 없다(본문 "wrap" 검색 0건). 32비트 같은 고정 폭을 정한 문장도 찾지 못했다.
- §4.4.2: "The FIX session may be reset to NextNumIn=1 and NextNumOut=1 over an active FIX session. This capability is provided for continuously operating markets that need to periodically reset the FIX session." 시각과 개시 측은 사전 합의하고, 개시 측은 먼저 `TestRequest`로 간격이 없는지 확인한 뒤 `ResetSeqNumFlag(141)=Y` Logon을 보낼 수 있다. (25쪽)
- §4.4.3: 접속 시 리셋은 "Operational risks, such as order overfills, could occur" 하므로 주의하라고 한다. (25쪽)
- §4.8.1: 받은 `MsgSeqNum`이 `NextNumIn`보다 작은데 `PossDupFlag`가 없으면 세션을 끝낸다(Logout, `SessionStatus(1409)=9`). (34쪽)

### 2.4 연결을 넘는 세션

- §4: "A single FIX session can exist across multiple sequential (not concurrent) FIX connections … a bi-directional durable session sharing characteristics of the guaranteed delivery and durable subscriber enterprise integration architecture patterns." (12쪽)
- §4.1: "A FIX session processor shall persist the NextNumOut and NextNumIn in order to support FIX session recovery across multiple FIX connections." (12쪽)
- §4.2: 세션 식별자는 "BeginString(8) + initiator CompID + acceptor CompID". (13쪽)
- §4.4.1: Logon의 `NextExpectedMsgSeqNum(789)`를 `NextNumOut`과 비교해 같으면 계속하고, 작으면 그 구간을 재전송하고, 크면 Logout한다. (23쪽)
- §4.4.4: 수락 측이 상태를 잃었을 때 개시 측 상태로 복원하는 절차. 이 경우 `AppLevelIndicator(1744)=1`로 "application layer recovery is needed"를 알린다. (25~26쪽)
- §4.6.4: 같은 SenderCompID의 유효한 세션이 있는데 두 번째 접속 시도가 오면 "immediately terminate the second connection attempt and not send a Logout(35=5) message". (33쪽)
- §5.4.3 LFIXT: "For LFIXT a FIX session exists over one FIX connection … A loss of either the FIX connection or the transport layer connection constitutes the end of a FIX session." 세션 계층 재전송이 없고 애플리케이션 계층이 복구한다. (50쪽)

### 2.5 생존 확인

- §4.3.4: "The HeartBtInt(108) field must be present in the Logon(35=A) message … must be agreed upon by the initiator and the acceptor … the maximum time allowed between receipt of messages from the peer." 메시지를 받을 때마다 타이머를 초기화한다. (15쪽)
- §4.3.5: 수락 측 정책은 세 가지다. 고정값 요구(불일치 시 Logout), 범위 요구, 개시 측 값 수용(echo). (15~16쪽)
- §4.5.5: 응답 대기 시간은 "TestRequestThreshold multiplied by the heartbeat interval", 권장 1.2~2.0이다. 응답이 없으면 Logout 후 전송 연결을 끊는다. 하트비트만으로는 상대 처리기의 정상 여부를 알 수 없을 수 있다고 적었다. (28~29쪽)
- §4.6: Logout 응답 대기는 HeartBtInt의 2배를 권장한다. (29쪽)

### 2.6 과부하·속도 제어

- 세션 계층 본문에서 "throttl", "flow control", "rate limit"는 0건이다. 관련 협상은 `MaxMessageSize(383)`뿐이다(§4.3.6, 16쪽).

### 2.7 인증 위치

- §4.3: FIX 연결 수립은 "creation of a transport layer connection, acceptance with optional authentication of the initiator by the acceptor and message synchronization". (14쪽)
- §4.3.1: "FIX implementations that use the TCP/IP protocol as the transport layer must use the FIX-over-TLS (FIXS) specification". (14쪽)
- §4.3.10 표 2: `Username(553)`, `Password(554)` "Clear text password - use is not recommended", `EncryptedPassword(1402)`, `RawData(96)` 등. (16~17쪽)
- §4.6.4: CompID나 IP가 잘못된 Logon은 Logout 없이 바로 끊는다. 무단 접속 시도일 수 있어서 정보를 흘리지 않기 위해서다. (33쪽)

## 3. ISO 8583

**입수 한계.** 본문(ISO 8583:2023, 18쪽)은 유료라 읽지 못했다. 아래는 공개 초록과 ISO가 공개한 부속서(머리말 `ISO/DIS 8583:2021(E)`)만으로 쓴 것이다. 초록에 따르면 "The method by which messages are transported or settlement takes place is not within the scope of this document." 그러므로 연결, 하트비트 주기, 연결 단위 흐름 제어, 채널 인증은 **규격 범위 밖**이다. 실무에서 쓰는 길이 프레이밍이나 TCP 세션 관리는 각 카드 네트워크 규격의 몫이고, 이 조사에서는 다루지 않았다.

### 3.1 결과 확정

- 부속서 E, Exception message flows: "If the original request/repeat sequence is not responded to for whatever reason, the sender of the request/repeat shall send a reversal. If the acquirer gets no response to a 100 or 200 message and stands in for the card issuer, any resulting 110 or 220 advice shall follow the reversal transaction. If a late response is received, the acquirer shall ignore it because a reversal transaction (and possibly an advice) has been sent." "There are no repeats or reversals for notification or instruction messages."
- 부속서 E, Reversal message rules: reversal은 이미 일어난 일이므로 advice(420/421)나 notification(440)만 쓴다. "A reversal advice shall not be declined except for specific reasons as defined in Annex D.3." "A reversal shall not be reversed."
- 부속서 E, 배치·파일 전송: "If an expected acknowledgement message is not received, the sender shall resend the instruction or notification message without change."
- **(추론)** 결과를 모를 때 "다시 묻기"가 아니라 "확정적으로 되돌리기"로 닫는다. 되돌리기 자체는 거절할 수 없는 advice라서 반복 전송으로 끝까지 밀어붙인다.

### 3.2 중복 제거

- 부속서 C, Systems trace audit number(bit 11, `n 12`): "A number assigned by a transaction originator to assist in identifying a transaction uniquely. The trace number remains unchanged for all messages within a two-message exchange, e.g. request/repeat and response."
- 부속서 F: 메시지 매칭에는 STAN을 쓰고(request, request repeat, request response 등), 거래 생애 매칭(승인→매입→reversal→chargeback)에는 `Transaction life cycle identification data`를 쓴다. 이 값은 "Once assigned … shall remain the same for all subsequent messages in the transaction life cycle".
- 부속서 D 표 D.17: MTI 셋째 자리가 거래 개시자다. 0 Acquirer, **1 Acquirer repeat**, 2 Card issuer, **3 Card issuer repeat**, 4 Other, **5 Other repeat**. 즉 재전송임을 메시지 유형으로 표시한다.
- 부속서 E 정산 규칙: "A repeat (xx1 or xx3) message is not added to reconciliation totals unless the original message was not received."
- 부속서 D 표 D.3 action code: 9113 "Duplicate transmission", 9122 "Message sequence number error", 9123 "Request in progress".
- 보관 기간이나 중복 판단 창은 부속서에 없다. 본문에 있는지는 **1차 출처 미확인**.

### 3.3 번호 공간

- STAN 길이: 현행 부속서 C는 `n 12`. 부속서 J는 ISO 8583:1993과 1987의 STAN을 `n 6`으로 적는다.
- 순환과 소진 규칙은 공개 부속서에 없다(**1차 출처 미확인**).

### 3.4 생존 확인·세션

- 부속서 E, Network management: "System condition management … may be used as part of normal system initialization or shutdown or as part of a failure recovery scheme", "System audit controls management, which is used to test integrity of interchange links". 부속서 D 표 D.14 기능 코드: 801 "System condition/sign-on", 802 "System condition/sign-off", 831 "System audit control/echo test".
- 주기, 협상, 장애 판정 규칙은 부속서에 없다.

### 3.5 과부하

- 별도 흐름 제어 메시지는 없고, 메시지별 응답 코드로 처리한다. 부속서 D 표 D.3 예: 9106 "Cutoff is in process (switch ending a day's business and starting the next. Transaction can be sent again in a few minutes)", 9103 "Re-enter transaction", 9120/9121 "Security software/hardware error – try again / do not retry".

### 3.6 인증 위치

- 부속서 C: MAC(bit 64, bit 128) "is used to validate the source and the text of the message between the sender and receiver. The last bit position of each bitmap shall be reserved for the Message authentication code data element."
- 부속서 E: 네트워크 관리의 System security management는 "key and password management and security alerts"를 다룬다. 부속서 D: 9116 "MAC incorrect", 9117 "MAC key sync error".

## 4. (참고) MQTT 5.0 QoS 2

- 결과 확정·중복: §4.3.3. 수신자는 PUBREC으로 "accepted ownership"을 알리고(4.3.3-8), PUBREL 전까지 같은 Packet Identifier의 PUBLISH에는 PUBREC만 다시 보내며 "MUST NOT cause duplicate messages to be delivered to any onward recipients"(4.3.3-10). PUBCOMP 뒤에는 같은 ID를 새 메시지로 본다(4.3.3-12). **(추론)** 중복 제거는 한 세션 안에서 Packet Identifier 단위로만 성립한다. 애플리케이션 종단 간 정확히 한 번은 아니다.
- 재전송: §4.4 "When a Client reconnects with Clean Start set to 0 and a session is present, both the Client and Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using their original Packet Identifiers. This is the only circumstance where a Client or Server is REQUIRED to resend messages." (4.4.0-1)
- 번호 공간: §2.2.1 "Two Byte Integer Packet Identifier", 새 패킷은 "non-zero Packet Identifier that is currently unused"(2.2.1-3). ACK를 받으면 재사용할 수 있다.
- 세션: §4.1 "The session can continue across a sequence of Network Connections. It lasts as long as the latest Network Connection plus the Session Expiry Interval." 서버 세션 상태에는 미확인 QoS 1/2 메시지와 "QoS 2 messages which have been received from the Client, but have not been completely acknowledged"가 들어간다.
- 생존 확인: §3.1.2.10 Keep Alive. 서버가 CONNACK의 Server Keep Alive로 값을 정할 수 있다(3.1.2-21). 1.5배 동안 받은 것이 없으면 서버가 연결을 끊는다(3.1.2-22).
- 흐름 제어: §4.9 Receive Maximum 송신 할당량. 0이 되면 QoS>0 PUBLISH를 보낼 수 없고, 다른 패킷 처리는 계속해야 한다(4.9.0-2, -3). 이유 코드 0x89 Server busy, 0x96 Message rate too high, 0x97 Quota exceeded(§2.4).
- 인증: CONNECT의 User Name/Password와 AUTH 패킷 기반 확장 인증(§4.12).

## 5. (참고) AMQP 1.0

- 번호: `sequence-no`는 "32-bit RFC-1982 serial number"(Part 2 §2.8.10). `next-outgoing-id`는 "MAY be initialized to an arbitrary value and is incremented … according to RFC-1982 serial number arithmetic"(§2.5.6). `delivery-count`도 "a 32-bit integer that wraps around"(§2.6.7).
- 식별·중복: `delivery-tag`(최대 32옥텟)는 "unique amongst all deliveries that could be considered unsettled by either end of the link"(§2.6.12, §2.8.7). 세션 범위의 `delivery-id`는 이를 짧게 가리키는 번호다.
- 결과 확정: `rcv-settle-mode = second`이면 "The receiver will only settle after sending the disposition to the sender and receiving a disposition indicating settlement of the delivery from the sender."(§2.8.3)
- 연결을 넘는 재개: §2.6 "Termini can exist beyond their associated link endpoints … The process of associating new link endpoints with existing termini and re-establishing communication is referred to as resuming a link. … Only the unsettled delivery state maintained at the termini is necessary for link resume". 링크 이름이 복구 키다(§2.6.1).
- 생존 확인: §2.4.5 `idle-time-out`. 각자 독립적으로 정하고, 실제 임계값의 절반을 알리라고 권고하며, 빈 프레임으로 유지한다. 넘으면 사유를 담은 close를 보낸다.
- 흐름 제어: 세션 창과 `link-credit`(§2.5.6, §2.6.7).
- 인증: 별도 SASL 계층과 TLS 계층(Part 5 §5.2, §5.3).

## Strand에 주는 시사점

결정이 아니라 관찰이다. Strand 현행 규격의 절 번호는 `docs/specification/STRAND_SPECIFICATION.md` 기준이다.

### #148 번호 소진 제거 방식

- 지금 Strand는 31비트 공간을 단조 증가로만 쓰고, 다 쓰면 `CLOSE(NO_ERROR)`로 닫는다(2.2.5절). 값의 범위(`1`~`0x7FFFFFFF`)는 SMPP와 같지만 용량은 다르다. Strand는 이 범위를 홀수·짝수로 나눠 엔드포인트마다 약 2^30개를 쓰고, SMPP는 방향마다 범위 전체(2^31−1개)를 따로 쓴다(SMPP50 §2.6.1). SMPP는 5.0에서 "1로 순환"을 규정했고, 응답을 받은 번호를 바로 재사용하는 방식도 허용한다(SMPP50 §4.7.24, §2.6.2).
- MQTT는 "현재 쓰이지 않는" 값을 재사용하고(§2.2.1), AMQP는 RFC 1982 순환 산술을 쓴다(§2.8.10). 세 방식 모두 번호 재등장을 허용하므로, "상대 공간 번호가 `highest_peer_stream_id` 이하이면 연결 오류"라는 Strand 4.2.1절의 번호 역행 규칙과 그대로는 함께 쓸 수 없다.
- FIX는 번호 상한을 두지 않는 대신, 연결을 유지한 채 번호를 1로 되돌리는 절차(§4.4.2)를 둔다. 이 절차는 "새 FIX 세션의 시작"으로 정의되므로(§4.1), 번호의 수명을 연결의 수명에서 떼어 내는 사례다.

### #149 연결을 넘는 세션 도입 여부

- 연결을 넘는 세션이 있는 쪽은 FIX(영속 `NextNumIn/Out`, `NextExpectedMsgSeqNum`), MQTT(Session Expiry, 서버가 미확인 QoS 2 수신분 보관), AMQP(링크 이름 기반 재개)다. 없는 쪽은 SMPP(5.0 §2.6.3)와 FIX의 경량 프로필 LFIXT(§5.4.3)다. LFIXT는 세션 계층 재전송을 빼고 애플리케이션 계층 복구에 맡긴다.
- 세션이 있으면 서버가 저장할 상태가 생긴다. FIX는 번호 두 개와 재전송용 메시지 저장소, MQTT는 미확인 메시지와 구독, AMQP는 터미널의 미정산 상태다. AMQP는 이 상태를 "애플리케이션 상태에서 재구성"할 수 있게 열어 두었다(§2.6).
- 동시 접속 정책 사례: FIX는 같은 세션의 두 번째 동시 접속을 Logout 없이 끊는다(§4.6.4).
- Strand는 지금 `stream_id`가 연결 안에서만 유일하고, 연결을 넘는 동일성은 `idempotency-key` 확장에 맡긴다(2.2.5절, 2.4.2절, 4.6절). 이는 FIX §4.9의 "전역 유일 영속 식별자" 대안이나 LFIXT 쪽 설계와 가깝다.

### #150 결과 확정 메커니즘

- 응답이 없을 때 세 가지 방식이 보인다.
  - (가) "받지 않은 것으로 간주"하고 다시 보내기: SMPP §2.8. 중복 제거 장치가 없다.
  - (나) 세션 계층 재전송 + 수신자 쪽 중복 판단: FIX `PossDupFlag`, MQTT QoS 2.
  - (다) 거절할 수 없는 취소(reversal)로 확정하고 늦은 응답은 버리기: ISO 8583 부속서 E.
- SMPP의 사후 조회 열쇠(`message_id`)는 응답에 실려 오기 때문에 응답 유실 시 조회가 막힌다(1.1절 추론). 조회 기반 확정을 생각한다면, 열쇠를 요청 쪽이 정하는지가 갈림길이 된다. ISO 8583의 STAN과 FIX의 `ClOrdID`는 요청 쪽이 정한다.
- FIX와 ISO 8583은 "다시 보낸 것"을 메시지에 표시한다(FIX `PossDupFlag`/`PossResend`, ISO MTI 셋째 자리 repeat). Strand의 `idempotency-key`에는 재전송 표시가 없다.
- 중복 판단 기간의 사례: FIX `ClOrdID`는 거래일 단위, MQTT는 PUBCOMP까지 + 세션 수명, AMQP는 정산까지. SMPP와 ISO 8583 부속서에는 없다.
- ISO 8583 9123 "Request in progress"는 "같은 요청이 아직 처리 중"이라는 응답 코드의 사례다. **(추론)** 지금 Strand `status`(2.5.1절)에는 이름이 딱 맞는 값이 없다. 가장 가까운 것은 `ABORTED`("동시 작업과 충돌")와 `ALREADY_EXISTS`다.

### #151 장기 연결 생존성

- 하트비트 주기를 협상하는 쪽은 FIX(`HeartBtInt` 필수, 수락 측 정책 세 가지), MQTT(Server Keep Alive가 덮어씀), AMQP(각자 독립 선언, 절반 값 권고)다. SMPP는 타이머를 권고만 하고 값은 범위 밖으로 둔다. Strand는 PING이 있고 대기 시간은 로컬 설정이다(3.3절, 3.4절).
- 장애 판정 배수 사례: FIX 1.2~2.0 × HeartBtInt(§4.5.5), MQTT 1.5 × Keep Alive(3.1.2-22).
- FIX §4.5.5는 하트비트가 와도 상대 처리기가 고장일 수 있다고 적고, 응답을 요구하는 `TestRequest`를 따로 둔다.
- 번호 소진 외에 연결을 정기적으로 닫게 만드는 요인으로 FIX는 "거래일 단위 세션"을 들고, 24시간 시장을 위해 연결을 유지한 채 리셋하는 절차를 따로 만들었다(§4.4.2).

### #152 과부하 제어

- 창(미응답 요청 수 상한)과 흐름 제어를 구분하는 문장이 SMPP 5.0 §2.9에 있다. Strand는 `max_concurrent_streams` 하나가 배압의 유일한 수량 한도이고, 넘으면 `RESET(REFUSED)`로 거절한다(4.5절). **(추론)** 이는 SMPP의 "창"에 해당한다.
- 속도 초과 거절 코드 사례: SMPP `ESME_RTHROTTLED`, MQTT 0x96/0x97, ISO 8583 9106(다시 보낼 시점 안내 포함). Strand에는 `RESOURCE_EXHAUSTED`(status)와 `OVERLOAD`(error_code)가 있다(2.5절).
- 부하 정도를 응답마다 실어 보내는 사례는 SMPP 5.0 `congestion_state`(0~100, 선택 TLV)다. 송신자는 이 값을 보고 속도를 조절한다.
- FIX 세션 계층에는 과부하 제어가 없다.

### #153 인증 위치

- 프로토콜 안 인증: SMPP `bind`(NULL 비밀번호 허용 가능), FIX Logon(TLS 필수 위에 자격 증명 필드), MQTT CONNECT/AUTH, AMQP SASL 계층.
- 메시지 단위 무결성: ISO 8583 MAC(bit 64/128)과 키 관리용 네트워크 관리 메시지.
- 채널에만 맡기는 사례: SMPP 5.0 §2.10이 암호화를 전용선, TLS, VPN에 맡긴다(인증 자체는 `bind`에 있다). Strand는 현재 기밀성, 무결성, 상대 인증을 모두 채널(상호 인증 TLS 1.3 권고)에 맡긴다(6.1절). **(추론)** 멱등 키를 "인증된 상대별로" 나눠 관리하라는 요구(6.2절)는 채널 인증의 신원을 애플리케이션이 받아 쓸 수 있다는 전제를 깔고 있다.
- FIX는 TCP 위에서 TLS를 MUST로 두면서도 Logon 자격 증명을 함께 둔다. 이는 채널 인증과 프로토콜 안 인증이 서로를 배제하지 않는 사례다.
