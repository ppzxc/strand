package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * 내 요청 장부(#217)의 internal seam 테스트. 장부 interface 너머로 보이는 결과만 확인한다: 번호표 색인과 스트림 색인의 동기화, 끝난 요청이
 * 어떤 색인에도 남지 않음, 확정성 판정 표(상태 x 사건, 규격 3.5절, 4.4절, 4.4.1절), 기한 만료 순서, 넘긴 가장 큰 번호의 갱신 시점.
 */
class OwnRequestLedgerTest {

    private static final int TYPE = 0x0100;
    private static final int OK = 0;

    private final Outbox outbox = new Outbox();
    private final OwnRequestLedger ledger = new OwnRequestLedger(outbox);

    private static List<OwnRequestLedger.Outcome> expire(OwnRequestLedger target, long now) {
        List<OwnRequestLedger.Outcome> ended = new ArrayList<>();
        target.expire(now, ended::add);
        return ended;
    }

    private void accept(long streamId, long ticket) {
        accept(streamId, ticket, null);
    }

    private void accept(long streamId, long ticket, Long deadlineAt) {
        ledger.accept(streamId, ticket, TYPE, Frames.request(TYPE, streamId, new byte[0]), deadlineAt);
    }

    private void assertEnded(
            OwnRequestLedger.Outcome outcome, long streamId, int status, Certainty certainty, boolean reset) {
        assertThat(outcome).isNotNull();
        assertThat(outcome.streamId).isEqualTo(streamId);
        assertThat(outcome.status).isEqualTo(status);
        assertThat(outcome.certainty).isEqualTo(certainty);
        assertThat(outcome.resetToSend).isEqualTo(reset);
    }

    // ---- 색인 동기화 ----

    /** 번호표로 찾은 요청과 스트림으로 찾은 요청이 같다. */
    @Test
    void ticketAndStreamLookupsFindTheSameRequest() {
        accept(1, 10);
        assertThat(ledger.streamIdOf(10)).isEqualTo(1L);
        assertThat(ledger.stateOfTicket(10)).isEqualTo(Endpoint.RequestState.NUMBERED_UNWRITTEN);
        assertThat(ledger.stateOfStream(1)).isEqualTo(Endpoint.RequestState.NUMBERED_UNWRITTEN);
        assertThat(ledger.openCount()).isEqualTo(1);
    }

    /** 끝난 요청은 어떤 색인에도 없다. */
    @Test
    void endedRequestIsInNoIndex() {
        accept(1, 10);
        ledger.onResponse(1, TYPE, OK);
        assertThat(ledger.streamIdOf(10)).isNull();
        assertThat(ledger.stateOfTicket(10)).isNull();
        assertThat(ledger.stateOfStream(1)).isNull();
        assertThat(ledger.openCount()).isZero();
        assertThat(ledger.cancelTicket(10)).isNull();
        assertThat(ledger.cancelStream(1, Status.CANCELLED)).isNull();
    }

    /** 무작위 사건 순서에서도 색인이 어긋나지 않는다. */
    @Test
    void indexesStayInSyncUnderRandomEventOrder() {
        for (long seed = 0; seed < 300; seed++) {
            Random random = new Random(seed);
            Outbox box = new Outbox();
            OwnRequestLedger l = new OwnRequestLedger(box);
            // 독립된 모델: 번호표 -> 쓰기 시작 여부. 끝나면 지운다.
            Map<Long, Boolean> written = new HashMap<>();
            Map<Long, Long> streamOf = new HashMap<>();
            long nextStream = 1;
            long ticket = 0;
            long now = 0;
            for (int step = 0; step < 60; step++) {
                switch (random.nextInt(8)) {
                    case 0:
                    case 1:
                        long deadline = random.nextBoolean() ? now + 1 + random.nextInt(5) : -1;
                        l.accept(
                                nextStream,
                                ticket,
                                TYPE,
                                Frames.request(TYPE, nextStream, new byte[0]),
                                deadline < 0 ? null : deadline);
                        written.put(ticket, false);
                        streamOf.put(ticket, nextStream);
                        nextStream += 2;
                        ticket++;
                        break;
                    case 2:
                        box.drain(0);
                        l.onWriteStarted();
                        written.replaceAll((t, w) -> true);
                        break;
                    case 3:
                        forget(l.cancelTicket(random.nextInt((int) ticket + 1)), written, streamOf);
                        break;
                    case 4:
                        forget(
                                l.cancelStream(1 + 2L * random.nextInt((int) ticket + 1), Status.CANCELLED),
                                written,
                                streamOf);
                        break;
                    case 5:
                        forget(l.onResponse(1 + 2L * random.nextInt((int) ticket + 1), TYPE, OK), written, streamOf);
                        break;
                    case 6:
                        forget(
                                l.onReset(1 + 2L * random.nextInt((int) ticket + 1), ErrorCode.CANCEL),
                                written,
                                streamOf);
                        break;
                    default:
                        now += random.nextInt(4);
                        for (OwnRequestLedger.Outcome o : expire(l, now)) {
                            forget(o, written, streamOf);
                        }
                        break;
                }
                assertThat(l.openCount()).as("seed %d step %d", seed, step).isEqualTo(written.size());
                for (long t = 0; t < ticket; t++) {
                    Boolean w = written.get(t);
                    Long s = streamOf.get(t);
                    assertThat(l.streamIdOf(t)).as("seed %d ticket %d", seed, t).isEqualTo(s);
                    if (w == null) {
                        assertThat(l.stateOfTicket(t)).isNull();
                        continue;
                    }
                    Endpoint.RequestState expected =
                            w ? Endpoint.RequestState.WRITING_STARTED : Endpoint.RequestState.NUMBERED_UNWRITTEN;
                    assertThat(l.stateOfTicket(t)).isEqualTo(expected);
                    assertThat(l.stateOfStream(s)).isEqualTo(expected);
                }
            }
        }
    }

    private static void forget(OwnRequestLedger.Outcome outcome, Map<Long, Boolean> written, Map<Long, Long> streamOf) {
        if (outcome == null) {
            return;
        }
        Long ticket = null;
        for (Map.Entry<Long, Long> e : streamOf.entrySet()) {
            if (e.getValue() == outcome.streamId && written.containsKey(e.getKey())) {
                ticket = e.getKey();
            }
        }
        assertThat(ticket).as("끝난 요청은 대기 중이었어야 한다").isNotNull();
        written.remove(ticket);
        streamOf.remove(ticket);
    }

    // ---- 확정성 판정 표: 상태 x 사건 ----

    /** 쓰기 전 취소는 RESET 없이 처리 안 됨. */
    @Test
    void cancelBeforeWriteIsNotProcessedWithoutReset() {
        accept(1, 0);
        assertEnded(ledger.cancelStream(1, Status.CANCELLED), 1, Status.CANCELLED, Certainty.NOT_PROCESSED, false);
    }

    /** 쓰기 시작 뒤 취소는 RESET을 보내고 결과 모름. */
    @Test
    void cancelAfterWriteStartedSendsResetAndIsUnknown() {
        accept(1, 0);
        startWrite();
        assertEnded(ledger.cancelStream(1, Status.CANCELLED), 1, Status.CANCELLED, Certainty.UNKNOWN, true);
    }

    /** 번호표 취소도 같은 판정이다. */
    @Test
    void cancelByTicketJudgesTheSame() {
        accept(1, 0);
        accept(3, 1);
        startWrite();
        accept(5, 2);
        assertEnded(ledger.cancelTicket(0), 1, Status.CANCELLED, Certainty.UNKNOWN, true);
        assertEnded(ledger.cancelTicket(2), 5, Status.CANCELLED, Certainty.NOT_PROCESSED, false);
    }

    /** 기한 만료는 상태에 따라 처리 안 됨 또는 결과 모름. */
    @Test
    void deadlineJudgesByState() {
        accept(1, 0, 5L);
        startWrite();
        accept(3, 1, 5L);
        assertThat(expire(ledger, 5)).isEmpty();
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 6);
        assertThat(ended).hasSize(2);
        assertEnded(ended.get(0), 1, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN, true);
        assertEnded(ended.get(1), 3, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED, false);
    }

    /** 기한 만료는 기한 순이 아니라 스트림 번호 순으로 끝낸다. */
    @Test
    void deadlineEndsInStreamOrderNotDeadlineOrder() {
        accept(1, 0, 9L);
        accept(3, 1, 2L);
        accept(5, 2, 4L);
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 10);
        assertThat(ended).extracting(o -> o.streamId).containsExactly(1L, 3L, 5L);
    }

    /** 응답은 확정성이 RESPONSE이고 쓰기 전이어도 프레임을 거둔다. */
    @Test
    void responseIsResponseCertaintyAndUnwritesUnwrittenFrame() {
        accept(1, 0);
        accept(3, 1);
        assertEnded(ledger.onResponse(1, TYPE, 7), 1, 7, Certainty.RESPONSE, false);
        assertThat(ledger.lastUnwrittenNode()).isNotNull();
        assertThat(drain().size()).isEqualTo(1);
    }

    /** 응답 type이 요청과 다르면 로컬 INTERNAL이다. */
    @Test
    void responseTypeMismatchIsLocalInternal() {
        accept(1, 0);
        assertEnded(ledger.onResponse(1, TYPE + 1, OK), 1, Event.RESPONSE_TYPE_MISMATCH, Certainty.RESPONSE, false);
    }

    /** 예약된 status는 UNKNOWN으로 본다. */
    @Test
    void reservedStatusIsUnknown() {
        accept(1, 0);
        assertEnded(ledger.onResponse(1, TYPE, 200), 1, Status.UNKNOWN, Certainty.RESPONSE, false);
    }

    /** REFUSED RESET은 쓰기 시작 뒤여도 처리 안 됨이다. */
    @Test
    void refusedResetIsNotProcessedEvenAfterWriteStarted() {
        accept(1, 0);
        startWrite();
        assertEnded(ledger.onReset(1, ErrorCode.REFUSED), 1, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
    }

    /** REFUSED가 아닌 RESET은 결과 모름이고 쓰기 전 프레임을 거둔다. */
    @Test
    void otherResetIsUnknownAndUnwritesUnwrittenFrame() {
        accept(1, 0);
        accept(3, 1);
        assertEnded(ledger.onReset(1, ErrorCode.CANCEL), 1, Status.CANCELLED, Certainty.UNKNOWN, false);
        assertThat(drain()).hasSize(1);
        accept(5, 2);
        startWrite();
        assertEnded(ledger.onReset(5, ErrorCode.INTERNAL_ERROR), 5, Status.INTERNAL, Certainty.UNKNOWN, false);
    }

    /** CLOSE last stream id 뒤의 요청은 쓰기 상태와 무관하게 처리 안 됨이고 프레임을 보내지 않는다. */
    @Test
    void requestsAfterCloseLastStreamIdAreNotProcessedAndFramesAreNotSent() {
        accept(1, 0);
        startWrite();
        accept(3, 1);
        accept(5, 2);
        List<OwnRequestLedger.Outcome> ended = ledger.endAfter(1);
        assertThat(ended).hasSize(2);
        assertEnded(ended.get(0), 3, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertEnded(ended.get(1), 5, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertThat(ledger.stateOfStream(1)).isEqualTo(Endpoint.RequestState.WRITING_STARTED);
        assertThat(ledger.stateOfStream(3)).isNull();
        assertThat(drain()).isEmpty();
    }

    /** CLOSE last_stream_id 뒤의 쓰기를 시작한 요청도 처리 안 됨이고 RESET을 보내지 않는다. */
    @Test
    void writtenRequestAfterCloseLastStreamIdIsNotProcessedWithoutReset() {
        accept(1, 0);
        accept(3, 1);
        startWrite();
        accept(5, 2);
        List<OwnRequestLedger.Outcome> ended = ledger.endAfter(1);
        assertThat(ended).hasSize(2);
        assertEnded(ended.get(0), 3, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertEnded(ended.get(1), 5, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertThat(ledger.stateOfStream(1)).isEqualTo(Endpoint.RequestState.WRITING_STARTED);
    }

    /** 쓰기 전 요청의 REFUSED RESET은 처리 안 됨이다. */
    @Test
    void refusedResetOfUnwrittenRequestIsNotProcessed() {
        accept(1, 0);
        assertEnded(ledger.onReset(1, ErrorCode.REFUSED), 1, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertThat(drain()).isEmpty();
    }

    /** 쓰기를 시작한 요청의 응답은 RESPONSE이고 프레임은 이미 나갔다. */
    @Test
    void responseToWrittenRequestIsResponse() {
        accept(1, 0);
        startWrite();
        assertEnded(ledger.onResponse(1, TYPE, 7), 1, 7, Certainty.RESPONSE, false);
        assertThat(ledger.openCount()).isZero();
    }

    /** 전송 계층이 닫히면 쓰기 전은 처리 안 됨 쓰기 시작 뒤는 결과 모름이고 모두 비운다. */
    @Test
    void transportClosedJudgesByStateAndClearsEverything() {
        accept(1, 0);
        startWrite();
        accept(3, 1);
        List<OwnRequestLedger.Outcome> ended = ledger.onTransportClosed();
        assertThat(ended).hasSize(2);
        assertEnded(ended.get(0), 1, Status.UNAVAILABLE, Certainty.UNKNOWN, false);
        assertEnded(ended.get(1), 3, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertThat(ledger.openCount()).isZero();
        assertThat(ledger.streamIdOf(0)).isNull();
        assertThat(ledger.streamIdOf(1)).isNull();
        assertThat(drain()).isEmpty();
        assertThat(expire(ledger, Long.MAX_VALUE)).isEmpty();
    }

    // ---- 쓰기 대기열과 넘긴 번호 ----

    /** 쓰기 전 마지막 요청 노드를 돌려준다. */
    @Test
    void lastUnwrittenRequestNodeIsReported() {
        assertThat(ledger.lastUnwrittenNode()).isNull();
        accept(1, 0);
        accept(3, 1);
        Outbox.Node last = ledger.lastUnwrittenNode();
        assertThat(last).isNotNull();
        assertThat(ledger.cancelStream(3, Status.CANCELLED)).isNotNull();
        assertThat(ledger.lastUnwrittenNode()).isNotSameAs(last).isNotNull();
        startWrite();
        assertThat(ledger.lastUnwrittenNode()).isNull();
    }

    /** 넘긴 가장 큰 번호는 쓰기 시작 사건에서만 바뀐다. */
    @Test
    void highestWrittenNumberChangesOnlyOnWriteStarted() {
        assertThat(ledger.highestWrittenStreamId()).isZero();
        accept(1, 0);
        accept(3, 1);
        assertThat(ledger.highestWrittenStreamId()).isZero();
        startWrite();
        assertThat(ledger.highestWrittenStreamId()).isEqualTo(3);
        accept(5, 2);
        ledger.cancelStream(5, Status.CANCELLED);
        ledger.onResponse(1, TYPE, OK);
        assertThat(ledger.highestWrittenStreamId()).isEqualTo(3);
    }

    /** Endpoint가 메시지를 꺼낸 뒤 쓰기 시작 사건을 알리는 순서를 따른다. */
    private void startWrite() {
        outbox.drain(0);
        ledger.onWriteStarted();
    }

    private List<OutboundMessage> drain() {
        return new ArrayList<>(outbox.drain(0));
    }
}
