package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * 내 요청 장부(#217)의 internal seam 테스트. 장부 interface 너머로 보이는 결과만 확인한다: 번호표 색인과 스트림 색인의 동기화, 끝난 요청이
 * 어떤 색인에도 남지 않음, 확정성 판정 표(상태 x 사건, 규격 3.5절, 4.4절, 4.4.1절), 기한 만료 순서, 넘긴 가장 큰 번호의 갱신 시점.
 */
class OwnRequestLedgerTest {

    private static final int TYPE = 0x0100;
    private static final int OK = 0;
    private static final long MAX_FRAME = HelloParams.DEFAULT_MAX_FRAME_SIZE;

    private final Outbox outbox = new Outbox();
    private final OwnRequestLedger ledger = new OwnRequestLedger(outbox, 1, 1);

    private static List<OwnRequestLedger.Outcome> expire(OwnRequestLedger target, long now) {
        List<OwnRequestLedger.Outcome> ended = new ArrayList<>();
        target.expire(now, ended::add);
        return ended;
    }

    private void accept(long streamId, long ticket) {
        accept(streamId, ticket, null);
    }

    /** 번호표와 스트림 번호는 장부가 정한다. 기대한 값과 같은지만 확인한다. */
    private void accept(long streamId, long ticket, Long deadlineAt) {
        assertThat(ledger.nextTicket()).isEqualTo(ticket);
        assertThat(ledger.open(TYPE, new byte[0], deadlineAt, MAX_FRAME))
                .isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.streamIdOf(ticket)).isEqualTo(streamId);
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
        accept(1, 0);
        assertThat(ledger.streamIdOf(0)).isEqualTo(1L);
        assertThat(ledger.stateOfTicket(0)).isEqualTo(OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN);
        assertThat(ledger.stateOfStream(1)).isEqualTo(OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN);
        assertThat(ledger.openCount()).isEqualTo(1);
    }

    /** 끝난 요청은 어떤 색인에도 없다. */
    @Test
    void endedRequestIsInNoIndex() {
        accept(1, 0);
        ledger.onResponse(1, TYPE, OK);
        assertThat(ledger.streamIdOf(0)).isNull();
        assertThat(ledger.stateOfTicket(0)).isNull();
        assertThat(ledger.stateOfStream(1)).isNull();
        assertThat(ledger.openCount()).isZero();
        assertThat(ledger.cancelTicket(0)).isNull();
        assertThat(ledger.cancelStream(1, Status.CANCELLED)).isNull();
    }

    private enum Phase {
        QUEUED,
        UNWRITTEN,
        WRITTEN
    }

    /** 장부와 독립된 모델의 요청 하나. 모델은 번호표 순서로만 줄 세운다. */
    private static final class Req {
        final long ticket;
        final Long deadlineAt;
        Phase phase = Phase.QUEUED;
        long streamId;

        Req(long ticket, Long deadlineAt) {
            this.ticket = ticket;
            this.deadlineAt = deadlineAt;
        }
    }

    /** 장부 없이 규격 3.5절, 4.4절, 4.4.1절로 정한 기대값을 계산하는 모델. */
    private static final class Model {
        final TreeMap<Long, Req> byTicket = new TreeMap<>();
        long nextTicket;
        long nextStream;
        long now;
        boolean gate;
        boolean closing;
        final long maxConcurrent;

        Model(long firstStream, long maxConcurrent) {
            this.nextStream = firstStream;
            this.maxConcurrent = maxConcurrent;
        }

        int numbered() {
            int count = 0;
            for (Req r : byTicket.values()) {
                if (r.phase != Phase.QUEUED) {
                    count++;
                }
            }
            return count;
        }

        List<Req> numberedInStreamOrder() {
            List<Req> list = new ArrayList<>();
            for (Req r : byTicket.values()) {
                if (r.phase != Phase.QUEUED) {
                    list.add(r);
                }
            }
            list.sort(Comparator.comparingLong(r -> r.streamId));
            return list;
        }

        List<Req> queuedInTicketOrder() {
            List<Req> list = new ArrayList<>();
            for (Req r : byTicket.values()) {
                if (r.phase == Phase.QUEUED) {
                    list.add(r);
                }
            }
            return list;
        }

        /** 번호를 줄 수 있는지. 닫는 중 -> 소진은 이 모델에 없음 -> 한도 순이다. */
        OwnRequestLedger.Admission admission() {
            if (closing) {
                return OwnRequestLedger.Admission.CLOSING;
            }
            if (numbered() >= maxConcurrent) {
                return OwnRequestLedger.Admission.STREAM_LIMIT;
            }
            return OwnRequestLedger.Admission.ADMITTED;
        }

        static Certainty unfinished(Req r) {
            return r.phase == Phase.WRITTEN ? Certainty.UNKNOWN : Certainty.NOT_PROCESSED;
        }
    }

    private static void assertOutcome(OwnRequestLedger.Outcome actual, Req expected, int status, Certainty certainty) {
        assertThat(actual).isNotNull();
        assertThat(actual.streamId).isEqualTo(expected.streamId);
        assertThat(actual.status).isEqualTo(status);
        assertThat(actual.certainty).isEqualTo(certainty);
        assertThat(actual.resetToSend).isEqualTo(certainty == Certainty.UNKNOWN && status != Status.UNAVAILABLE);
    }

    /** 무작위 사건 순서에서 색인 동기화, 끝난 요청이 색인에 남지 않음, 결과의 순서와 확정성을 독립된 모델과 비교한다. */
    @Test
    void ledgerAgreesWithModelUnderRandomEventOrder() {
        for (long seed = 0; seed < 400; seed++) {
            runScenario(seed);
        }
    }

    private void runScenario(long seed) {
        Random random = new Random(seed);
        boolean server = seed % 2 == 1;
        long first = server ? 2 : 1;
        Outbox box = new Outbox();
        OwnRequestLedger l = new OwnRequestLedger(box, first, server ? 0 : 1);
        long limit = seed % 3 == 0 ? 3 : HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
        l.setPeerMaxConcurrentStreams(limit);
        Model m = new Model(first, limit);
        for (int step = 0; step < 80; step++) {
            String at = "seed " + seed + " step " + step;
            switch (random.nextInt(12)) {
                case 0:
                case 1:
                case 2:
                    submit(l, m, random, at);
                    break;
                case 3:
                    m.gate = true;
                    openQueued(l, m, Long.MAX_VALUE, at);
                    break;
                case 4:
                    openQueued(l, m, random.nextInt((int) m.nextTicket + 1), at);
                    break;
                case 5:
                    writeStarted(l, m, box, at);
                    break;
                case 6:
                    cancelTicket(l, m, random.nextInt((int) m.nextTicket + 2) - 1, at);
                    break;
                case 7:
                    cancelStream(l, m, first + 2L * random.nextInt((int) m.nextTicket + 2), at);
                    break;
                case 8:
                    respondOrReset(l, m, first + 2L * random.nextInt((int) m.nextTicket + 2), random.nextBoolean(), at);
                    break;
                case 9:
                    advance(l, m, 1 + random.nextInt(4), at);
                    break;
                case 10:
                    close(l, m, random, at);
                    break;
                default:
                    if (random.nextInt(6) == 0) {
                        transportClosed(l, m, at);
                        return;
                    }
                    break;
            }
            assertInSync(l, m, at);
        }
        transportClosed(l, m, "seed " + seed + " end");
    }

    private void submit(OwnRequestLedger l, Model m, Random random, String at) {
        long ticket = m.nextTicket;
        assertThat(l.nextTicket()).as(at).isEqualTo(ticket);
        Long deadline = random.nextBoolean() ? m.now + 1 + random.nextInt(5) : null;
        byte[] payload = new byte[random.nextInt(3)];
        if (!m.gate) {
            assertThat(l.queue(TYPE, payload, deadline)).as(at).isEqualTo(ticket);
            m.byTicket.put(ticket, new Req(ticket, deadline));
            m.nextTicket++;
            return;
        }
        OwnRequestLedger.Admission expected = m.admission();
        assertThat(l.open(TYPE, payload, deadline, MAX_FRAME)).as(at).isEqualTo(expected);
        m.nextTicket++;
        if (expected == OwnRequestLedger.Admission.ADMITTED) {
            number(m, new Req(ticket, deadline));
        }
    }

    private static void number(Model m, Req r) {
        r.phase = Phase.UNWRITTEN;
        r.streamId = m.nextStream;
        m.nextStream += 2;
        m.byTicket.put(r.ticket, r);
    }

    private void openQueued(OwnRequestLedger l, Model m, long before, String at) {
        while (true) {
            List<Req> queued = m.queuedInTicketOrder();
            Req head = queued.isEmpty() ? null : queued.get(0);
            OwnRequestLedger.Admission actual = l.openQueued(before, MAX_FRAME);
            if (head == null || head.ticket >= before) {
                assertThat(actual).as(at).isEqualTo(OwnRequestLedger.Admission.NONE);
                return;
            }
            OwnRequestLedger.Admission expected = m.admission();
            assertThat(actual).as(at).isEqualTo(expected);
            if (expected == OwnRequestLedger.Admission.ADMITTED) {
                number(m, head);
            } else {
                m.byTicket.remove(head.ticket);
            }
        }
    }

    private void writeStarted(OwnRequestLedger l, Model m, Outbox box, String at) {
        long unwritten = m.byTicket.values().stream()
                .filter(r -> r.phase == Phase.UNWRITTEN)
                .count();
        assertThat(box.drain(0)).as("쓰기 전 요청의 프레임만 쓰기 대기열에 있다 " + at).hasSize((int) unwritten);
        l.onWriteStarted();
        m.byTicket.values().stream().filter(r -> r.phase == Phase.UNWRITTEN).forEach(r -> r.phase = Phase.WRITTEN);
    }

    private void cancelTicket(OwnRequestLedger l, Model m, long ticket, String at) {
        OwnRequestLedger.Outcome actual = l.cancelTicket(ticket);
        Req r = m.byTicket.remove(ticket);
        if (r == null) {
            assertThat(actual).as(at).isNull();
            return;
        }
        assertOutcome(actual, r, Status.CANCELLED, Model.unfinished(r));
    }

    private void cancelStream(OwnRequestLedger l, Model m, long streamId, String at) {
        OwnRequestLedger.Outcome actual = l.cancelStream(streamId, Status.CANCELLED);
        Req r = m.numberedInStreamOrder().stream()
                .filter(x -> x.streamId == streamId)
                .findFirst()
                .orElse(null);
        if (r == null) {
            assertThat(actual).as(at).isNull();
            return;
        }
        m.byTicket.remove(r.ticket);
        assertOutcome(actual, r, Status.CANCELLED, Model.unfinished(r));
    }

    private void respondOrReset(OwnRequestLedger l, Model m, long streamId, boolean response, String at) {
        OwnRequestLedger.Outcome actual =
                response ? l.onResponse(streamId, TYPE, OK) : l.onReset(streamId, ErrorCode.CANCEL);
        Req r = m.numberedInStreamOrder().stream()
                .filter(x -> x.streamId == streamId)
                .findFirst()
                .orElse(null);
        if (r == null) {
            assertThat(actual).as(at).isNull();
            return;
        }
        m.byTicket.remove(r.ticket);
        assertThat(actual.streamId).as(at).isEqualTo(streamId);
        assertThat(actual.certainty).as(at).isEqualTo(response ? Certainty.RESPONSE : Certainty.UNKNOWN);
        assertThat(actual.resetToSend).isFalse();
    }

    private void advance(OwnRequestLedger l, Model m, int ms, String at) {
        m.now += ms;
        List<Req> due = new ArrayList<>(m.queuedInTicketOrder());
        due.removeIf(r -> r.deadlineAt == null || m.now <= r.deadlineAt);
        List<Req> dueNumbered = new ArrayList<>(m.numberedInStreamOrder());
        dueNumbered.removeIf(r -> r.deadlineAt == null || m.now <= r.deadlineAt);
        due.addAll(dueNumbered);
        List<OwnRequestLedger.Outcome> actual = expire(l, m.now);
        assertThat(actual).as(at).hasSameSizeAs(due);
        for (int i = 0; i < due.size(); i++) {
            assertOutcome(actual.get(i), due.get(i), Status.DEADLINE_EXCEEDED, Model.unfinished(due.get(i)));
            m.byTicket.remove(due.get(i).ticket);
        }
    }

    private void close(OwnRequestLedger l, Model m, Random random, String at) {
        if (random.nextBoolean()) {
            List<Req> numbered = m.numberedInStreamOrder();
            long last = numbered.isEmpty() || random.nextBoolean()
                    ? 0
                    : numbered.get(random.nextInt(numbered.size())).streamId;
            List<Req> cut = new ArrayList<>(numbered);
            cut.removeIf(r -> r.streamId <= last);
            List<OwnRequestLedger.Outcome> actual = l.endAfter(last);
            assertThat(actual).as(at).hasSameSizeAs(cut);
            for (int i = 0; i < cut.size(); i++) {
                assertOutcome(actual.get(i), cut.get(i), Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
                m.byTicket.remove(cut.get(i).ticket);
            }
        }
        List<Req> queued = m.queuedInTicketOrder();
        List<OwnRequestLedger.Outcome> actual = l.onClosing();
        assertThat(actual).as(at).hasSameSizeAs(queued);
        for (int i = 0; i < queued.size(); i++) {
            assertOutcome(actual.get(i), queued.get(i), Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
            m.byTicket.remove(queued.get(i).ticket);
        }
        m.closing = true;
    }

    /** 전송 계층 닫힘은 번호 받은 요청이 먼저(스트림 번호 순), 번호 없는 요청이 뒤(낸 순서)다. */
    private void transportClosed(OwnRequestLedger l, Model m, String at) {
        List<Req> expected = new ArrayList<>(m.numberedInStreamOrder());
        expected.addAll(m.queuedInTicketOrder());
        List<OwnRequestLedger.Outcome> actual = l.onTransportClosed();
        assertThat(actual).as(at).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertOutcome(actual.get(i), expected.get(i), Status.UNAVAILABLE, Model.unfinished(expected.get(i)));
        }
        m.byTicket.clear();
        assertInSync(l, m, at);
        assertThat(expire(l, Long.MAX_VALUE)).as(at).isEmpty();
    }

    private void assertInSync(OwnRequestLedger l, Model m, String at) {
        assertThat(l.openCount()).as(at).isEqualTo(m.numbered());
        for (long t = 0; t < m.nextTicket; t++) {
            Req r = m.byTicket.get(t);
            if (r == null) {
                assertThat(l.stateOfTicket(t)).as("ticket %d %s", t, at).isNull();
                assertThat(l.streamIdOf(t)).as("ticket %d %s", t, at).isNull();
                continue;
            }
            OwnRequestLedger.RequestState state = r.phase == Phase.QUEUED
                    ? OwnRequestLedger.RequestState.QUEUED_WITHOUT_NUMBER
                    : r.phase == Phase.UNWRITTEN
                            ? OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN
                            : OwnRequestLedger.RequestState.WRITING_STARTED;
            assertThat(l.stateOfTicket(t)).as("ticket %d %s", t, at).isEqualTo(state);
            if (r.phase == Phase.QUEUED) {
                assertThat(l.streamIdOf(t)).as("ticket %d %s", t, at).isNull();
            } else {
                assertThat(l.streamIdOf(t)).as("ticket %d %s", t, at).isEqualTo(r.streamId);
                assertThat(l.stateOfStream(r.streamId)).as(at).isEqualTo(state);
            }
        }
    }

    // ---- 번호 없는 요청: 확정성 판정 표의 번호 없음 행, 만료 순서, 번호표 발급 ----

    /** 번호 없는 요청의 취소는 스트림 번호 없이 처리 안 됨이고 RESET이 없다. */
    @Test
    void cancelQueuedRequestIsNotProcessedWithoutStreamAndReset() {
        long ticket = ledger.queue(TYPE, new byte[0], null);
        assertThat(ledger.stateOfTicket(ticket)).isEqualTo(OwnRequestLedger.RequestState.QUEUED_WITHOUT_NUMBER);
        assertThat(ledger.streamIdOf(ticket)).isNull();
        assertThat(ledger.openCount()).isZero();
        assertEnded(ledger.cancelTicket(ticket), 0, Status.CANCELLED, Certainty.NOT_PROCESSED, false);
        assertThat(ledger.stateOfTicket(ticket)).isNull();
        assertThat(ledger.cancelTicket(ticket)).isNull();
    }

    /** 번호 없는 요청의 기한 만료는 처리 안 됨이다. */
    @Test
    void deadlineOfQueuedRequestIsNotProcessed() {
        ledger.queue(TYPE, new byte[0], 5L);
        assertThat(expire(ledger, 5)).isEmpty();
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 6);
        assertThat(ended).hasSize(1);
        assertEnded(ended.get(0), 0, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED, false);
    }

    /** 만료는 번호 없는 요청이 먼저(낸 순서) 번호 받은 요청이 뒤(스트림 번호 순)이고, 기한이 늦어도 그렇다. */
    @Test
    void expiryEndsQueuedBeforeNumberedWhateverTheDeadline() {
        accept(1, 0, 3L);
        accept(3, 1, 2L);
        ledger.queue(TYPE, new byte[0], 9L); // 번호표 2
        ledger.queue(TYPE, new byte[0], 8L); // 번호표 3
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 10);
        assertThat(ended).extracting(o -> o.streamId).containsExactly(0L, 0L, 1L, 3L);
    }

    /** 번호를 받아도 기한은 그대로이고 번호 받은 요청으로 끝난다. */
    @Test
    void deadlineSurvivesNumbering() {
        ledger.queue(TYPE, new byte[0], 5L);
        assertThat(ledger.openQueued(Long.MAX_VALUE, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 6);
        assertThat(ended).hasSize(1);
        assertEnded(ended.get(0), 1, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED, false);
        assertThat(drain()).isEmpty();
    }

    /** 번호 없는 요청은 번호표 순서대로 번호를 받고 상한 번호표 앞의 것만 받는다. */
    @Test
    void queuedRequestsGetNumbersInTicketOrderBelowTheLimit() {
        long a = ledger.queue(TYPE, new byte[0], null);
        long b = ledger.queue(TYPE, new byte[0], null);
        long c = ledger.queue(TYPE, new byte[0], null);
        assertThat(ledger.openQueued(c, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.openQueued(c, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.openQueued(c, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.NONE);
        assertThat(ledger.streamIdOf(a)).isEqualTo(1L);
        assertThat(ledger.streamIdOf(b)).isEqualTo(3L);
        assertThat(ledger.stateOfTicket(c)).isEqualTo(OwnRequestLedger.RequestState.QUEUED_WITHOUT_NUMBER);
    }

    /** 번호를 줄 수 없는 요청은 번호표만 쓰고 스트림 번호는 쓰지 않는다. */
    @Test
    void refusalConsumesATicketButNoStreamNumber() {
        ledger.setPeerMaxConcurrentStreams(1);
        accept(1, 0);
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.STREAM_LIMIT);
        assertThat(ledger.nextTicket()).isEqualTo(2);
        assertThat(ledger.onResponse(1, TYPE, OK)).isNotNull();
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.streamIdOf(2)).isEqualTo(3L);
    }

    /** 프레임이 상대의 max_frame_size를 넘으면 번호표도 스트림 번호도 쓰지 않는다. */
    @Test
    void oversizeConsumesNeitherTicketNorStreamNumber() {
        assertThat(ledger.open(TYPE, new byte[100], null, 64)).isEqualTo(OwnRequestLedger.Admission.OVERSIZE);
        assertThat(ledger.nextTicket()).isZero();
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.streamIdOf(0)).isEqualTo(1L);
    }

    /** 번호가 소진되면 요청을 받지 않는다. 한도보다 먼저 판정한다. */
    @Test
    void exhaustedNumbersRefuseBeforeTheConcurrencyLimit() {
        OwnRequestLedger last = new OwnRequestLedger(new Outbox(), Long.MAX_VALUE, 1);
        last.setPeerMaxConcurrentStreams(1);
        assertThat(last.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(last.onResponse(Long.MAX_VALUE, TYPE, OK)).isNotNull();
        assertThat(last.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.EXHAUSTED);
    }

    /** 닫는 중에는 새 요청을 받지 않는다. 소진이나 한도보다 먼저 판정한다. */
    @Test
    void closingRefusesNewRequestsFirst() {
        ledger.setPeerMaxConcurrentStreams(0);
        ledger.onClosing();
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.CLOSING);
    }

    /** 전송 계층이 닫힌 뒤의 번호표 발급. */
    @Test
    void issueTicketHandsOutTicketsInOrder() {
        assertThat(ledger.issueTicket()).isZero();
        assertThat(ledger.issueTicket()).isEqualTo(1);
        assertThat(ledger.nextTicket()).isEqualTo(2);
    }

    /** 번호 없는 요청이 있어도 닫는 중 처리는 번호 받은 요청을 건드리지 않는다. */
    @Test
    void closingEndsOnlyQueuedRequests() {
        accept(1, 0);
        ledger.queue(TYPE, new byte[0], null);
        ledger.queue(TYPE, new byte[0], null);
        List<OwnRequestLedger.Outcome> ended = ledger.onClosing();
        assertThat(ended).hasSize(2);
        assertEnded(ended.get(0), 0, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertThat(ledger.stateOfStream(1)).isEqualTo(OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN);
        assertThat(ledger.openQueued(Long.MAX_VALUE, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.NONE);
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
        assertThat(ledger.stateOfStream(1)).isEqualTo(OwnRequestLedger.RequestState.WRITING_STARTED);
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
        assertThat(ledger.stateOfStream(1)).isEqualTo(OwnRequestLedger.RequestState.WRITING_STARTED);
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
