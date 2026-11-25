package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

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

    private void accept(long streamId, long ticket, Long deadlineAt) {
        assertThat(ledger.nextTicket()).isEqualTo(ticket);
        assertThat(ledger.open(TYPE, new byte[0], deadlineAt, MAX_FRAME))
                .isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.streamIdOf(ticket)).isEqualTo(streamId);
    }

    @Test
    void outcomeTellsNumberedFromQueuedRequests() {
        accept(1, 0);
        long queuedTicket = ledger.queue(TYPE, new byte[0], null);
        assertThat(ledger.cancelTicket(0).numbered()).isTrue();
        assertThat(ledger.cancelTicket(queuedTicket).numbered()).isFalse();
    }

    private void assertEnded(
            OwnRequestLedger.Outcome outcome, long streamId, int status, Certainty certainty, boolean reset) {
        assertThat(outcome).isNotNull();
        assertThat(outcome.streamId).isEqualTo(streamId);
        assertThat(outcome.status).isEqualTo(status);
        assertThat(outcome.certainty).isEqualTo(certainty);
        assertThat(outcome.resetToSend).isEqualTo(reset);
    }

    @Test
    void ticketAndStreamLookupsFindTheSameRequest() {
        accept(1, 0);
        assertThat(ledger.streamIdOf(0)).isEqualTo(1L);
        assertThat(ledger.stateOfTicket(0)).isEqualTo(OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN);
        assertThat(ledger.stateOfStream(1)).isEqualTo(OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN);
        assertThat(ledger.openCount()).isEqualTo(1);
    }

    @Test
    void endedRequestIsInNoIndex() {
        accept(1, 0);
        startWrite();
        ledger.onResponse(1, TYPE, OK, new byte[0]);
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
        assertThat(writeAll(box, l)).as("쓰기 전 요청의 프레임만 쓰기 대기열에 있다 " + at).hasSize((int) unwritten);
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
                response ? l.onResponse(streamId, TYPE, OK, new byte[0]) : l.onReset(streamId, ErrorCode.CANCEL);
        Req r = m.numberedInStreamOrder().stream()
                .filter(x -> x.streamId == streamId)
                .findFirst()
                .orElse(null);
        if (r == null || r.phase == Phase.UNWRITTEN) {

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

    private void transportClosed(OwnRequestLedger l, Model m, String at) {
        List<Req> expected = new ArrayList<>(m.numberedInStreamOrder());
        expected.addAll(m.queuedInTicketOrder());
        List<OwnRequestLedger.Outcome> actual = l.onTransportClosed(false);
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

    @Test
    void deadlineOfQueuedRequestIsNotProcessed() {
        ledger.queue(TYPE, new byte[0], 5L);
        assertThat(expire(ledger, 5)).isEmpty();
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 6);
        assertThat(ended).hasSize(1);
        assertEnded(ended.get(0), 0, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED, false);
    }

    @Test
    void expiryEndsQueuedBeforeNumberedWhateverTheDeadline() {
        accept(1, 0, 3L);
        accept(3, 1, 2L);
        ledger.queue(TYPE, new byte[0], 9L);
        ledger.queue(TYPE, new byte[0], 8L);
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 10);
        assertThat(ended).extracting(o -> o.streamId).containsExactly(0L, 0L, 1L, 3L);
    }

    @Test
    void deadlineSurvivesNumbering() {
        ledger.queue(TYPE, new byte[0], 5L);
        assertThat(ledger.openQueued(Long.MAX_VALUE, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 6);
        assertThat(ended).hasSize(1);
        assertEnded(ended.get(0), 1, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED, false);
        assertThat(drain()).isEmpty();
    }

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

    @Test
    void refusalConsumesATicketButNoStreamNumber() {
        ledger.setPeerMaxConcurrentStreams(1);
        accept(1, 0);
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.STREAM_LIMIT);
        assertThat(ledger.nextTicket()).isEqualTo(2);
        startWrite();
        assertThat(ledger.onResponse(1, TYPE, OK, new byte[0])).isNotNull();
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.streamIdOf(2)).isEqualTo(3L);
    }

    @Test
    void oversizeConsumesNeitherTicketNorStreamNumber() {
        assertThat(ledger.open(TYPE, new byte[100], null, 64)).isEqualTo(OwnRequestLedger.Admission.OVERSIZE);
        assertThat(ledger.nextTicket()).isZero();
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        assertThat(ledger.streamIdOf(0)).isEqualTo(1L);
    }

    @Test
    void exhaustedNumbersRefuseBeforeTheConcurrencyLimit() {
        Outbox lastOutbox = new Outbox();
        OwnRequestLedger last = new OwnRequestLedger(lastOutbox, Long.MAX_VALUE, 1);
        last.setPeerMaxConcurrentStreams(1);
        assertThat(last.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.ADMITTED);
        writeAll(lastOutbox, last);
        assertThat(last.onResponse(Long.MAX_VALUE, TYPE, OK, new byte[0])).isNotNull();
        assertThat(last.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.EXHAUSTED);
    }

    @Test
    void closingRefusesNewRequestsFirst() {
        ledger.setPeerMaxConcurrentStreams(0);
        ledger.onClosing();
        assertThat(ledger.open(TYPE, new byte[0], null, MAX_FRAME)).isEqualTo(OwnRequestLedger.Admission.CLOSING);
    }

    @Test
    void issueTicketHandsOutTicketsInOrder() {
        assertThat(ledger.issueTicket()).isZero();
        assertThat(ledger.issueTicket()).isEqualTo(1);
        assertThat(ledger.nextTicket()).isEqualTo(2);
    }

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

    @Test
    void cancelBeforeWriteIsNotProcessedWithoutReset() {
        accept(1, 0);
        assertEnded(ledger.cancelStream(1, Status.CANCELLED), 1, Status.CANCELLED, Certainty.NOT_PROCESSED, false);
    }

    @Test
    void cancelAfterWriteStartedSendsResetAndIsUnknown() {
        accept(1, 0);
        startWrite();
        assertEnded(ledger.cancelStream(1, Status.CANCELLED), 1, Status.CANCELLED, Certainty.UNKNOWN, true);
    }

    @Test
    void cancelByTicketJudgesTheSame() {
        accept(1, 0);
        accept(3, 1);
        startWrite();
        accept(5, 2);
        assertEnded(ledger.cancelTicket(0), 1, Status.CANCELLED, Certainty.UNKNOWN, true);
        assertEnded(ledger.cancelTicket(2), 5, Status.CANCELLED, Certainty.NOT_PROCESSED, false);
    }

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

    @Test
    void deadlineEndsInStreamOrderNotDeadlineOrder() {
        accept(1, 0, 9L);
        accept(3, 1, 2L);
        accept(5, 2, 4L);
        List<OwnRequestLedger.Outcome> ended = expire(ledger, 10);
        assertThat(ended).extracting(o -> o.streamId).containsExactly(1L, 3L, 5L);
    }

    @Test
    void responseToUnhandedRequestIsDiscardedAndKeepsFrame() {
        accept(1, 0);
        accept(3, 1);
        assertThat(ledger.onResponse(1, TYPE, 7, new byte[0])).isNull();
        assertThat(ledger.openCount()).isEqualTo(2);
        assertThat(ledger.stateOfStream(1)).isEqualTo(OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN);
        assertThat(drain()).hasSize(2);
    }

    @Test
    void responseTypeMismatchIsLocalInternal() {
        accept(1, 0);
        startWrite();
        assertEnded(
                ledger.onResponse(1, TYPE + 1, OK, new byte[0]),
                1,
                Event.RESPONSE_TYPE_MISMATCH,
                Certainty.RESPONSE,
                false);
    }

    @Test
    void reservedStatusIsUnknown() {
        accept(1, 0);
        startWrite();
        assertEnded(ledger.onResponse(1, TYPE, 200, new byte[0]), 1, Status.UNKNOWN, Certainty.RESPONSE, false);
    }

    @Test
    void refusedResetIsNotProcessedEvenAfterWriteStarted() {
        accept(1, 0);
        startWrite();
        assertEnded(ledger.onReset(1, ErrorCode.REFUSED), 1, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
    }

    @Test
    void resetToUnhandedRequestIsDiscardedAndHandedOffOneIsUnknown() {
        accept(1, 0);
        accept(3, 1);
        assertThat(ledger.onReset(1, ErrorCode.CANCEL)).isNull();
        assertThat(ledger.openCount()).isEqualTo(2);
        assertThat(drain()).hasSize(2);
        accept(5, 2);
        startWrite();
        assertEnded(ledger.onReset(5, ErrorCode.INTERNAL_ERROR), 5, Status.INTERNAL, Certainty.UNKNOWN, false);
    }

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

    @Test
    void refusedResetOfUnwrittenRequestIsDiscarded() {
        accept(1, 0);
        assertThat(ledger.onReset(1, ErrorCode.REFUSED)).isNull();
        assertThat(drain()).hasSize(1);
    }

    @Test
    void responseToWrittenRequestIsResponse() {
        accept(1, 0);
        startWrite();
        assertEnded(ledger.onResponse(1, TYPE, 7, new byte[0]), 1, 7, Certainty.RESPONSE, false);
        assertThat(ledger.openCount()).isZero();
    }

    @Test
    void transportClosedJudgesByStateAndClearsEverything() {
        accept(1, 0);
        startWrite();
        accept(3, 1);
        List<OwnRequestLedger.Outcome> ended = ledger.onTransportClosed(false);
        assertThat(ended).hasSize(2);
        assertEnded(ended.get(0), 1, Status.UNAVAILABLE, Certainty.UNKNOWN, false);
        assertEnded(ended.get(1), 3, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false);
        assertThat(ledger.openCount()).isZero();
        assertThat(ledger.streamIdOf(0)).isNull();
        assertThat(ledger.streamIdOf(1)).isNull();
        assertThat(drain()).isEmpty();
        assertThat(expire(ledger, Long.MAX_VALUE)).isEmpty();
    }

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
        ledger.onResponse(1, TYPE, OK, new byte[0]);
        assertThat(ledger.highestWrittenStreamId()).isEqualTo(3);
    }

    @Test
    void unwritingTwiceLeavesTheUnwrittenListIntact() throws Exception {
        accept(1, 0);
        accept(3, 1);
        accept(5, 2);
        java.lang.reflect.Field byTicket = OwnRequestLedger.class.getDeclaredField("byTicket");
        byTicket.setAccessible(true);
        Object middle = ((java.util.Map<?, ?>) byTicket.get(ledger)).get(1L);
        java.lang.reflect.Method unwrite = OwnRequestLedger.class.getDeclaredMethod("unwrite", middle.getClass());
        unwrite.setAccessible(true);
        unwrite.invoke(ledger, middle);
        unwrite.invoke(ledger, middle);

        assertThat(ledger.lastUnwrittenNode()).isNotNull();
        assertThat(drain()).hasSize(2);
        assertThat(ledger.stateOfStream(1)).isEqualTo(OwnRequestLedger.RequestState.WRITING_STARTED);
        assertThat(ledger.stateOfStream(5)).isEqualTo(OwnRequestLedger.RequestState.WRITING_STARTED);
        assertThat(ledger.lastUnwrittenNode()).isNull();
    }

    private void startWrite() {
        writeAll(outbox, ledger);
    }

    private List<OutboundMessage> drain() {
        return writeAll(outbox, ledger);
    }

    private static List<OutboundMessage> writeAll(Outbox box, OwnRequestLedger l) {
        List<OutboundMessage> written = new ArrayList<>();
        for (Outbox.Node node = box.first(); node != null; node = box.first()) {
            written.add(node.message);
            l.onWriteStarted(node);
            box.removeForWriting(node);
            l.onWriteCompleted(node);
        }
        return written;
    }
}
