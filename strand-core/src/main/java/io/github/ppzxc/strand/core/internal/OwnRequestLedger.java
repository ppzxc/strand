package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

final class OwnRequestLedger {

    private static final byte[] NO_BODY = new byte[0];

    enum RequestState {
        QUEUED_WITHOUT_NUMBER,

        NUMBERED_UNWRITTEN,

        WRITING_STARTED
    }

    enum Admission {
        ADMITTED,

        NONE,

        CLOSING,

        EXHAUSTED,

        STREAM_LIMIT,

        OVERSIZE
    }

    static final class Outcome {

        final long ticket;

        final long streamId;
        final int status;
        final Certainty certainty;

        final byte[] body;

        final boolean resetToSend;

        private final boolean numbered;

        Outcome(long ticket, long streamId, int status, Certainty certainty, boolean resetToSend, boolean numbered) {
            this(ticket, streamId, status, certainty, NO_BODY, resetToSend, numbered);
        }

        Outcome(
                long ticket,
                long streamId,
                int status,
                Certainty certainty,
                byte[] body,
                boolean resetToSend,
                boolean numbered) {
            this.body = body;
            this.ticket = ticket;
            this.streamId = streamId;
            this.status = status;
            this.certainty = certainty;
            this.resetToSend = resetToSend;
            this.numbered = numbered;
        }

        boolean numbered() {
            return numbered;
        }
    }

    private static final class Record {
        final long ticket;
        final int type;

        final byte[] extensions;

        byte @Nullable [] payload;

        final @Nullable Expiry expiry;

        long streamId;

        RequestState state = RequestState.QUEUED_WITHOUT_NUMBER;

        Outbox.@Nullable Node node;

        @Nullable
        Record prevUnwritten;

        @Nullable
        Record nextUnwritten;

        boolean linked;

        boolean delivered;

        @Nullable
        Record prevQueued;

        @Nullable
        Record nextQueued;

        Record(long ticket, int type, byte[] extensions, byte @Nullable [] payload, @Nullable Long deadlineAt) {
            this.ticket = ticket;
            this.type = type;
            this.extensions = extensions;
            this.payload = payload;
            this.expiry = deadlineAt == null ? null : new Expiry(deadlineAt, ticket, this);
        }
    }

    private static final class Expiry implements Comparable<Expiry> {
        final long deadlineAt;
        final long ticket;
        final Record record;

        Expiry(long deadlineAt, long ticket, Record record) {
            this.deadlineAt = deadlineAt;
            this.ticket = ticket;
            this.record = record;
        }

        @Override
        public int compareTo(Expiry other) {
            int byDeadline = Long.compare(deadlineAt, other.deadlineAt);
            return byDeadline != 0 ? byDeadline : Long.compare(ticket, other.ticket);
        }
    }

    private static final Comparator<Record> EXPIRY_ORDER = (a, b) -> {
        boolean aQueued = a.state == RequestState.QUEUED_WITHOUT_NUMBER;
        if (aQueued != (b.state == RequestState.QUEUED_WITHOUT_NUMBER)) {
            return aQueued ? -1 : 1;
        }
        return aQueued ? Long.compare(a.ticket, b.ticket) : Long.compare(a.streamId, b.streamId);
    };

    private final Outbox outbox;

    private final Map<Long, Record> byStream = new LinkedHashMap<>();

    private final Map<Long, Record> byTicket = new HashMap<>();

    private final Map<Long, Record> queued = new HashMap<>();

    private final TreeSet<Expiry> expirations = new TreeSet<>();

    private final int ownParity;

    private @Nullable Record firstQueued;
    private @Nullable Record lastQueued;
    private @Nullable Record lastUnwritten;

    private long highestWrittenStreamId;

    private final Map<Outbox.Node, Long> writingStreams = new HashMap<>();

    private long nextTicket;

    private long nextStreamId;

    private long peerMaxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;

    private boolean closing;

    OwnRequestLedger(Outbox outbox, long firstStreamId, int ownParity) {
        this.outbox = outbox;
        this.nextStreamId = firstStreamId;
        this.ownParity = ownParity;
    }

    void setPeerMaxConcurrentStreams(long peerMaxConcurrentStreams) {
        this.peerMaxConcurrentStreams = peerMaxConcurrentStreams;
    }

    long peerMaxConcurrentStreams() {
        return peerMaxConcurrentStreams;
    }

    boolean isOwnSpace(long streamId) {
        return (streamId & 1) == ownParity;
    }

    long nextTicket() {
        return nextTicket;
    }

    long issueTicket() {
        return nextTicket++;
    }

    Admission open(int type, byte[] payload, @Nullable Long deadlineAt, long peerMaxFrameSize) {
        return open(type, NO_BODY, payload, deadlineAt, peerMaxFrameSize);
    }

    Admission open(int type, byte[] extensions, byte[] payload, @Nullable Long deadlineAt, long peerMaxFrameSize) {
        Record record = new Record(nextTicket, type, extensions, null, deadlineAt);
        Admission admission = number(record, payload, peerMaxFrameSize);
        if (admission != Admission.OVERSIZE) {
            nextTicket++;
        }
        if (admission == Admission.ADMITTED && record.expiry != null) {
            expirations.add(record.expiry);
        }
        return admission;
    }

    long queue(int type, byte[] payload, @Nullable Long deadlineAt) {
        return queue(type, NO_BODY, payload, deadlineAt);
    }

    long queue(int type, byte[] extensions, byte[] payload, @Nullable Long deadlineAt) {
        Record record = new Record(nextTicket++, type, extensions.clone(), payload.clone(), deadlineAt);
        queued.put(record.ticket, record);
        record.prevQueued = lastQueued;
        if (lastQueued == null) {
            firstQueued = record;
        } else {
            lastQueued.nextQueued = record;
        }
        lastQueued = record;
        if (record.expiry != null) {
            expirations.add(record.expiry);
        }
        return record.ticket;
    }

    Admission openQueued(long beforeTicket, long peerMaxFrameSize) {
        Record record = firstQueued;
        if (record == null || record.ticket >= beforeTicket) {
            return Admission.NONE;
        }
        byte[] payload = record.payload;
        if (payload == null) {
            throw new IllegalStateException("번호 없는 요청에 본문이 없다: 번호표 " + record.ticket);
        }
        unqueue(record);
        Admission admission = number(record, payload, peerMaxFrameSize);
        if (admission != Admission.ADMITTED) {
            if (record.expiry != null) {
                expirations.remove(record.expiry);
            }
        }
        return admission;
    }

    private Admission number(Record record, byte[] payload, long peerMaxFrameSize) {
        if (closing) {
            return Admission.CLOSING;
        }
        if (!Frames.isValidStreamId(nextStreamId)) {
            return Admission.EXHAUSTED;
        }
        if (byStream.size() >= peerMaxConcurrentStreams) {
            return Admission.STREAM_LIMIT;
        }
        long streamId = nextStreamId;
        byte[] frame = Frames.request(record.type, streamId, record.extensions, payload);
        if (frame.length > peerMaxFrameSize) {
            return Admission.OVERSIZE;
        }
        nextStreamId += 2;
        record.streamId = streamId;
        record.state = RequestState.NUMBERED_UNWRITTEN;
        record.payload = null;
        record.node = outbox.addApplication(OutboundMessage.of(frame));
        record.node.owner = record;
        record.linked = true;
        register(record);
        return Admission.ADMITTED;
    }

    void onWriteStarted(Outbox.Node node) {
        Record record = recordOf(node);
        if (record != null && record.linked && record.node == node) {
            record.node = null;
            unwrite(record);
            record.state = RequestState.WRITING_STARTED;
            record.node = node;
            writingStreams.put(node, record.streamId);
        }
    }

    void onWriteCompleted(Outbox.Node node) {
        Long streamId = writingStreams.remove(node);
        if (streamId != null) {
            highestWrittenStreamId = Math.max(highestWrittenStreamId, streamId);
        }
        Record record = recordOf(node);
        if (record != null && record.node == node && byStream.get(record.streamId) == record) {
            record.delivered = true;
            record.node = null;
        }
    }

    private static @Nullable Record recordOf(Outbox.Node node) {
        return node.owner instanceof Record ? (Record) node.owner : null;
    }

    long firstQueuedTicket() {
        return firstQueued == null ? -1 : firstQueued.ticket;
    }

    @Nullable
    RequestState stateOfTicket(long ticket) {

        if (queued.containsKey(ticket)) {
            return RequestState.QUEUED_WITHOUT_NUMBER;
        }
        Record record = byTicket.get(ticket);
        return record == null ? null : record.state;
    }

    @Nullable
    RequestState stateOfStream(long streamId) {
        Record record = byStream.get(streamId);
        return record == null ? null : record.state;
    }

    @Nullable
    Long streamIdOf(long ticket) {
        Record record = byTicket.get(ticket);
        return record == null ? null : record.streamId;
    }

    int queuedCount() {
        return queued.size();
    }

    int openCount() {
        return byStream.size();
    }

    long highestWrittenStreamId() {
        return highestWrittenStreamId;
    }

    Outbox.@Nullable Node lastUnwrittenNode() {
        return lastUnwritten == null ? null : lastUnwritten.node;
    }

    @Nullable
    Outcome cancelStream(long streamId, int status) {
        Record record = byStream.get(streamId);
        return record == null ? null : abort(record, status);
    }

    @Nullable
    Outcome cancelTicket(long ticket) {
        Record record = queued.get(ticket);
        if (record == null) {
            record = byTicket.get(ticket);
        }
        return record == null ? null : abort(record, Status.CANCELLED);
    }

    OptionalLong nextExpiryAt() {
        if (expirations.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(Deadlines.after(expirations.first().deadlineAt));
    }

    boolean expire(long now, Consumer<Outcome> sink) {
        if (expirations.isEmpty() || now <= expirations.first().deadlineAt) {
            return false;
        }
        List<Record> expired = new ArrayList<>();
        while (!expirations.isEmpty() && now > expirations.first().deadlineAt) {
            expired.add(expirations.pollFirst().record);
        }
        Collections.sort(expired, EXPIRY_ORDER);
        boolean endedNumbered = false;
        for (Record record : expired) {
            endedNumbered |= record.state != RequestState.QUEUED_WITHOUT_NUMBER;
            sink.accept(abort(record, Status.DEADLINE_EXCEEDED));
        }
        return endedNumbered;
    }

    @Nullable
    Outcome onResponse(long streamId, int responseType, int rawStatus, byte[] body) {
        Record record = byStream.get(streamId);
        if (record == null || !record.delivered) {
            return null;
        }
        boolean typeMatches = responseType == record.type;
        int status = typeMatches ? Status.normalize(rawStatus) : Event.RESPONSE_TYPE_MISMATCH;
        unregister(record);
        unwrite(record);
        return new Outcome(
                record.ticket, streamId, status, Certainty.RESPONSE, typeMatches ? body : NO_BODY, false, true);
    }

    @Nullable
    Outcome onReset(long streamId, long errorCode) {
        Record record = byStream.get(streamId);
        if (record == null || !record.delivered) {
            return null;
        }
        unregister(record);
        unwrite(record);
        Certainty certainty = errorCode == ErrorCode.REFUSED ? Certainty.NOT_PROCESSED : Certainty.UNKNOWN;
        return new Outcome(record.ticket, streamId, Status.forReset(errorCode), certainty, false, true);
    }

    List<Outcome> endAfter(long lastStreamId) {
        List<Record> ended = new ArrayList<>();
        for (Record record : byStream.values()) {
            if (record.streamId > lastStreamId) {
                ended.add(record);
            }
        }
        List<Outcome> outcomes = new ArrayList<>(ended.size());
        for (Record record : ended) {
            unregister(record);
            unwrite(record);
            outcomes.add(new Outcome(
                    record.ticket, record.streamId, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false, true));
        }
        return outcomes;
    }

    List<Outcome> onClosing() {
        closing = true;
        List<Outcome> outcomes = new ArrayList<>(queued.size());
        for (Record record = firstQueued; record != null; record = record.nextQueued) {
            outcomes.add(new Outcome(record.ticket, 0, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false, false));
            if (record.expiry != null) {
                expirations.remove(record.expiry);
            }
        }
        clearQueued();
        return outcomes;
    }

    List<Outcome> onTransportClosed(boolean transportBroken) {
        List<Outcome> outcomes = new ArrayList<>(byStream.size() + queued.size());
        for (Record record : byStream.values()) {
            boolean handedOff = record.delivered || (!transportBroken && record.state == RequestState.WRITING_STARTED);
            Certainty certainty = handedOff ? Certainty.UNKNOWN : Certainty.NOT_PROCESSED;
            unwrite(record);
            outcomes.add(new Outcome(record.ticket, record.streamId, Status.UNAVAILABLE, certainty, false, true));
        }
        for (Record record = firstQueued; record != null; record = record.nextQueued) {
            outcomes.add(new Outcome(record.ticket, 0, Status.UNAVAILABLE, endUnfinished(record), false, false));
        }
        byStream.clear();
        byTicket.clear();
        writingStreams.clear();
        clearQueued();
        expirations.clear();
        return outcomes;
    }

    private Outcome abort(Record record, int status) {
        unregister(record);
        Certainty certainty = endUnfinished(record);
        return new Outcome(
                record.ticket,
                record.streamId,
                status,
                certainty,
                certainty == Certainty.UNKNOWN,
                record.state != RequestState.QUEUED_WITHOUT_NUMBER);
    }

    private static Certainty unfinishedCertainty(Record record) {
        return record.state == RequestState.WRITING_STARTED ? Certainty.UNKNOWN : Certainty.NOT_PROCESSED;
    }

    private Certainty endUnfinished(Record record) {
        Certainty certainty = unfinishedCertainty(record);
        unwrite(record);
        return certainty;
    }

    private void register(Record record) {
        byStream.put(record.streamId, record);
        byTicket.put(record.ticket, record);
        record.prevUnwritten = lastUnwritten;
        if (lastUnwritten != null) {
            lastUnwritten.nextUnwritten = record;
        }
        lastUnwritten = record;
    }

    private void unqueue(Record record) {
        queued.remove(record.ticket);
        Record prev = record.prevQueued;
        Record next = record.nextQueued;
        if (prev == null) {
            firstQueued = next;
        } else {
            prev.nextQueued = next;
        }
        if (next == null) {
            lastQueued = prev;
        } else {
            next.prevQueued = prev;
        }
        record.prevQueued = null;
        record.nextQueued = null;
    }

    private void clearQueued() {
        queued.clear();
        firstQueued = null;
        lastQueued = null;
    }

    private void unregister(Record record) {
        if (record.state == RequestState.QUEUED_WITHOUT_NUMBER) {
            unqueue(record);
        } else {
            byStream.remove(record.streamId);
            byTicket.remove(record.ticket);
        }
        if (record.expiry != null) {
            expirations.remove(record.expiry);
        }
    }

    private void unwrite(Record record) {
        if (!record.linked) {
            return;
        }
        Outbox.Node node = record.node;
        if (node != null) {
            outbox.remove(node);
        }
        record.linked = false;
        Record prev = record.prevUnwritten;
        Record next = record.nextUnwritten;
        if (prev != null) {
            prev.nextUnwritten = next;
        }
        if (next == null) {
            lastUnwritten = prev;
        } else {
            next.prevUnwritten = prev;
        }
        record.node = null;
        record.prevUnwritten = null;
        record.nextUnwritten = null;
    }
}
