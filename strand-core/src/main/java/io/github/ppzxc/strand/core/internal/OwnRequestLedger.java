package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * 내 요청 장부. 번호를 받은 내 요청의 생애를 소유한다(번호 받음 → 쓰기 전 → 쓰기 시작 → 결과). {@link Endpoint}는 사건을 알리고, 장부가
 * 돌려주는 {@link Outcome}(확정성이 붙은 결과, 보낼 RESET)을 반영한다. 이벤트 객체를 만들거나 이벤트 목록을 만지지 않는다.
 *
 * <p>요청 하나는 {@link Record} 하나이고 상태({@link Endpoint.RequestState#NUMBERED_UNWRITTEN}, {@link
 * Endpoint.RequestState#WRITING_STARTED})는 기록의 값이다. 스트림→요청 색인, 번호표→요청 색인, 기한 색인, 쓰기 전 요청의 연결 리스트는
 * {@link #register}와 {@link #unregister}에서만 함께 고치므로 어긋날 수 없다. 끝난 요청은 어떤 색인에도 남지 않는다.
 *
 * <p>결과 확정성 판정(규격 3.5절, 4.4절, 4.4.1절)은 이 클래스에만 있다. 응답이나 RESET 없이 끝나는 경우(취소, 기한, 전송 계층 닫힘)는
 * {@link #unfinishedCertainty} 한 자리가 정한다: 쓰기 전이면 처리 안 됨, 쓰기를 시작한 뒤면 결과 모름이다. 나머지 사건은 사건 자체가 확정성을
 * 정한다: 응답은 {@link Certainty#RESPONSE}({@link #onResponse}), 거절(REFUSED) RESET은 쓰기를 시작했어도 처리 안 됨, 그 밖의 RESET은 결과 모름
 * ({@link #onReset}), CLOSE {@code last_stream_id} 뒤의 요청은 처리 안 됨({@link #endAfter})이다.
 *
 * <p>번호표 조회와 기한 색인의 비용 특성은 #202를 따른다. 번호표 조회는 O(1)이고 {@link #expire}는 만료된 요청 수에 비례해서만 일한다.
 */
final class OwnRequestLedger {

    /** 요청 하나가 끝나며 장부가 {@link Endpoint}에 돌려주는 결과. */
    static final class Outcome {
        final long streamId;
        final int status;
        final Certainty certainty;
        /** 쓰기를 시작한 요청을 끝낸 것이라 {@code RESET(CANCEL)}을 보내야 한다. */
        final boolean resetToSend;

        Outcome(long streamId, int status, Certainty certainty, boolean resetToSend) {
            this.streamId = streamId;
            this.status = status;
            this.certainty = certainty;
            this.resetToSend = resetToSend;
        }
    }

    /** 번호를 받은 내 요청 하나의 기록. */
    private static final class Record {
        final long streamId;
        final long ticket;
        final int type;
        /** 요청 기한(가상 시계의 절대 시각). 기한이 없으면 null. */
        final @Nullable Long deadlineAt;

        Endpoint.RequestState state = Endpoint.RequestState.NUMBERED_UNWRITTEN;
        /** 쓰기 대기열의 노드. 쓰기 전일 때만 있다. */
        Outbox.@Nullable Node node;
        /** 쓰기 전 요청의 연결 리스트(넣은 순서). 쓰기 전일 때만 이어져 있다. */
        @Nullable
        Record prevUnwritten;

        @Nullable
        Record nextUnwritten;

        Record(long streamId, long ticket, int type, @Nullable Long deadlineAt) {
            this.streamId = streamId;
            this.ticket = ticket;
            this.type = type;
            this.deadlineAt = deadlineAt;
        }
    }

    /** 기한 색인 항목. 기한 순이고 같은 기한이면 스트림 번호 순이다. */
    private static final class Expiry implements Comparable<Expiry> {
        final long deadlineAt;
        final long streamId;

        Expiry(long deadlineAt, long streamId) {
            this.deadlineAt = deadlineAt;
            this.streamId = streamId;
        }

        @Override
        public int compareTo(Expiry other) {
            int byDeadline = Long.compare(deadlineAt, other.deadlineAt);
            return byDeadline != 0 ? byDeadline : Long.compare(streamId, other.streamId);
        }
    }

    private final Outbox outbox;
    /** 스트림 번호 → 요청. 보낸 순서대로다(규격 1.3절). */
    private final Map<Long, Record> byStream = new LinkedHashMap<>();
    /** 번호표 → 요청. */
    private final Map<Long, Record> byTicket = new HashMap<>();
    /** 기한이 있는 요청의 기한 순 색인. 가장 이른 기한만 보므로 {@link #expire}는 만료된 요청 수에 비례한다. */
    private final TreeSet<Expiry> expirations = new TreeSet<>();

    private @Nullable Record firstUnwritten;
    private @Nullable Record lastUnwritten;
    /** 요청 프레임을 전송 계층에 넘긴 내 스트림 가운데 가장 큰 번호. 상대 CLOSE의 {@code last_stream_id} 검사 기준이다(규격 3.5절). */
    private long highestWrittenStreamId;

    OwnRequestLedger(Outbox outbox) {
        this.outbox = outbox;
    }

    /**
     * 요청 접수. 번호를 받은 요청 하나를 기록하고 요청 프레임을 쓰기 대기열에 넣는다. 호출자는 관문과 한도를 이미 통과시켰다.
     *
     * @param frame 요청 프레임
     * @param deadlineAt 기한(가상 시계의 절대 시각). 없으면 null
     */
    void accept(long streamId, long ticket, int type, byte[] frame, @Nullable Long deadlineAt) {
        Record record = new Record(streamId, ticket, type, deadlineAt);
        record.node = outbox.addApplication(OutboundMessage.binary(frame));
        register(record);
    }

    /**
     * 쓰기 시작 사건. 쓰기 대기열의 메시지를 전송 계층에 넘겼다. 쓰기 전이던 요청은 모두 쓰기 시작 상태가 되고 넘긴 가장 큰 번호가 갱신된다.
     * 넘긴 가장 큰 번호를 바꾸는 유일한 자리다.
     */
    void onWriteStarted() {
        for (Record record = firstUnwritten; record != null; ) {
            Record next = record.nextUnwritten;
            highestWrittenStreamId = Math.max(highestWrittenStreamId, record.streamId);
            record.state = Endpoint.RequestState.WRITING_STARTED;
            record.node = null;
            record.prevUnwritten = null;
            record.nextUnwritten = null;
            record = next;
        }
        firstUnwritten = null;
        lastUnwritten = null;
    }

    /** 번호표로 요청의 상태를 본다. 끝났거나 번호를 받지 못했거나 낸 적 없으면 null이다. */
    Endpoint.@Nullable RequestState stateOfTicket(long ticket) {
        Record record = byTicket.get(ticket);
        return record == null ? null : record.state;
    }

    Endpoint.@Nullable RequestState stateOfStream(long streamId) {
        Record record = byStream.get(streamId);
        return record == null ? null : record.state;
    }

    /** 번호표가 받은 스트림 번호. 끝났거나 번호를 받지 못했거나 낸 적 없으면 null이다. */
    @Nullable
    Long streamIdOf(long ticket) {
        Record record = byTicket.get(ticket);
        return record == null ? null : record.streamId;
    }

    /** 번호를 받아 아직 끝나지 않은 내 요청의 수. */
    int openCount() {
        return byStream.size();
    }

    long highestWrittenStreamId() {
        return highestWrittenStreamId;
    }

    /** CLOSE를 둘 자리: 쓰기 전 요청 가운데 마지막 것의 대기열 노드(규격 3.5절). 쓰기 전 요청이 없으면 null이다. */
    Outbox.@Nullable Node lastUnwrittenNode() {
        return lastUnwritten == null ? null : lastUnwritten.node;
    }

    /** 스트림 번호로 취소한다(규격 4.4절). 대기 중이 아니면 null이다. */
    @Nullable
    Outcome cancelStream(long streamId, int status) {
        Record record = byStream.get(streamId);
        return record == null ? null : abort(record, status);
    }

    /** 번호표로 취소한다(규격 4.4절). 번호를 받은 대기 중 요청이 아니면 null이다. */
    @Nullable
    Outcome cancelTicket(long ticket) {
        Record record = byTicket.get(ticket);
        return record == null ? null : abort(record, Status.CANCELLED);
    }

    /**
     * 시계 진행 사건. 기한이 지난 요청을 끝낸다(규격 4.4.1절). 끝내는 순서는 기한 순이 아니라 스트림 번호 순이다. 결과는 요청 하나를 끝낼
     * 때마다 곧바로 {@code sink}에 넘긴다. 쓰기를 시작한 요청의 {@code RESET}을 보내는 일과 쓰기 전 요청의 프레임을 거두는 일이 요청 순서대로
     * 엇갈려야 쓰기 대기열의 송신 순서가 바뀌지 않기 때문이다.
     *
     * @return 끝낸 요청이 있으면 true
     */
    boolean expire(long now, Consumer<Outcome> sink) {
        if (expirations.isEmpty() || now <= expirations.first().deadlineAt) {
            return false;
        }
        List<Long> expired = new ArrayList<>();
        while (!expirations.isEmpty() && now > expirations.first().deadlineAt) {
            expired.add(expirations.pollFirst().streamId);
        }
        Collections.sort(expired);
        for (long streamId : expired) {
            Record record = byStream.get(streamId);
            if (record != null) {
                sink.accept(abort(record, Status.DEADLINE_EXCEEDED));
            }
        }
        return true;
    }

    /**
     * 응답 도착 사건. 응답 type이 요청과 다르면 status와 payload를 버리고 로컬 INTERNAL로 끝낸다(규격 4.1절). 대기 중이 아니면 null이다.
     * 상대가 아직 쓰지 않은 내 요청에 답한 경우(상대의 규격 위반)에도 쓰기 대기열에서 프레임을 거둔다.
     */
    @Nullable
    Outcome onResponse(long streamId, int responseType, int rawStatus) {
        Record record = byStream.get(streamId);
        if (record == null) {
            return null;
        }
        int status = responseType == record.type ? Status.normalize(rawStatus) : Event.RESPONSE_TYPE_MISMATCH;
        unregister(record);
        unwrite(record);
        return new Outcome(streamId, status, Certainty.RESPONSE, false);
    }

    /**
     * RESET 도착 사건(규격 4.4절 결과 표). 거절(REFUSED)한 요청은 쓰기를 시작했어도 처리를 시작하지 않았다. 대기 중이 아니면 null이다.
     */
    @Nullable
    Outcome onReset(long streamId, long errorCode) {
        Record record = byStream.get(streamId);
        if (record == null) {
            return null;
        }
        unregister(record);
        unwrite(record);
        Certainty certainty = errorCode == ErrorCode.REFUSED ? Certainty.NOT_PROCESSED : Certainty.UNKNOWN;
        return new Outcome(streamId, Status.forReset(errorCode), certainty, false);
    }

    /**
     * CLOSE {@code last_stream_id} 사건. 그보다 큰 번호의 요청은 상대가 처리하지 않았으므로 아직 넘기지 못한 프레임도 보내지 않고 처리 안
     * 됨으로 끝낸다(규격 3.5절).
     */
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
            outcomes.add(new Outcome(record.streamId, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false));
        }
        return outcomes;
    }

    /**
     * 전송 계층 닫힘 사건. 대기 중인 요청을 모두 끝내고 장부를 비운다. 요청의 마지막 바이트를 전송 계층에 넘겼는지로 정한다(규격 3.5절).
     * 못 넘긴 프레임은 이후에도 넘기지 않도록 거둔다.
     */
    List<Outcome> onTransportClosed() {
        List<Outcome> outcomes = new ArrayList<>(byStream.size());
        for (Record record : byStream.values()) {
            outcomes.add(new Outcome(record.streamId, Status.UNAVAILABLE, unfinishedCertainty(record), false));
        }
        byStream.clear();
        byTicket.clear();
        expirations.clear();
        return outcomes;
    }

    /**
     * 취소나 기한으로 끝낸다(규격 4.4절, 4.4.1절). 한 바이트도 쓰기 전이면 프레임을 거두고 {@code RESET} 없이 처리 안 됨으로 끝낸다.
     * 쓰기를 시작했으면 프레임은 이미 끊을 수 없으므로 그대로 두고 {@code RESET(CANCEL)}을 보내 결과 모름으로 끝낸다.
     */
    private Outcome abort(Record record, int status) {
        unregister(record);
        Certainty certainty = unfinishedCertainty(record);
        return new Outcome(record.streamId, status, certainty, certainty == Certainty.UNKNOWN);
    }

    /**
     * 응답이나 RESET을 받지 못한 채 끝나는 요청(취소, 기한, 전송 계층 닫힘)의 확정성을 정하는 유일한 자리(규격 3.5절, 4.4절, 4.4.1절). 쓰기 전이면 프레임을 거두고 처리
     * 안 됨, 쓰기를 시작했으면 프레임을 이미 끊을 수 없어 결과 모름이다.
     */
    private Certainty unfinishedCertainty(Record record) {
        return unwrite(record) ? Certainty.NOT_PROCESSED : Certainty.UNKNOWN;
    }

    /** 요청을 모든 색인에 함께 넣는다. 쓰기 전 요청의 연결 리스트 끝에도 잇는다. */
    private void register(Record record) {
        byStream.put(record.streamId, record);
        byTicket.put(record.ticket, record);
        if (record.deadlineAt != null) {
            expirations.add(new Expiry(record.deadlineAt, record.streamId));
        }
        record.prevUnwritten = lastUnwritten;
        if (lastUnwritten == null) {
            firstUnwritten = record;
        } else {
            lastUnwritten.nextUnwritten = record;
        }
        lastUnwritten = record;
    }

    /** 요청을 스트림·번호표·기한 색인에서 함께 뺀다. 쓰기 전 연결 리스트와 대기열 노드는 {@link #unwrite}가 맡는다. */
    private void unregister(Record record) {
        byStream.remove(record.streamId);
        byTicket.remove(record.ticket);
        if (record.deadlineAt != null) {
            expirations.remove(new Expiry(record.deadlineAt, record.streamId));
        }
    }

    /**
     * 아직 쓰기 전인 요청의 프레임을 쓰기 대기열에서 거두고 쓰기 전 연결 리스트에서 뺀다.
     *
     * @return 쓰기 전이어서 거뒀으면 true, 이미 쓰기를 시작했으면 false
     */
    private boolean unwrite(Record record) {
        if (record.state != Endpoint.RequestState.NUMBERED_UNWRITTEN) {
            return false;
        }
        Outbox.Node node = record.node;
        if (node != null) {
            outbox.remove(node);
        }
        Record prev = record.prevUnwritten;
        Record next = record.nextUnwritten;
        if (prev == null) {
            firstUnwritten = next;
        } else {
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
        return true;
    }
}
