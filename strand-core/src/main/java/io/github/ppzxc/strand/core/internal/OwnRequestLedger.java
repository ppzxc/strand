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

/**
 * 내 요청 장부. 내 요청 하나의 생애 전부를 소유한다(번호표 발급 → 번호 없음 → 번호 받음 → 쓰기 전 → 쓰기 시작 → 결과). {@link Endpoint}는
 * 사건을 알리고, 장부가 돌려주는 {@link Outcome}(확정성이 붙은 결과, 보낼 RESET)과 {@link Admission}(번호를 줄 수 있는지)을 반영한다. 이벤트
 * 객체를 만들거나 이벤트 목록을 만지지 않는다.
 *
 * <p>요청 하나는 {@link Record} 하나이고 상태({@link RequestState})는 기록의 값이다. 스트림→요청 색인, 번호표→요청 색인, 번호 없는 요청의
 * 대기열, 기한 색인, 쓰기 전 요청의 연결 리스트는 {@link #register}, {@link #queue}, {@link #unregister}에서만 함께 고치므로 어긋날 수
 * 없다. 끝난 요청은 어떤 색인에도 남지 않는다. 기한 색인은 하나이고 번호를 받아도 항목이 그대로라 다시 넣지 않는다.
 *
 * <p>스트림 번호와 번호표의 발급, 내 번호 공간의 홀짝, 번호를 줄 수 없는 경우(CLOSE 뒤, 번호 소진, 상대 {@code max_concurrent_streams} 도달,
 * 규격 2.2.5절, 3.2.1절, 3.5절)의 판정도 여기에 있다.
 *
 * <p>결과 확정성 판정(규격 3.5절, 4.4절, 4.4.1절)은 이 클래스에만 있다. 응답이나 RESET 없이 끝나는 경우(취소, 기한, 전송 계층 닫힘)는
 * {@link #unfinishedCertainty} 한 자리가 정한다: 쓰기를 시작했으면 결과 모름, 아니면(번호 없음, 쓰기 전) 처리 안 됨이다. 나머지 사건은 사건
 * 자체가 확정성을 정한다: 응답은 {@link Certainty#RESPONSE}({@link #onResponse}), 거절(REFUSED) RESET은 쓰기를 시작했어도 처리 안 됨, 그 밖의
 * RESET은 결과 모름({@link #onReset}), CLOSE를 보내거나 받아 끝나는 번호 없는 요청과 CLOSE {@code last_stream_id} 뒤의 요청은 처리 안 됨({@link
 * #onClosing}, {@link #endAfter})이다.
 *
 * <p>번호표 조회와 기한 색인의 비용 특성은 #202를 따른다. 번호표 조회는 O(1)이고 {@link #expire}는 만료된 요청 수에 비례해서만 일한다.
 */
final class OwnRequestLedger {

    /**
     * 요청이 쓰기 대기열 어디에 있는지(규격 1.3절, 4.4.1절). 입출력 없는(sans-IO) 모델에서는 {@link Endpoint#drainMessages()}가 프레임을 돌려준
     * 때를 쓰기를 시작한 때로 본다. 돌려준 프레임은 전송 계층이 이미 받았으므로 끊거나 거둘 수 없다.
     */
    enum RequestState {
        /** 번호를 받지 못한 요청. 프레임이 아직 없다. */
        QUEUED_WITHOUT_NUMBER,
        /** 번호를 받았지만 프레임이 아직 {@code drainMessages()}로 나가지 않았다. 쓰기 전이다. */
        NUMBERED_UNWRITTEN,
        /** 프레임이 {@code drainMessages()}로 나갔다. 쓰기를 시작했고 끝까지 쓴다. */
        WRITING_STARTED
    }

    /** 번호를 줄 수 있는지에 대한 장부의 답. */
    enum Admission {
        /** 번호를 받아 쓰기 대기열에 들어갔다. */
        ADMITTED,
        /** 번호를 받으려는 번호 없는 요청이 더 없다({@link #openQueued}). */
        NONE,
        /** CLOSE를 보냈거나 받았다(규격 3.5절). 번호표만 쓴다. */
        CLOSING,
        /** 스트림 번호가 소진됐다(규격 2.2.5절). 번호표만 쓴다. 호출자가 연결을 닫는다. */
        EXHAUSTED,
        /** 상대가 알린 {@code max_concurrent_streams}에 닿았다(규격 3.2.1절). 번호표만 쓴다. */
        STREAM_LIMIT,
        /** 프레임이 상대의 {@code max_frame_size}를 넘는다(규격 3.2.1절). 번호표도 스트림 번호도 쓰지 않는다. */
        OVERSIZE
    }

    /** 요청 하나가 끝나며 장부가 {@link Endpoint}에 돌려주는 결과. 번호를 받지 못한 요청이면 {@code streamId}는 0이다. */
    static final class Outcome {
        final long streamId;
        final int status;
        final Certainty certainty;
        /** 쓰기를 시작한 요청을 끝낸 것이라 {@code RESET(CANCEL)}을 보내야 한다. */
        final boolean resetToSend;
        /** 번호를 받은 요청의 결과인가. 번호 없는 요청의 결과는 스트림 번호가 없다(0). */
        private final boolean numbered;

        Outcome(long streamId, int status, Certainty certainty, boolean resetToSend, boolean numbered) {
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

    /** 내 요청 하나의 기록. */
    private static final class Record {
        final long ticket;
        final int type;
        /** 번호를 받기 전의 본문. 번호를 받으면 프레임이 되므로 비운다. */
        byte @Nullable [] payload;
        /** 기한 색인 항목. 기한이 없으면 null. */
        final @Nullable Expiry expiry;

        /** 번호를 받기 전에는 0이다. */
        long streamId;

        RequestState state = RequestState.QUEUED_WITHOUT_NUMBER;
        /** 쓰기 대기열의 노드. 쓰기 전일 때만 있다. */
        Outbox.@Nullable Node node;
        /** 쓰기 전 요청의 연결 리스트(넣은 순서). 쓰기 전일 때만 이어져 있다. */
        @Nullable
        Record prevUnwritten;

        @Nullable
        Record nextUnwritten;

        /** 쓰기 대기열 노드와 쓰기 전 연결 리스트에 이어져 있다. {@link #unwrite}를 두 번 불러도 한 번만 거두게 한다. */
        boolean linked;

        /** 번호 없는 요청의 연결 리스트(낸 순서). 번호가 없을 때만 이어져 있다. */
        @Nullable
        Record prevQueued;

        @Nullable
        Record nextQueued;

        Record(long ticket, int type, byte @Nullable [] payload, @Nullable Long deadlineAt) {
            this.ticket = ticket;
            this.type = type;
            this.payload = payload;
            this.expiry = deadlineAt == null ? null : new Expiry(deadlineAt, ticket, this);
        }
    }

    /** 기한 색인 항목. 기한 순이고 같은 기한이면 번호표 순이다. */
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

    /** 만료 순서: 번호 없는 요청이 먼저(번호표 순), 번호를 받은 요청이 뒤(스트림 번호 순). */
    private static final Comparator<Record> EXPIRY_ORDER = (a, b) -> {
        boolean aQueued = a.state == RequestState.QUEUED_WITHOUT_NUMBER;
        if (aQueued != (b.state == RequestState.QUEUED_WITHOUT_NUMBER)) {
            return aQueued ? -1 : 1;
        }
        return aQueued ? Long.compare(a.ticket, b.ticket) : Long.compare(a.streamId, b.streamId);
    };

    private final Outbox outbox;
    /** 번호를 받은 요청의 스트림 번호 → 요청. 보낸 순서대로다(규격 1.3절). */
    private final Map<Long, Record> byStream = new LinkedHashMap<>();
    /** 번호표 → 번호를 받은 요청. 번호 없는 요청은 {@link #queued}에만 있다. 요청 하나는 둘 가운데 하나에만 있다. */
    private final Map<Long, Record> byTicket = new HashMap<>();
    /** 번호를 받지 못한 요청. 낸 순서(번호표 순서)대로다. */
    private final Map<Long, Record> queued = new HashMap<>();
    /** 기한이 있는 요청의 기한 순 색인. 가장 이른 기한만 보므로 {@link #expire}는 만료된 요청 수에 비례한다. */
    private final TreeSet<Expiry> expirations = new TreeSet<>();
    /** 내 번호 공간의 홀짝: 클라이언트는 1, 서버는 0이다(규격 2.2.5절). */
    private final int ownParity;

    private @Nullable Record firstQueued;
    private @Nullable Record lastQueued;
    private @Nullable Record firstUnwritten;
    private @Nullable Record lastUnwritten;
    /** 요청 프레임을 전송 계층에 넘긴 내 스트림 가운데 가장 큰 번호. 상대 CLOSE의 {@code last_stream_id} 검사 기준이다(규격 3.5절). */
    private long highestWrittenStreamId;
    /** 다음 요청에 줄 번호표(낸 순서, 0부터). */
    private long nextTicket;
    /** 다음 요청에 줄 스트림 번호. 2^63-1을 넘으면 음수로 넘쳐 소진으로 읽힌다. */
    private long nextStreamId;

    private long peerMaxConcurrentStreams = HelloParams.DEFAULT_MAX_CONCURRENT_STREAMS;
    /** CLOSE를 보냈거나 받았다(규격 3.5절). 새 요청에 번호를 주지 않는다. */
    private boolean closing;

    /**
     * @param firstStreamId 첫 스트림 번호. 호출자가 {@code ownParity}의 번호 공간인지 이미 확인했다
     * @param ownParity 내 번호 공간의 홀짝(클라이언트 1, 서버 0)
     */
    OwnRequestLedger(Outbox outbox, long firstStreamId, int ownParity) {
        this.outbox = outbox;
        this.nextStreamId = firstStreamId;
        this.ownParity = ownParity;
    }

    /** 상대 HELLO가 알린 {@code max_concurrent_streams}. 알리지 않았으면 기본값이다(규격 3.2.1절). */
    void setPeerMaxConcurrentStreams(long peerMaxConcurrentStreams) {
        this.peerMaxConcurrentStreams = peerMaxConcurrentStreams;
    }

    /** 스트림 번호가 내 번호 공간에 속하는지(규격 2.2.5절). */
    boolean isOwnSpace(long streamId) {
        return (streamId & 1) == ownParity;
    }

    /** 다음 요청에 줄 번호표. 받지 않는다. */
    long nextTicket() {
        return nextTicket;
    }

    /** 요청을 만들지 않고 번호표 하나를 쓴다. 전송 계층이 닫힌 뒤 로컬에서 실패시킨 요청의 번호표다. */
    long issueTicket() {
        return nextTicket++;
    }

    /**
     * AUTH 관문이 열린 뒤의 요청. 번호를 줄 수 있으면 번호를 주고 요청 프레임을 쓰기 대기열에 넣는다. 번호표는 {@link Admission#OVERSIZE}가 아니면 쓴다.
     * 판정 순서는 CLOSE 뒤, 번호 소진, 한도, 프레임 크기다. 번호표는 호출 전의 {@link #nextTicket()}이다.
     *
     * @param deadlineAt 기한(가상 시계의 절대 시각). 없으면 null
     * @param peerMaxFrameSize 상대가 알린 {@code max_frame_size}
     */
    Admission open(int type, byte[] payload, @Nullable Long deadlineAt, long peerMaxFrameSize) {
        Record record = new Record(nextTicket, type, null, deadlineAt);
        Admission admission = number(record, payload, peerMaxFrameSize);
        if (admission != Admission.OVERSIZE) {
            nextTicket++;
        }
        if (admission == Admission.ADMITTED && record.expiry != null) {
            expirations.add(record.expiry);
        }
        return admission;
    }

    /**
     * AUTH 관문이 열리기 전의 요청. 번호 없는 요청으로 대기열에 두고 번호표를 쓴다. 본문은 복사해 둔다.
     *
     * @param deadlineAt 기한(가상 시계의 절대 시각). 없으면 null
     * @return 번호표
     */
    long queue(int type, byte[] payload, @Nullable Long deadlineAt) {
        Record record = new Record(nextTicket++, type, payload.clone(), deadlineAt);
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

    /**
     * 번호 없는 요청 가운데 가장 먼저 낸 것에 번호를 준다. 번호표가 {@code beforeTicket}보다 작은 것만 준다. 번호를 줄 수 없으면(
     * {@link Admission#ADMITTED}가 아니면) 그 요청은 장부에서 지워진다. 호출자는 로컬 실패를 이벤트로 낸다. 한 번에 하나만 처리하는 것은
     * 호출자가 단계마다 연결 상태를 다시 보기 위해서다(번호를 주다가 닫히면 남은 번호 없는 요청이 끝난다).
     *
     * @return 번호 없는 요청이 없거나 번호표 한도에 닿으면 {@link Admission#NONE}
     */
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

    /** 번호를 줄 수 있는지 판정하고, 줄 수 있으면 번호를 주고 프레임을 쓰기 대기열에 넣는다. */
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
        byte[] frame = Frames.request(record.type, streamId, payload);
        if (frame.length > peerMaxFrameSize) {
            return Admission.OVERSIZE;
        }
        nextStreamId += 2;
        record.streamId = streamId;
        record.state = RequestState.NUMBERED_UNWRITTEN;
        record.payload = null;
        record.node = outbox.addApplication(OutboundMessage.binary(frame));
        record.linked = true;
        register(record);
        return Admission.ADMITTED;
    }

    /**
     * 쓰기 시작 사건. 쓰기 대기열의 메시지를 전송 계층에 넘겼다. 쓰기 전이던 요청은 모두 쓰기 시작 상태가 되고 넘긴 가장 큰 번호가 갱신된다.
     * 넘긴 가장 큰 번호를 바꾸는 유일한 자리다.
     */
    void onWriteStarted() {
        for (Record record = firstUnwritten; record != null; ) {
            Record next = record.nextUnwritten;
            highestWrittenStreamId = Math.max(highestWrittenStreamId, record.streamId);
            record.state = RequestState.WRITING_STARTED;
            record.linked = false;
            record.node = null;
            record.prevUnwritten = null;
            record.nextUnwritten = null;
            record = next;
        }
        firstUnwritten = null;
        lastUnwritten = null;
    }

    /** 번호표로 요청의 상태를 본다. 번호 없는 요청도 찾는다. 끝났거나 번호표를 받고도 요청이 되지 못했거나 낸 적 없으면 null이다. */
    @Nullable
    RequestState stateOfTicket(long ticket) {
        // 번호 없는 요청은 값 객체를 읽지 않고 색인에 있는지만 본다(큰 대기열에서 조회마다 객체를 따라가는 비용을 피한다).
        if (queued.containsKey(ticket)) {
            return RequestState.QUEUED_WITHOUT_NUMBER;
        }
        Record record = byTicket.get(ticket);
        return record == null ? null : record.state;
    }

    /** 스트림 번호로 요청의 상태를 본다. 테스트 전용 package-private 엿보기이고 {@link Endpoint}는 쓰지 않는다. 대기 중이 아니면 null이다. */
    @Nullable
    RequestState stateOfStream(long streamId) {
        Record record = byStream.get(streamId);
        return record == null ? null : record.state;
    }

    /** 번호표가 받은 스트림 번호. 테스트 전용 package-private 엿보기이고 {@link Endpoint}는 쓰지 않는다. 끝났거나 번호를 받지 못했거나 낸 적 없으면 null이다. */
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

    /** 번호표로 취소한다(규격 4.4절). 번호 없는 요청도 취소한다. 대기 중이 아니면 null이다. */
    @Nullable
    Outcome cancelTicket(long ticket) {
        Record record = queued.get(ticket);
        if (record == null) {
            record = byTicket.get(ticket);
        }
        return record == null ? null : abort(record, Status.CANCELLED);
    }

    /**
     * {@link #expire}가 처음으로 요청을 끝내는 가장 이른 시각(기한 다음 밀리초). 기한 있는 요청이 없으면 비어 있다. 기한 색인의 첫 원소만 본다.
     */
    OptionalLong nextExpiryAt() {
        if (expirations.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(Deadlines.after(expirations.first().deadlineAt));
    }

    /**
     * 시계 진행 사건. 기한이 지난 요청을 끝낸다(규격 4.4.1절). 끝내는 순서는 기한 순이 아니라 번호 없는 요청이 먼저(낸 순서), 번호를 받은
     * 요청이 뒤(스트림 번호 순)다. 결과는 요청 하나를 끝낼 때마다 곧바로 {@code sink}에 넘긴다. 쓰기를 시작한 요청의 {@code RESET}을 보내는
     * 일과 쓰기 전 요청의 프레임을 거두는 일이 요청 순서대로 엇갈려야 쓰기 대기열의 송신 순서가 바뀌지 않기 때문이다.
     *
     * @return 번호를 받은 요청을 끝냈으면 true
     */
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
        return new Outcome(streamId, status, Certainty.RESPONSE, false, true);
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
        return new Outcome(streamId, Status.forReset(errorCode), certainty, false, true);
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
            outcomes.add(new Outcome(record.streamId, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false, true));
        }
        return outcomes;
    }

    /**
     * CLOSE를 보내거나 받는 사건. 이후 새 요청에 번호를 주지 않고, 번호를 받지 못하고 기다리던 요청은 모두 처리 안 됨으로 끝낸다(규격
     * 3.5절). 번호를 받은 요청은 건드리지 않는다. 낸 순서대로 돌려준다.
     */
    List<Outcome> onClosing() {
        closing = true;
        List<Outcome> outcomes = new ArrayList<>(queued.size());
        for (Record record = firstQueued; record != null; record = record.nextQueued) {
            outcomes.add(new Outcome(0, Status.UNAVAILABLE, Certainty.NOT_PROCESSED, false, false));
            if (record.expiry != null) {
                expirations.remove(record.expiry);
            }
        }
        clearQueued();
        return outcomes;
    }

    /**
     * 전송 계층 닫힘 사건. 대기 중인 요청을 모두 끝내고 장부를 비운다. 번호를 받은 요청(스트림 번호 순)이 먼저고 번호 없는 요청(낸 순서)이 뒤다.
     * 요청의 마지막 바이트를 전송 계층에 넘겼는지로 정한다(규격 3.5절). 못 넘긴 프레임은 이후에도 넘기지 않도록 거둔다.
     */
    List<Outcome> onTransportClosed() {
        List<Outcome> outcomes = new ArrayList<>(byStream.size() + queued.size());
        for (Record record : byStream.values()) {
            outcomes.add(new Outcome(record.streamId, Status.UNAVAILABLE, endUnfinished(record), false, true));
        }
        for (Record record = firstQueued; record != null; record = record.nextQueued) {
            outcomes.add(new Outcome(0, Status.UNAVAILABLE, endUnfinished(record), false, false));
        }
        byStream.clear();
        byTicket.clear();
        clearQueued();
        expirations.clear();
        return outcomes;
    }

    /**
     * 취소나 기한으로 끝낸다(규격 4.4절, 4.4.1절). 한 바이트도 쓰기 전이면 프레임을 거두고 {@code RESET} 없이 처리 안 됨으로 끝낸다.
     * 쓰기를 시작했으면 프레임은 이미 끊을 수 없으므로 그대로 두고 {@code RESET(CANCEL)}을 보내 결과 모름으로 끝낸다.
     */
    private Outcome abort(Record record, int status) {
        unregister(record);
        Certainty certainty = endUnfinished(record);
        return new Outcome(
                record.streamId,
                status,
                certainty,
                certainty == Certainty.UNKNOWN,
                record.state != RequestState.QUEUED_WITHOUT_NUMBER);
    }

    /**
     * 응답이나 RESET을 받지 못한 채 끝나는 요청(취소, 기한, 전송 계층 닫힘)의 확정성을 정하는 유일한 자리(규격 3.5절, 4.4절, 4.4.1절). 쓰기를
     * 시작했으면 프레임을 이미 끊을 수 없어 결과 모름이다. 번호가 없거나 쓰기 전이면 처리 안 됨이다. 요청의 상태만 보고 아무것도 바꾸지 않는다.
     */
    private static Certainty unfinishedCertainty(Record record) {
        return record.state == RequestState.WRITING_STARTED ? Certainty.UNKNOWN : Certainty.NOT_PROCESSED;
    }

    /**
     * 응답이나 RESET 없이 끝나는 요청을 거두고 확정성을 돌려준다. 쓰기 전이면 프레임을 쓰기 대기열에서 거둔다(번호가 없으면 프레임이 없다). 확정성은
     * {@link #unfinishedCertainty}가 정한다.
     */
    private Certainty endUnfinished(Record record) {
        Certainty certainty = unfinishedCertainty(record);
        unwrite(record);
        return certainty;
    }

    /** 번호를 받은 요청을 색인에 함께 넣는다. 쓰기 전 요청의 연결 리스트 끝에도 잇는다. 기한 색인은 건드리지 않는다. 번호 없는 요청은 {@link #queue}가, 바로 번호를 받는 요청은 {@link #open}이 넣는다. */
    private void register(Record record) {
        byStream.put(record.streamId, record);
        byTicket.put(record.ticket, record);
        record.prevUnwritten = lastUnwritten;
        if (lastUnwritten == null) {
            firstUnwritten = record;
        } else {
            lastUnwritten.nextUnwritten = record;
        }
        lastUnwritten = record;
    }

    /** 번호 없는 요청을 대기열(색인과 연결 리스트)에서 뺀다. */
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

    /** 요청을 스트림·번호 없는 요청 대기열·번호표·기한 색인에서 함께 뺀다. 쓰기 전 연결 리스트와 대기열 노드는 {@link #unwrite}가 맡는다. */
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

    /**
     * 아직 쓰기 전인 요청의 프레임을 쓰기 대기열에서 거두고 쓰기 전 연결 리스트에서 뺀다. 이미 거뒀거나 쓰기를 시작했거나 번호가 없으면 아무것도
     * 하지 않는다(두 번 불러도 안전하다).
     */
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
    }
}
