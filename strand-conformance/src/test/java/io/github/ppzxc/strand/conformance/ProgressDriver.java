package io.github.ppzxc.strand.conformance;

import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * 적합성 러너가 엔드포인트를 진행 보고 방식으로 구동하는 방법: 한도 있는 입력, 나노초 시계, 다음 출력과 진행 보고, 한도 있는 사건 수거,
 * 위임된 인증 판단. 벡터의 "넘김"은 출력 진행 보고로 표현한다. 전부 넘기면 출력 전체를 보고하고, 부분 쓰기 한도가 있으면 그 바이트까지만 보고해
 * 쓰기를 시작했지만 넘기지 못한 출력을 남긴다.
 */
final class ProgressDriver {

    /** 사건을 한 번에 수거하는 최대 개수. 작게 잡아 한도 있는 수거 경로를 늘 탄다. */
    private static final int EVENT_BATCH = 8;

    /** 수거 공간. 사건은 작게 잡아 수거 경로를 타고, 출력은 보류 단계가 쌓는 양보다 넉넉하게 잡는다. */
    private static final CollectionSpace SPACE = CollectionSpace.of(64, 1 << 16);

    /** 인증 판단 위임에 답하는 쪽. 러너가 벡터의 인증 설정으로 만든다. */
    interface AuthPolicy {
        AuthDecision decide(String mechanism, byte[] credentials);

        OptionalLong credentialLifetimeSeconds();
    }

    private final Endpoint endpoint;
    private final boolean webSocket;
    private final AuthPolicy auth;
    private final List<Event> events = new ArrayList<>();
    private long elapsedNanos;
    /** 지금 쓰는 출력에 이미 보고한 바이트 수. 다 쓰면 0으로 돌아간다. */
    private int written;

    ProgressDriver(Endpoint endpoint, boolean webSocket, AuthPolicy auth) {
        this.endpoint = endpoint;
        this.webSocket = webSocket;
        this.auth = auth;
    }

    Endpoint endpoint() {
        return endpoint;
    }

    /** 바이트 흐름으로 온 입력. 수거가 필요하다고 멈추면 사건을 수거하고 이어 넣는다. */
    void receiveStream(byte[] bytes) {
        int at = 0;
        while (at < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(bytes, at, bytes.length - at, Integer.MAX_VALUE, SPACE);
            at += progress.consumed();
            if (progress.collectionRequired()) {
                requireCollection(progress.consumed());
            } else if (progress.consumed() == 0) {
                break;
            }
        }
        collectEvents();
    }

    /** 정확히 한 단위(서문 또는 프레임 하나)가 온 입력. 메시지 통로는 메시지 하나를 이렇게 넣는다. */
    void receiveUnit(byte[] unit) {
        while (true) {
            ReceiveProgress progress = endpoint.receiveUnit(unit, 0, unit.length, SPACE);
            if (!progress.collectionRequired()) {
                break;
            }
            requireCollection(progress.consumed());
        }
        collectEvents();
    }

    /** 바이트로 넘길 수 없는 입력(예: 텍스트 메시지)이 온 알림. */
    void receiveUnsupportedInput() {
        while (true) {
            ReceiveProgress progress = endpoint.receiveUnsupportedInput(SPACE);
            if (!progress.collectionRequired()) {
                break;
            }
            requireCollection(progress.consumed());
        }
        collectEvents();
    }

    /** 수거해야 이어 갈 수 있다. 사건을 수거해도 소비도 수거도 없으면 출력이 공간을 막은 것이므로 멈춘다. */
    private void requireCollection(int consumed) {
        if (collectEvents() == 0 && consumed == 0) {
            throw new IllegalStateException("수거 공간이 출력으로 막혔다");
        }
    }

    /** 가상 시계를 밀리초만큼 보낸다. */
    void advanceMilliseconds(long milliseconds) {
        elapsedNanos += milliseconds * 1_000_000L;
        endpoint.advanceTo(elapsedNanos);
        collectEvents();
    }

    /** 지금까지 수거한 사건을 돌려주고 비운다. 인증 판단 요청은 러너가 답하므로 들어 있지 않다. */
    List<Event> takeEvents() {
        collectEvents();
        List<Event> taken = new ArrayList<>(events);
        events.clear();
        return taken;
    }

    /** 넘길 수 있는 출력을 전부 넘기고(진행 보고) 넘긴 메시지를 순서대로 돌려준다. */
    List<OutboundMessage> flush() {
        return flush(Long.MAX_VALUE);
    }

    /**
     * 최대 {@code budgetBytes}바이트까지만 출력을 넘긴다. 한도에 닿으면 그 출력은 쓰기를 시작했지만 다 넘기지 못한 채 남고, 돌려주는 목록에도
     * 들지 않는다. 다음 {@link #flush}가 남은 부분을 넘긴 때 완성된 메시지로 들어간다.
     */
    List<OutboundMessage> flush(long budgetBytes) {
        List<OutboundMessage> sent = new ArrayList<>();
        long budget = budgetBytes;
        while (true) {
            EndpointOutput output = endpoint.nextOutput();
            if (output == null) {
                break;
            }
            if (webSocket) {
                // 메시지 통로는 메시지 전체를 넘겼을 때 전체 길이를 보고한다. 부분 쓰기는 없다.
                endpoint.outputProgress(output.id(), output.length());
                sent.add(output.message());
                continue;
            }
            int remaining = output.length() - written;
            int take = (int) Math.min(budget, remaining);
            if (take > 0) {
                endpoint.outputProgress(output.id(), take);
            }
            budget -= take;
            if (take < remaining) {
                written += take;
                break;
            }
            written = 0;
            sent.add(output.message());
        }
        collectEvents();
        return sent;
    }

    /** 사건을 한도 있게 수거해 쌓는다. 수거한 사건 수(인증 판단 요청 포함)를 돌려준다. */
    private int collectEvents() {
        int collected = 0;
        while (true) {
            List<Event> batch = endpoint.drainEvents(EVENT_BATCH);
            if (batch.isEmpty()) {
                return collected;
            }
            collected += batch.size();
            for (Event event : batch) {
                if (event.kind() == Event.Kind.AUTHENTICATION_REQUIRED) {
                    decideAuthentication(event);
                } else {
                    events.add(event);
                }
            }
        }
    }

    private void decideAuthentication(Event event) {
        AuthDecision decision = auth.decide(event.authenticationMechanism().orElse(""), event.body());
        endpoint.completeAuthentication(event.authenticationId(), decision, auth.credentialLifetimeSeconds());
        if (decision == AuthDecision.PASS) {
            // 핸들러는 인증 직후 준비돼 있다고 본다. 준비 전 거절은 전송 모듈의 정책이다.
            endpoint.handlerReady();
        }
    }
}
