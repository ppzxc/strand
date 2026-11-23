package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * 테스트용 엔드포인트 구동 도구. core {@link Endpoint}를 진행 보고 방식으로 돌린다: 한도 있는 입력, 나노초 시계, 다음 출력과 진행 보고, 한도 있는 사건
 * 수거, 위임된 인증 판단. core·netty·적합성 테스트가 함께 쓴다.
 *
 * <p>입력·시계·출력 진행 중 나온 사건은 인증 판단 요청이면 정책으로 곧바로 답하고, 나머지는 쌓아 두었다가 {@link #takeEvents}로 돌려준다. 인증 정책이 없으면
 * 인증 판단 요청도 다른 사건처럼 쌓는다. 설정(수거 공간, 사건 수거 한도, 한 번에 넣는 단위 수, 메시지 통로 여부)은 모듈마다 다르게 둘 수 있다.
 */
public final class EndpointDriver {
    /** 인증 판단 위임에 답하는 쪽. */
    public interface Authentication {
        AuthDecision decide(String mechanism, byte[] credentials);

        /** 판단에 붙이는 자격 유효 기간(초). 없으면 정하지 않는다. */
        default OptionalLong credentialLifetimeSeconds() {
            return OptionalLong.empty();
        }
    }

    /** 수거 한도에 걸리지 않게 하는 넉넉한 수거 공간. */
    public static final CollectionSpace ROOMY = CollectionSpace.of(Integer.MAX_VALUE, Integer.MAX_VALUE);

    private final Endpoint endpoint;
    private final List<Event> events = new ArrayList<>();
    private Authentication authentication;
    private CollectionSpace space = ROOMY;
    private int eventBatch = Integer.MAX_VALUE;
    private int unitsPerReceive = Integer.MAX_VALUE;
    private boolean messageChannel;
    private boolean eager;
    private boolean answered;
    private long elapsedNanos;
    /** 지금 쓰는 출력에 이미 보고한 바이트 수. 다 쓰면 0으로 돌아간다. */
    private int written;

    private EndpointDriver(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    public static EndpointDriver of(Endpoint endpoint) {
        return new EndpointDriver(endpoint);
    }

    /** 인증 판단 요청에 답하는 정책을 정한다. */
    public EndpointDriver authentication(Authentication authentication) {
        this.authentication = authentication;
        return this;
    }

    /** 익명 인증만 통과시키는 정책을 정한다. 자격 유효 기간은 주어진 값을 쓴다. */
    public EndpointDriver anonymousOnly(OptionalLong credentialLifetimeSeconds) {
        return authentication(new Authentication() {
            @Override
            public AuthDecision decide(String mechanism, byte[] credentials) {
                return mechanism.equals("ANONYMOUS") && credentials.length == 0
                        ? AuthDecision.PASS
                        : AuthDecision.REJECT;
            }

            @Override
            public OptionalLong credentialLifetimeSeconds() {
                return credentialLifetimeSeconds;
            }
        });
    }

    public EndpointDriver space(CollectionSpace space) {
        this.space = space;
        return this;
    }

    /** 사건을 한 번에 수거하는 최대 개수. */
    public EndpointDriver eventBatch(int eventBatch) {
        this.eventBatch = eventBatch;
        return this;
    }

    /** 바이트 흐름 입력에서 한 번에 처리하는 최대 단위 수. */
    public EndpointDriver unitsPerReceive(int unitsPerReceive) {
        this.unitsPerReceive = unitsPerReceive;
        return this;
    }

    /** 메시지 통로: 출력을 메시지 전체로만 넘기고 부분 쓰기는 없다. */
    public EndpointDriver messageChannel(boolean messageChannel) {
        this.messageChannel = messageChannel;
        return this;
    }

    /**
     * 사건을 입력·시계·출력 진행마다 수거할지 정한다. 꺼 두면(기본) 첫 인증 판단에 답할 때까지만 입력 직후 사건을 수거하고, 그 뒤 사건은 엔드포인트에
     * 남겨 두었다가 {@link #takeEvents}가 수거한다. 사건을 아직 넘기지 않은 상태를 시험하는 테스트가 쓴다.
     */
    public EndpointDriver eagerEvents(boolean eager) {
        this.eager = eager;
        return this;
    }

    public Endpoint endpoint() {
        return endpoint;
    }

    /** 바이트 흐름으로 온 입력을 모두 넣는다. 수거가 필요하면 사건을 수거하고 이어 넣는다. */
    public void receiveStream(byte[] bytes) {
        int at = 0;
        while (at < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(bytes, at, bytes.length - at, unitsPerReceive, space);
            at += progress.consumed();
            if (progress.collectionRequired()) {
                requireCollection(progress.consumed());
            } else {
                afterInput();
                if (progress.consumed() == 0) {
                    break;
                }
            }
        }
        afterInput();
    }

    /** 정확히 한 단위(서문 또는 프레임 하나)가 온 입력. 메시지 통로는 메시지 하나를 이렇게 넣는다. */
    public void receiveUnit(byte[] unit) {
        while (true) {
            ReceiveProgress progress = endpoint.receiveUnit(unit, 0, unit.length, space);
            if (!progress.collectionRequired()) {
                break;
            }
            requireCollection(progress.consumed());
        }
        afterInput();
    }

    /** 바이트로 넘길 수 없는 입력(예: 텍스트 메시지)이 온 알림. */
    public void receiveUnsupportedInput() {
        while (true) {
            ReceiveProgress progress = endpoint.receiveUnsupportedInput(space);
            if (!progress.collectionRequired()) {
                break;
            }
            requireCollection(progress.consumed());
        }
        afterInput();
    }

    /** 수거해야 이어 갈 수 있다. 사건을 수거해도 소비도 수거도 없으면 출력이 공간을 막은 것이므로 멈춘다. */
    private void requireCollection(int consumed) {
        if (collectEvents() == 0 && consumed == 0) {
            throw new IllegalStateException("수거 공간이 출력으로 막혔다");
        }
    }

    /** 가상 시계를 밀리초만큼 앞으로 보낸다. */
    public void advanceMilliseconds(long milliseconds) {
        if (milliseconds < 0) {
            throw new IllegalArgumentException("경과 시간은 음수일 수 없다: " + milliseconds);
        }
        elapsedNanos += milliseconds * Deadlines.NANOS_PER_MILLI;
        endpoint.advanceTo(elapsedNanos);
        afterActivity();
    }

    /** 단위 입력처럼 이 도구를 거치지 않은 입력 뒤에 나온 사건을 수거해 인증 판단 요청에 답한다. 나머지 사건은 쌓아 둔다. */
    public void settle() {
        afterInput();
    }

    /** 지금까지 수거한 사건을 돌려주고 비운다. 인증 정책이 답한 인증 판단 요청은 들어 있지 않다. */
    public List<Event> takeEvents() {
        collectEvents();
        List<Event> taken = new ArrayList<>(events);
        events.clear();
        return taken;
    }

    /** 넘길 수 있는 출력을 전부 넘기고(진행 보고) 넘긴 메시지를 순서대로 돌려준다. */
    public List<OutboundMessage> flush() {
        return flush(Long.MAX_VALUE);
    }

    /**
     * 최대 {@code budgetBytes}바이트까지만 출력을 넘긴다. 한도에 닿으면 그 출력은 쓰기를 시작했지만 다 넘기지 못한 채 남고, 돌려주는 목록에도
     * 들지 않는다. 다음 {@link #flush}가 남은 부분을 넘긴 때 완성된 메시지로 들어간다.
     */
    public List<OutboundMessage> flush(long budgetBytes) {
        List<OutboundMessage> sent = new ArrayList<>();
        long budget = budgetBytes;
        while (true) {
            EndpointOutput output = endpoint.nextOutput();
            if (output == null) {
                break;
            }
            if (messageChannel) {
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
        afterActivity();
        return sent;
    }

    private void afterInput() {
        if (eager || (authentication != null && !answered)) {
            collectEvents();
        }
    }

    private void afterActivity() {
        if (eager) {
            collectEvents();
        }
    }

    /** 사건을 한도 있게 수거해 쌓는다. 수거한 사건 수(인증 판단 요청 포함)를 돌려준다. */
    private int collectEvents() {
        int collected = 0;
        while (true) {
            List<Event> batch = endpoint.drainEvents(eventBatch);
            if (batch.isEmpty()) {
                return collected;
            }
            collected += batch.size();
            for (Event event : batch) {
                if (authentication != null && event.kind() == Event.Kind.AUTHENTICATION_REQUIRED) {
                    decideAuthentication(event);
                } else {
                    events.add(event);
                }
            }
        }
    }

    private void decideAuthentication(Event event) {
        AuthDecision decision;
        try {
            decision = authentication.decide(event.authenticationMechanism().orElse(""), event.body());
        } catch (RuntimeException e) {
            decision = AuthDecision.TRANSIENT_FAILURE;
        }
        endpoint.completeAuthentication(event.authenticationId(), decision, authentication.credentialLifetimeSeconds());
        answered = true;
    }
}
