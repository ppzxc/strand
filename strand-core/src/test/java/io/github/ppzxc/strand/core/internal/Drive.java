package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * 테스트용 진행 보고 구동 도우미. 입력은 한도 있는 입력({@code receiveBounded})으로 넣고, 출력은 다음 출력을 꺼내 전체를 쓴 것으로 보고하고,
 * 시계는 나노초({@code advanceTo})로 옮긴다. 인증은 위임된 인증 판단({@code AUTHENTICATION_REQUIRED} 사건과 {@code
 * completeAuthentication})으로 처리한다. 테스트는 입력 방법만 이 도우미로 바꾸고 단언은 그대로 둔다.
 */
final class Drive {
    /** 판단을 대신하는 테스트용 인증 정책. */
    @FunctionalInterface
    interface Authentication {
        AuthDecision decide(String mechanism, byte[] credentials);
    }

    private static final class Session {
        final EndpointConfig config;
        final Authentication authentication;
        final List<Event> stash = new ArrayList<>();
        long elapsedNanos;
        boolean answered;

        Session(EndpointConfig config, Authentication authentication) {
            this.config = config;
            this.authentication = authentication;
        }
    }

    private static final Map<Endpoint, Session> SESSIONS = new WeakHashMap<>();

    private Drive() {}

    private static Authentication anonymousOnly() {
        return (mechanism, credentials) ->
                mechanism.equals("ANONYMOUS") && credentials.length == 0 ? AuthDecision.PASS : AuthDecision.REJECT;
    }

    /** 익명 인증만 통과시키는 서버. 인증 판단은 사건으로 위임되고 도우미가 그 자리에서 입력한다. */
    static Endpoint server(EndpointConfig config) {
        return server(config, anonymousOnly());
    }

    static Endpoint server(EndpointConfig config, Authentication authentication) {
        Endpoint endpoint = Endpoint.serverWithDeferredAuthentication(config);
        SESSIONS.put(endpoint, new Session(config, authentication));
        return endpoint;
    }

    private static Session session(Endpoint endpoint) {
        return SESSIONS.computeIfAbsent(endpoint, e -> new Session(null, null));
    }

    /** 모든 바이트를 넣는다. 한 단위씩 처리하며 위임된 인증 판단이 나오면 곧바로 입력한다. */
    static void receive(Endpoint endpoint, byte[] bytes) {
        Session session = session(endpoint);
        int offset = 0;
        while (true) {
            ReceiveProgress progress =
                    endpoint.receiveBounded(bytes, offset, bytes.length - offset, 1, CollectionSpace.UNLIMITED);
            offset += progress.consumed();
            if (session.authentication != null && !session.answered) {
                answerAuthentication(endpoint, session);
            }
            if (offset >= bytes.length || progress.consumed() == 0) {
                return;
            }
        }
    }

    /** 단위 입력처럼 {@link #receive}를 거치지 않은 입력 뒤에 위임된 인증 판단이 나왔으면 입력한다. */
    static void settle(Endpoint endpoint) {
        Session session = session(endpoint);
        if (session.authentication != null && !session.answered) {
            answerAuthentication(endpoint, session);
        }
    }

    private static void answerAuthentication(Endpoint endpoint, Session session) {
        for (Event event : endpoint.drainEvents(Integer.MAX_VALUE)) {
            if (event.kind() != Event.Kind.AUTHENTICATION_REQUIRED) {
                session.stash.add(event);
                continue;
            }
            AuthDecision decision;
            try {
                decision = session.authentication.decide(
                        event.authenticationMechanism().get(), event.body());
            } catch (RuntimeException e) {
                decision = AuthDecision.TRANSIENT_FAILURE;
            }
            endpoint.completeAuthentication(
                    event.authenticationId(), decision, session.config.credentialLifetimeSeconds());
            session.answered = true;
        }
    }

    /** 쌓인 출력을 꺼내 모두 쓴 것으로 보고하고 순서대로 돌려준다. */
    static List<OutboundMessage> drainMessages(Endpoint endpoint) {
        List<OutboundMessage> drained = new ArrayList<>();
        EndpointOutput output;
        while ((output = endpoint.nextOutput()) != null) {
            drained.add(output.message());
            endpoint.outputProgress(output.id(), output.length());
        }
        return drained;
    }

    static List<Event> drainEvents(Endpoint endpoint) {
        Session session = session(endpoint);
        List<Event> events = new ArrayList<>(session.stash);
        session.stash.clear();
        events.addAll(endpoint.drainEvents(Integer.MAX_VALUE));
        return events;
    }

    /** 가상 시계를 밀리초만큼 앞으로 보낸다. */
    static void advance(Endpoint endpoint, long ms) {
        if (ms < 0) {
            throw new IllegalArgumentException("경과 시간은 음수일 수 없다: " + ms);
        }
        Session session = session(endpoint);
        session.elapsedNanos += ms * Deadlines.NANOS_PER_MILLI;
        endpoint.advanceTo(session.elapsedNanos);
    }
}
