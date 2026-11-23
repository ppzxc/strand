package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.CollectionSpace;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.core.internal.ReceiveProgress;
import java.util.ArrayList;
import java.util.List;

/**
 * 시험 상대의 core {@link Endpoint}에 받은 바이트를 한도 있는 입력으로 넣는다. 위임된 인증 판단은 익명 인증만 통과시키는 정책으로 곧바로 입력하고, 그
 * 밖의 사건은 돌려준다.
 */
final class EndpointFeed {
    private static final CollectionSpace SPACE = CollectionSpace.of(1 << 16, 1 << 16);
    private static final int EVENT_BATCH = 1024;

    private EndpointFeed() {}

    /** 받은 바이트를 모두 넣고, 그 사이 나온 사건 가운데 인증 판단을 뺀 것을 돌려준다. */
    static List<Event> receive(Endpoint endpoint, byte[] bytes) {
        List<Event> events = new ArrayList<>();
        int position = 0;
        while (position < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(bytes, position, bytes.length - position, 64, SPACE);
            position += progress.consumed();
            events.addAll(drain(endpoint));
            if (!progress.collectionRequired() && progress.consumed() == 0) {
                break;
            }
        }
        return events;
    }

    /** 쌓인 사건을 모두 수거하고 인증 판단은 입력한다. 인증 판단을 뺀 사건을 돌려준다. */
    static List<Event> drain(Endpoint endpoint) {
        List<Event> events = new ArrayList<>();
        for (List<Event> batch = endpoint.drainEvents(EVENT_BATCH);
                !batch.isEmpty();
                batch = endpoint.drainEvents(EVENT_BATCH)) {
            for (Event event : batch) {
                if (event.kind() == Event.Kind.AUTHENTICATION_REQUIRED) {
                    boolean anonymous =
                            event.authenticationMechanism().orElse("").equals("ANONYMOUS") && event.body().length == 0;
                    endpoint.completeAuthentication(
                            event.authenticationId(), anonymous ? AuthDecision.PASS : AuthDecision.REJECT);
                } else {
                    events.add(event);
                }
            }
        }
        return events;
    }
}
