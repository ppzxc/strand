package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** 서버 핸들러 준비 전에 온 첫 요청의 거절(UNAVAILABLE·처리 안 됨)을 예제가 유한하게 다시 보내는 정책을 본다. */
class AppExampleSupportTest {
    private static final byte[] EMPTY = new byte[0];

    @Test
    void aRefusedRequestIsSentAgainUntilTheServerHandlerIsReady() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        RequestResult result = AppExampleSupport.requestAfterRefusal(() -> calls.incrementAndGet() < 3
                ? new RequestResult(Status.UNAVAILABLE, Certainty.NOT_PROCESSED, EMPTY)
                : new RequestResult(Status.OK, Certainty.RESPONSE, EMPTY));

        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(calls).hasValue(3);
    }

    @Test
    void anAnswerThatIsNotARefusalIsReturnedAtOnce() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        RequestResult result = AppExampleSupport.requestAfterRefusal(() -> {
            calls.incrementAndGet();
            return new RequestResult(Status.UNAVAILABLE, Certainty.UNKNOWN, EMPTY);
        });

        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThat(calls).hasValue(1);
    }
}
