package io.github.ppzxc.strand.nio.internal;

import java.util.concurrent.CompletableFuture;

/** 상대가 연 스트림의 요청 하나. type, 요청 본문, 응답할 스트림 번호를 가진다. */
public final class IncomingRequest {

    private final ConnectionDriver driver;
    private final long streamId;
    private final int type;
    private final byte[] body;

    IncomingRequest(ConnectionDriver driver, long streamId, int type, byte[] body) {
        this.driver = driver;
        this.streamId = streamId;
        this.type = type;
        this.body = body;
    }

    /**
     * 응답할 스트림 번호.
     *
     * @return 스트림 번호
     */
    public long streamId() {
        return streamId;
    }

    /**
     * 애플리케이션 {@code type}.
     *
     * @return type 값
     */
    public int type() {
        return type;
    }

    /**
     * 요청 본문. 부를 때마다 복사본을 돌려준다.
     *
     * @return 본문의 복사본
     */
    public byte[] body() {
        return body.clone();
    }

    /**
     * 이 요청에 응답한다. {@link ConnectionDriver#respond}와 같다.
     *
     * @param status 규격 2.5.1절의 status 값
     * @param payload 응답 본문
     * @return 응답 명령이 처리되면 끝나는 Future
     */
    public CompletableFuture<Void> respond(int status, byte[] payload) {
        return driver.respond(streamId, status, payload);
    }
}
