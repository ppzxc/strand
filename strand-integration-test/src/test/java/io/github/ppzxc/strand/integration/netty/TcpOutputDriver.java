package io.github.ppzxc.strand.integration.netty;

import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointOutput;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 검증용 TCP 구동. core 출력을 channel.write/flush/Future 없이 실제 소켓 쓰기량으로만 보고한다. 모든 메서드는 이벤트 루프에서 부른다. 한
 * 번의 {@link #pump()} 안에서 유효성 확인(nextOutput), 비차단 소켓 쓰기, 진행 보고(outputProgress)가 끊김 없이 이어진다.
 */
final class TcpOutputDriver {
    private final BoundaryChannel channel;
    private final Endpoint endpoint;
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    private final List<ByteBuf> allocated = new ArrayList<>();
    private final List<Integer> reported = new ArrayList<>();
    private ByteBuf current;
    private long currentId = -1;
    private int zeroWrites;
    private boolean retryScheduled;

    TcpOutputDriver(BoundaryChannel channel, Endpoint endpoint) {
        this.channel = channel;
        this.endpoint = endpoint;
        // 통로가 어떤 경로로 닫혀도 core에 끊김을 알리고 남은 내부 참조를 반환한다.
        channel.closeFuture().addListener(future -> {
            endpoint.disconnect();
            releaseCurrent();
        });
    }

    /** 이벤트 루프에서 core 명령 하나를 실행한 뒤 곧바로 출력을 이어 쓴다. 명령과 쓰기 시도 사이에 다른 명령이 끼지 않는다. */
    void command(Runnable command) {
        channel.eventLoop().execute(() -> {
            command.run();
            pump();
        });
    }

    /** 명령만 이벤트 루프에 넣는다. 한 번에 밀려 있던 여러 명령 뒤에 {@link #pump()}가 이어지는 경우를 재현한다. */
    void enqueue(Runnable command) {
        channel.eventLoop().execute(command);
    }

    void enqueuePump() {
        channel.eventLoop().execute(this::pump);
    }

    void pump() {
        if (!channel.isOpen()) return;
        try {
            while (true) {
                EndpointOutput output = endpoint.nextOutput();
                if (output == null) {
                    releaseCurrent();
                    return;
                }
                if (output.id() != currentId) {
                    releaseCurrent(); // 쓰기 전에 철회되거나 우선순위가 바뀐 출력의 참조
                    current = allocator.directBuffer(output.length());
                    current.writeBytes(output.bytes());
                    allocated.add(current);
                    currentId = output.id();
                }
                int count = channel.writeAttempt(current.nioBuffer());
                endpoint.outputProgress(currentId, count);
                reported.add(count);
                if (count == 0) {
                    zeroWrites++;
                    scheduleRetry();
                    return;
                }
                current.skipBytes(count);
                if (!current.isReadable()) {
                    releaseCurrent();
                }
            }
        } catch (IOException failure) {
            channel.close();
        }
    }

    /** 쓰기 가능 알림 연결은 이 검증 범위가 아니므로 짧은 주기로 다시 시도한다. */
    private void scheduleRetry() {
        if (retryScheduled) return;
        retryScheduled = true;
        channel.eventLoop()
                .schedule(
                        () -> {
                            retryScheduled = false;
                            pump();
                        },
                        1,
                        TimeUnit.MILLISECONDS);
    }

    private void releaseCurrent() {
        if (current != null) {
            current.release();
            current = null;
            currentId = -1;
        }
    }

    boolean hasPendingOutput() {
        return endpoint.nextOutput() != null;
    }

    int zeroWrites() {
        return zeroWrites;
    }

    List<Integer> reported() {
        return new ArrayList<>(reported);
    }

    /** 만든 모든 버퍼의 참조 수가 0이고 할당기가 센 직접 메모리 사용량이 0이다. 중복 해제는 해제 시점에 예외로 드러난다. */
    boolean allReleased() {
        for (ByteBuf buffer : allocated) {
            if (buffer.refCnt() != 0) return false;
        }
        return allocator.metric().usedDirectMemory() == 0;
    }

    int allocatedCount() {
        return allocated.size();
    }
}
