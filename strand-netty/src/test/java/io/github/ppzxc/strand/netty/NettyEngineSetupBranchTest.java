package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Resource;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class NettyEngineSetupBranchTest {

    private static Throwable failureOf(NettyRig rig) {
        assertThatThrownBy(() -> rig.ready.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
        try {
            rig.ready.get();
            throw new AssertionError("준비가 실패해야 한다");
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void anExceptionBeforeTheChannelIsActiveEndsThePreparation() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            rig.loop.register(rig.channel).sync();

            rig.loop.execute(() -> rig.channel.pipeline().fireExceptionCaught(new IOException("활성화 전 오류")));

            assertThat(failureOf(rig)).isInstanceOf(IOException.class).hasMessage("활성화 전 오류");
        }
    }

    @Test
    void aSecondFailureBeforeTheChannelIsActiveKeepsTheFirstCause() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            rig.loop.register(rig.channel).sync();

            rig.onLoop(() -> {
                rig.channel.pipeline().fireExceptionCaught(new IOException("첫 원인"));
                rig.channel.pipeline().fireExceptionCaught(new IOException("두 번째 원인"));
            });

            assertThat(failureOf(rig)).hasMessage("첫 원인");
        }
    }

    @Test
    void aCancelRequestedBeforeTheEngineIsBoundIsDeliveredWhenItBinds() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            ConnectAttemptState state = new ConnectAttemptState();
            rig.params.attempt = state;
            assertThat(state.cancel()).isTrue();

            rig.connection.startClient(rig.target);

            assertThat(failureOf(rig)).isInstanceOf(ConnectionAttemptCancelledException.class);
        }
    }

    @Test
    void aCancelDeliveredAfterThePreparationEndedChangesNothing() throws Exception {
        try (NettyRig rig = NettyRig.client()) {
            java.lang.reflect.Field field = NettyStrandConnection.class.getDeclaredField("engine");
            field.setAccessible(true);
            ConnectionEngine engine = (ConnectionEngine) field.get(rig.connection);

            engine.cancelAttempt();
            rig.settle();

            assertThat(rig.connection.closed().isDone()).isFalse();
        }
    }

    @Test
    void anExceptionOnAnActiveConnectionEndsItThroughTheTransport() throws Exception {
        try (NettyRig rig = NettyRig.client()) {
            rig.loop.execute(() -> rig.channel.pipeline().fireExceptionCaught(new IOException("전송 오류")));

            NettyRig.awaitTrue(() -> rig.connection.closed().isDone());
        }
    }

    @Test
    void aPreparationDeadlineThatPassesBeforeTheFirstAddressIsTriedEndsTheAttempt() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            rig.onLoop(() -> {
                rig.connection.startClient(rig.target);
                rig.advance(Duration.ofHours(2));
            });

            assertThat(failureOf(rig)).isInstanceOf(PreparationDeadlineExceededException.class);
        }
    }

    @Test
    void aChannelThatIsAlreadyRegisteredFailsTheClientRegistration() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            rig.loop.register(rig.channel).sync();

            rig.connection.startClient(rig.target);

            assertThat(failureOf(rig)).isNotNull();
        }
    }

    @Test
    void aChannelThatIsAlreadyRegisteredFailsTheServerRegistration() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            rig.loop.register(rig.channel).sync();

            rig.connection.startServer();

            assertThat(failureOf(rig)).isNotNull();
        }
    }

    @Test
    void anUnresolvedTargetFailsWhenNoResolveSlotIsLeft() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            long limit = rig.fixture.limits.perOwner().get(Resource.RESOLVE_TASKS);
            assertThat(rig.owner.tryReserve(Resource.RESOLVE_TASKS, limit)).isTrue();

            rig.connection.startClient(InetSocketAddress.createUnresolved("some-host", rig.target.getPort()));

            assertThat(failureOf(rig)).isInstanceOf(IOException.class);
        }
    }

    @Test
    void aChannelClosedWithoutAnyCauseEndsThePreparationAndLaterAddressesAreNotTried() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            rig.loop.register(rig.channel).sync();
            rig.channel.close().sync();

            assertThat(failureOf(rig)).isInstanceOf(IllegalStateException.class);
            rig.connection.startClient(rig.target);
            rig.settleLoop();

            NettyRig.awaitTrue(() -> !rig.channel.isRegistered());
        }
    }

    @Test
    void inboundEventsOtherThanBytesAreIgnoredAndActivationIsNotRepeated() throws Exception {
        try (NettyRig rig = NettyRig.client()) {
            rig.onLoop(() -> {
                rig.channel.pipeline().fireChannelRead("바이트가 아닌 메시지");
                rig.channel.pipeline().fireChannelActive();
            });

            rig.settle();
            assertThat(rig.connection.closed().isDone()).isFalse();
        }
    }

    @Test
    void inboundEventsAfterTheConnectionIsStoppedAreIgnored() throws Exception {
        try (NettyRig rig = NettyRig.client()) {
            rig.connection.abort();
            NettyRig.awaitTrue(() -> rig.connection.closed().isDone());

            rig.onLoop(() -> {
                rig.channel.pipeline().fireChannelActive();
                rig.channel.pipeline().fireChannelRead(io.netty.buffer.Unpooled.wrappedBuffer(new byte[] {1, 2, 3}));
            });
            rig.settleLoop();

            assertThat(rig.connection.closed().isDone()).isTrue();
        }
    }

    @Test
    void bytesBeforeTheChannelIsActiveAreIgnored() throws Exception {
        try (NettyRig rig = NettyRig.idle()) {
            rig.loop.register(rig.channel).sync();

            rig.onLoop(() ->
                    rig.channel.pipeline().fireChannelRead(io.netty.buffer.Unpooled.wrappedBuffer(new byte[] {1})));
            rig.settleLoop();

            assertThat(rig.ready.isDone()).isFalse();
        }
    }
}
