package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Extensions;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Status;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class NettyStrandUsageTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    static Stream<String> transports() {
        return Stream.of("tcp", "tls");
    }

    private AppFixture fixture(String transport) throws Exception {
        AppFixture fixture = transport.equals("tls") ? AppFixture.tls() : AppFixture.plain();
        closeables.add(fixture);
        return fixture;
    }

    private static CredentialVerifier acceptAll(String principal) {
        return credentials -> AuthenticationResult.accepted(principal);
    }

    private static RequestResult requestWhenReady(AppFixture fixture, NettyStrandConnection connection, byte[] payload)
            throws Exception {
        long limit = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        RequestResult result =
                fixture.result(connection.request(Request.of(TYPE, payload)).result());
        while (result.status() == Status.UNAVAILABLE
                && result.certainty() == Certainty.NOT_PROCESSED
                && System.nanoTime() < limit) {
            Thread.sleep(10);
            result =
                    fixture.result(connection.request(Request.of(TYPE, payload)).result());
        }
        return result;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private Listener listen(NettyStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private NettyStrandConnection connect(NettyStrandClient client, Listener listener) throws Exception {
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        closeables.add(connection::abort);
        return connection;
    }

    @ParameterizedTest
    @MethodSource("transports")
    void bidirectionalRequestsAndResponsesWorkAfterPreparation(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        CompletableFuture<NettyStrandConnection> serverSide = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("alice"))
                .handlerFactory(connection -> {
                    serverSide.complete(connection);
                    return PreparedHandler.of(request -> {
                        request.respond(
                                Status.OK, bytes("echo:" + new String(request.payload(), StandardCharsets.UTF_8)));
                        request.connection().request(Request.of(TYPE, bytes("reverse")));
                    });
                })
                .build();
        Listener listener = listen(server);
        CompletableFuture<byte[]> reversed = new CompletableFuture<>();
        NettyStrandClient client = fixture.clientBuilder()
                .handler(request -> {
                    reversed.complete(request.payload());
                    request.respond(Status.OK, bytes("ack"));
                })
                .build();

        NettyStrandConnection connection = connect(client, listener);
        RequestResult forward = requestWhenReady(fixture, connection, bytes("ping"));

        assertThat(forward.status()).isEqualTo(Status.OK);
        assertThat(forward.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(forward.payload(), StandardCharsets.UTF_8)).isEqualTo("echo:ping");
        assertThat(reversed.get(15, TimeUnit.SECONDS)).isEqualTo(bytes("reverse"));
        NettyStrandConnection accepted = serverSide.get(15, TimeUnit.SECONDS);
        RequestHandle reverseRequest = accepted.request(Request.of(TYPE, bytes("again")));
        RequestResult backward = fixture.result(reverseRequest.result());
        assertThat(backward.status()).isEqualTo(Status.OK);
        assertThat(backward.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(backward.payload(), StandardCharsets.UTF_8)).isEqualTo("ack");
    }

    @ParameterizedTest
    @MethodSource("transports")
    void connectSucceedsOnlyAfterTheAuthenticatedPeerAndHandlerAreReady(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        AtomicReference<Object> factoryPeer = new AtomicReference<>();
        AtomicReference<List<String>> seenMechanism = new AtomicReference<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> {
                    seenMechanism.set(Arrays.asList(
                            credentials.mechanism(), new String(credentials.secret(), StandardCharsets.UTF_8)));
                    return AuthenticationResult.accepted("user-" + credentials.mechanism());
                })
                .handlerFactory(connection -> {
                    factoryPeer.set(connection.authenticatedPeer().principal());
                    return PreparedHandler.of(request -> request.respond(Status.OK, request.payload()));
                })
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder()
                .credentials(Credentials.of("BEARER", bytes("opaque")))
                .build();

        NettyStrandConnection connection = connect(client, listener);

        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (factoryPeer.get() == null && System.nanoTime() < limit) {
            Thread.sleep(10);
        }
        assertThat(factoryPeer.get()).isEqualTo("user-BEARER");
        assertThat(seenMechanism.get()).containsExactly("BEARER", "opaque");
        assertThat(connection.authenticatedPeer().remoteAddress()).isEqualTo(AppFixture.loopback(listener));
        assertThat(requestWhenReady(fixture, connection, bytes("x")).status()).isEqualTo(Status.OK);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void incomingRequestExposesTypeExtensionsBodyConnectionAndEnded(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        CompletableFuture<IncomingRequest> seen = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handler(seen::complete)
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(fixture.clientBuilder().build(), listener);
        Extensions extensions =
                Extensions.builder().idempotencyKey(bytes("key-1")).build();

        RequestHandle handle =
                connection.request(Request.of(TYPE, bytes("body")).extensions(extensions));
        IncomingRequest request = seen.get(15, TimeUnit.SECONDS);

        assertThat(request.type()).isEqualTo(TYPE);
        assertThat(request.payload()).isEqualTo(bytes("body"));
        assertThat(request.extensions().get(3))
                .hasValueSatisfying(value -> assertThat(value).isEqualTo(bytes("key-1")));
        assertThat(request.connection()).isNotNull();
        assertThat(request.ended().isDone()).isFalse();
        assertThatThrownBy(() -> request.ended().resultNow()).isInstanceOf(IllegalStateException.class);
        CompletableFuture<ResponseAcceptance> later = CompletableFuture.supplyAsync(() -> {
            try {
                return fixture.result(request.respond(Status.OK, bytes("late")));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(later.get(15, TimeUnit.SECONDS).kind()).isEqualTo(AcceptanceKind.ACCEPTED);
        assertThat(fixture.result(handle.result()).payload()).isEqualTo(bytes("late"));
        assertThat(fixture.result(request.ended()).isResponseAccepted()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("transports")
    void onlyTheFirstResponseIsAcceptedAndTheRestReportWhy(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        CompletableFuture<IncomingRequest> seen = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handler(seen::complete)
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(fixture.clientBuilder().build(), listener);
        connection.request(Request.of(TYPE, bytes("body")));
        IncomingRequest request = seen.get(15, TimeUnit.SECONDS);

        ResponseAcceptance first = fixture.result(request.respond(Status.OK, bytes("one")));
        ResponseAcceptance second = fixture.result(request.respond(Status.OK, bytes("two")));

        assertThat(first.kind()).isEqualTo(AcceptanceKind.ACCEPTED);
        assertThat(second.kind()).isIn(AcceptanceKind.ALREADY_RESPONDED, AcceptanceKind.REQUEST_ENDED);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void handlerExceptionAnswersEmptyInternalAndKeepsTheConnection(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handler(request -> {
                    if (request.payload().length == 0) {
                        throw new IllegalStateException("업무 오류 메시지는 상대에게 보이지 않는다");
                    }
                    request.respond(Status.OK, request.payload());
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(fixture.clientBuilder().build(), listener);

        RequestResult failed =
                fixture.result(connection.request(Request.of(TYPE, new byte[0])).result());
        RequestResult next =
                fixture.result(connection.request(Request.of(TYPE, bytes("ok"))).result());

        assertThat(failed.status()).isEqualTo(Status.INTERNAL);
        assertThat(failed.payload()).isEmpty();
        assertThat(next.status()).isEqualTo(Status.OK);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void userCodeNeverRunsOnTheTransportThreadAndRunsOneAtATimePerConnection(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        List<String> threads = new CopyOnWriteArrayList<>();
        List<Integer> overlap = new CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicInteger running = new java.util.concurrent.atomic.AtomicInteger();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> {
                    threads.add(Thread.currentThread().getName());
                    return AuthenticationResult.accepted("p");
                })
                .handlerFactory(connection -> {
                    threads.add(Thread.currentThread().getName());
                    return PreparedHandler.of(request -> {
                        overlap.add(running.incrementAndGet());
                        threads.add(Thread.currentThread().getName());
                        Thread.sleep(30);
                        running.decrementAndGet();
                        request.respond(Status.OK, new byte[0]);
                    });
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(fixture.clientBuilder().build(), listener);

        fixture.requestWhenServed(connection, Request.of(TYPE, new byte[0]));
        RequestHandle[] handles = new RequestHandle[4];
        for (int i = 0; i < handles.length; i++) {
            handles[i] = connection.request(Request.of(TYPE, new byte[0]));
        }
        for (RequestHandle handle : handles) {
            fixture.result(handle.result());
        }

        assertThat(threads).isNotEmpty().allSatisfy(name -> assertThat(name).startsWith("app-executor-"));
        assertThat(overlap).containsOnly(1);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void closeThenAbortAndTheRetainedClosedResultReleaseEverythingTheConnectionOwned(String transport)
            throws Exception {
        AppFixture fixture = fixture(transport);
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, new byte[0]));
                })
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = connect(client, listener);
        NettyStrandConnection peer = accepted.get(15, TimeUnit.SECONDS);
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(1);

        connection.close();
        connection.close();
        CloseNotice notice = fixture.result(connection.closed());
        CloseNotice peerNotice = fixture.result(peer.closed());

        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).hasValue(CloseNotice.Sender.LOCAL);
        assertThat(peerNotice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(peerNotice.sender()).hasValue(CloseNotice.Sender.PEER);
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.TLS_CIPHERTEXT_BYTES).current())
                .isZero();
        assertThat(client.resourceUsage(Resource.FRAME_ASSEMBLY_BYTES).current())
                .isZero();
        assertThat(connection.resourceUsage(Resource.FRAME_ASSEMBLY_BYTES).current())
                .isZero();
        connection.abort();
        assertThat(fixture.pool.isShutdown()).isFalse();
        assertThat(fixture.result(connection.closed())).isSameAs(notice);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void abortEndsTheConnectionImmediatelyAndThePeerSeesADisconnect(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> {});
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(fixture.clientBuilder().build(), listener);
        NettyStrandConnection peer = accepted.get(15, TimeUnit.SECONDS);
        RequestHandle pendingRequest = connection.request(Request.of(TYPE, bytes("never answered")));

        connection.abort();

        CloseNotice notice = fixture.result(connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(fixture.result(peer.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        RequestResult result = fixture.result(pendingRequest.result());
        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.certainty()).isIn(Certainty.UNKNOWN, Certainty.NOT_PROCESSED);
        RequestResult afterEnd = fixture.result(
                connection.request(Request.of(TYPE, bytes("late"))).result());
        assertThat(afterEnd.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(afterEnd.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void requestDeadlineStartsAtTheCallAndZeroExpiresImmediately(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handler(request -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(fixture.clientBuilder().build(), listener);

        RequestResult immediate = fixture.result(connection
                .request(Request.of(TYPE, bytes("x")), RequestOptions.deadlineAfter(Duration.ZERO))
                .result());
        RequestResult timedOut = fixture.result(connection
                .request(Request.of(TYPE, bytes("y")), RequestOptions.deadlineAfter(Duration.ofMillis(150)))
                .result());

        assertThat(immediate.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(immediate.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(timedOut.status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(timedOut.certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThatThrownBy(() -> RequestOptions.deadlineAfter(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void cancelYieldsTheRetainedCancelledResultNotTheCancelCall(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handler(request -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(fixture.clientBuilder().build(), listener);
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("slow")));

        handle.cancel();
        handle.cancel();

        RequestResult result = fixture.result(handle.result());
        assertThat(result.status()).isEqualTo(Status.CANCELLED);
        assertThat(fixture.result(handle.result())).isSameAs(result);
        handle.cancel();
        assertThat(fixture.result(handle.result())).isSameAs(result);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void outgoingRequestLimitRejectsLocallyAndReturnsTheReservations(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handler(request -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = connect(client, listener);
        RequestHandle[] handles = new RequestHandle[8];
        for (int i = 0; i < handles.length; i++) {
            handles[i] = connection.request(Request.of(TYPE, bytes("held")));
        }

        RequestResult rejected = fixture.result(
                connection.request(Request.of(TYPE, bytes("one too many"))).result());

        assertThat(rejected.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(rejected.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(rejected.resource()).hasValue(Resource.OUTGOING_REQUESTS);
        assertThat(connection.resourceUsage(Resource.OUTGOING_REQUESTS).rejections())
                .isEqualTo(1);
        for (RequestHandle handle : handles) {
            handle.cancel();
        }
        for (RequestHandle handle : handles) {
            fixture.result(handle.result());
        }
        assertThat(connection.resourceUsage(Resource.OUTGOING_REQUESTS).current())
                .isZero();
        assertThat(connection.resourceUsage(Resource.OUTGOING_BYTES).current()).isZero();
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isZero();
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).highWater()).isEqualTo(8);
    }

    @ParameterizedTest
    @MethodSource("transports")
    void clientWithoutAHandlerAnswersUnimplemented(String transport) throws Exception {
        AppFixture fixture = fixture(transport);
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(acceptAll("p"))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> {});
                })
                .build();
        Listener listener = listen(server);
        connect(fixture.clientBuilder().build(), listener);

        RequestResult result = fixture.result(accepted.get(15, TimeUnit.SECONDS)
                .request(Request.of(TYPE, bytes("x")))
                .result());

        assertThat(result.status()).isEqualTo(Status.UNIMPLEMENTED);
    }
}
