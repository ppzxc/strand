package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

class PassageWrappingTest {

    private static final class CountingPassage implements Passage {
        private final Passage delegate;
        final AtomicLong written = new AtomicLong();

        CountingPassage(Passage delegate) {
            this.delegate = delegate;
        }

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
            return delegate.await(timeoutMs, waitForRead, waitForWrite);
        }

        @Override
        public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
            int count = delegate.writeBytes(bytes, offset, length);
            written.addAndGet(count);
            return count;
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static RequestResult echoOnce(
            AppFixture fixture,
            PassageWrapping.@Nullable Wrapper serverWrapper,
            PassageWrapping.@Nullable Wrapper clientWrapper)
            throws Exception {
        NioStrandServer.Builder serverBuilder = fixture.serverBuilder();
        if (serverWrapper != null) {
            PassageWrapped.server(serverBuilder, serverWrapper);
        }
        NioStrandClient.Builder clientBuilder = fixture.clientBuilder();
        if (clientWrapper != null) {
            PassageWrapped.client(clientBuilder, clientWrapper);
        }
        NioStrandServer server = serverBuilder
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        NioStrandClient client = clientBuilder.build();
        try {
            Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
            NioStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
            RequestResult result = fixture.result(connection
                    .request(Request.of(AppFixture.TYPE, new byte[] {7}))
                    .result());
            connection.close();
            fixture.result(connection.closed());
            listener.close();
            return result;
        } finally {
            AppFixture.get(client.shutdown());
            AppFixture.get(server.shutdown());
        }
    }

    private static PassageWrapping.Wrapper counting(List<PassageWrapping.Side> sides, List<CountingPassage> wrapped) {
        return (side, passage) -> {
            sides.add(side);
            CountingPassage counting = new CountingPassage(passage);
            wrapped.add(counting);
            return counting;
        };
    }

    @Test
    void theEngineRunsEachSideOnThePassageTheWrapperReturns() throws Exception {
        List<PassageWrapping.Side> sides = new CopyOnWriteArrayList<>();
        List<CountingPassage> wrapped = new CopyOnWriteArrayList<>();
        PassageWrapping.Wrapper wrapper = counting(sides, wrapped);
        try (AppFixture fixture = AppFixture.plain()) {
            RequestResult result = echoOnce(fixture, wrapper, wrapper);

            assertThat(result.status()).isEqualTo(Status.OK);
            assertThat(sides).containsExactlyInAnyOrder(PassageWrapping.Side.CLIENT, PassageWrapping.Side.SERVER);
            assertThat(wrapped)
                    .hasSize(2)
                    .allSatisfy(passage -> assertThat(passage.written.get()).isPositive());
        }
    }

    @Test
    void aWrapperOnlyAffectsConnectionsOfTheBuildersItWasInjectedInto() throws Exception {
        List<PassageWrapping.Side> sides = new CopyOnWriteArrayList<>();
        List<CountingPassage> wrapped = new CopyOnWriteArrayList<>();
        PassageWrapping.Wrapper wrapper = counting(sides, wrapped);
        try (AppFixture observed = AppFixture.plain();
                AppFixture other = AppFixture.plain()) {

            echoOnce(observed, wrapper, null);
            assertThat(sides).containsExactly(PassageWrapping.Side.SERVER);

            sides.clear();
            wrapped.clear();
            assertThat(echoOnce(other, null, null).status()).isEqualTo(Status.OK);
            assertThat(sides).isEmpty();
            assertThat(wrapped).isEmpty();
        }
    }

    @Test
    void connectionsOfBuildersWithoutAWrapperAreNotWrappedWhileAnotherPairIsWrappedConcurrently() throws Exception {
        List<PassageWrapping.Side> sides = new CopyOnWriteArrayList<>();
        List<CountingPassage> wrapped = new CopyOnWriteArrayList<>();
        PassageWrapping.Wrapper wrapper = counting(sides, wrapped);
        try (AppFixture observed = AppFixture.plain();
                AppFixture other = AppFixture.plain()) {
            NioStrandServer.Builder wrappedServer = PassageWrapped.server(observed.serverBuilder(), wrapper);
            NioStrandServer holder = wrappedServer
                    .authenticate(c -> AuthenticationResult.accepted("p"))
                    .handler(request -> request.respond(Status.OK, request.payload()))
                    .build();
            try {

                assertThat(echoOnce(other, null, null).status()).isEqualTo(Status.OK);
                assertThat(sides).isEmpty();
            } finally {
                AppFixture.get(holder.shutdown());
            }
        }
    }

    @Test
    void tlsPassagesAreLeftUnwrappedBecauseTheEngineQueriesThemDirectly() throws Exception {
        List<PassageWrapping.Side> sides = new CopyOnWriteArrayList<>();
        List<CountingPassage> wrapped = new CopyOnWriteArrayList<>();
        PassageWrapping.Wrapper wrapper = counting(sides, wrapped);
        try (AppFixture fixture = AppFixture.tls()) {
            assertThat(echoOnce(fixture, wrapper, wrapper).status()).isEqualTo(Status.OK);

            assertThat(sides).isEmpty();
        }
    }

    @Test
    void thePassageWrappingSeamAppearsNowhereInThePublicApi() throws Exception {
        List<String> exposing = publicPackageClassNames().stream()
                .map(PassageWrappingTest::load)
                .filter(type -> Modifier.isPublic(type.getModifiers()))
                .flatMap(type -> Stream.concat(
                        Stream.of(type.getMethods()).map(Method::toGenericString),
                        Stream.of(type.getConstructors()).map(Object::toString)))
                .filter(signature -> signature.contains("PassageWrapping"))
                .collect(Collectors.toList());

        assertThat(publicPackageClassNames()).contains("io.github.ppzxc.strand.nio.NioStrandServer");
        assertThat(exposing).isEmpty();
    }

    private static Class<?> load(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> publicPackageClassNames() throws Exception {
        String folder = "io/github/ppzxc/strand/nio";
        Path location = Paths.get(NioStrandServer.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        List<String> entries = new ArrayList<>();
        if (Files.isDirectory(location)) {
            try (Stream<Path> files = Files.list(location.resolve(folder))) {
                files.forEach(path -> entries.add(folder + "/" + path.getFileName()));
            }
        } else {
            try (JarFile jar = new JarFile(location.toFile())) {
                jar.stream().map(JarEntry::getName).forEach(entries::add);
            }
        }
        return entries.stream()
                .filter(name -> name.startsWith(folder + "/") && name.endsWith(".class"))
                .filter(name -> name.indexOf('/', folder.length() + 1) < 0 && !name.contains("$"))
                .map(name ->
                        name.substring(0, name.length() - ".class".length()).replace('/', '.'))
                .collect(Collectors.toList());
    }
}
