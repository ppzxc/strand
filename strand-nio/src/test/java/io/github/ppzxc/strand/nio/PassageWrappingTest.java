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
import org.junit.jupiter.api.Test;

/**
 * 제품 엔진이 연 통로를 테스트가 감쌀 수 있는 테스트 전용 지점({@link PassageWrapping})을 확인한다. 지점은 {@code internal}에만 있고 공개 API에는 나타나지 않는다.
 */
class PassageWrappingTest {

    /** 쓴 바이트를 세는 통로. 감싼 통로 위에서도 연결이 그대로 도는지 보려고 모든 호출을 그대로 넘긴다. */
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

    private static RequestResult echoOnce(AppFixture fixture) throws Exception {
        NioStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        NioStrandClient client = fixture.clientBuilder().build();
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

    @Test
    @SuppressWarnings("try")
    void theEngineRunsEachSideOnThePassageTheWrapperReturns() throws Exception {
        List<PassageWrapping.Side> sides = new CopyOnWriteArrayList<>();
        List<CountingPassage> wrapped = new CopyOnWriteArrayList<>();
        try (AppFixture fixture = AppFixture.plain();
                PassageWrapping.Installation installed = PassageWrapping.install((side, passage) -> {
                    sides.add(side);
                    CountingPassage counting = new CountingPassage(passage);
                    wrapped.add(counting);
                    return counting;
                })) {
            RequestResult result = echoOnce(fixture);

            assertThat(result.status()).isEqualTo(Status.OK);
            assertThat(sides).containsExactlyInAnyOrder(PassageWrapping.Side.CLIENT, PassageWrapping.Side.SERVER);
            assertThat(wrapped)
                    .hasSize(2)
                    .allSatisfy(passage -> assertThat(passage.written.get()).isPositive());
        }
    }

    @Test
    void connectionsOpenedAfterTheInstallationIsClosedAreNotWrapped() throws Exception {
        List<PassageWrapping.Side> sides = new CopyOnWriteArrayList<>();
        try (AppFixture fixture = AppFixture.plain()) {
            PassageWrapping.install((side, passage) -> {
                        sides.add(side);
                        return passage;
                    })
                    .close();

            assertThat(echoOnce(fixture).status()).isEqualTo(Status.OK);

            assertThat(sides).isEmpty();
        }
    }

    @Test
    @SuppressWarnings("try")
    void tlsPassagesAreLeftUnwrappedBecauseTheEngineQueriesThemDirectly() throws Exception {
        List<PassageWrapping.Side> sides = new CopyOnWriteArrayList<>();
        try (AppFixture fixture = AppFixture.tls();
                PassageWrapping.Installation installed = PassageWrapping.install((side, passage) -> {
                    sides.add(side);
                    return passage;
                })) {
            assertThat(echoOnce(fixture).status()).isEqualTo(Status.OK);

            assertThat(sides).isEmpty();
        }
    }

    /** 통로를 감싸는 지점은 공개 패키지의 public 타입·메서드 어디에도 드러나지 않는다. */
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

    /** 공개 패키지({@code io.github.ppzxc.strand.nio})의 클래스 이름. 컴파일 결과가 디렉터리든 jar든 읽는다. */
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
