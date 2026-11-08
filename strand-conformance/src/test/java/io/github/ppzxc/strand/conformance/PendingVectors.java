package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 아직 구현하지 않은 동작에 기대 core로 실패하는 벡터의 목록({@code pending-vectors.txt}). 한 줄에 {@code 파일명/id} 하나이고
 * 이름순으로 정렬한다. 줄 하나가 벡터 하나라서, 여러 티켓이 자기 벡터를 지워도 변경이 서로 겹치지 않는다. {@code #}으로 시작하는
 * 줄과 빈 줄은 건너뛴다.
 */
final class PendingVectors {

    static final String RESOURCE = "/pending-vectors.txt";

    private PendingVectors() {}

    /** 목록을 읽는다. 정렬되어 있지 않거나 같은 항목이 두 번 있으면 실패한다. */
    static Set<String> load() {
        List<String> lines = new ArrayList<>();
        try (InputStream in = PendingVectors.class.getResourceAsStream(RESOURCE)) {
            assertThat(in).as("테스트 리소스 %s", RESOURCE).isNotNull();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    lines.add(trimmed);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<String> sorted = new ArrayList<>(lines);
        Collections.sort(sorted);
        assertThat(lines).as("pending-vectors.txt는 이름순이다").isEqualTo(sorted);
        Set<String> unique = new LinkedHashSet<>(lines);
        assertThat(unique).as("pending-vectors.txt에 같은 항목이 두 번 있다").hasSameSizeAs(lines);
        return unique;
    }
}
