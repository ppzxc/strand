package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** 연결 인증 관문(AUTH)과 채널 요구가 규격, 형식 문서, 스키마, 용어집에 같은 모습으로 들어 있는지 본다(#166). core를 실행하지 않는다. */
class AuthSpecTest {

    private static final Path ROOT =
            VectorSuite.defaultDirectory().resolve("../..").normalize();
    private static final Path SPEC = ROOT.resolve("docs/specification/STRAND_SPECIFICATION.md");
    private static final Path FORMAT = ROOT.resolve("docs/specification/STRAND_CONFORMANCE_VECTORS.md");
    private static final Path SCHEMA = ROOT.resolve("strand-conformance/schema/vector-format-2.schema.json");
    private static final Path GLOSSARY = ROOT.resolve("GLOSSARY.md");

    private static final Pattern HEADING = Pattern.compile("^(#{2,4}) (\\d+(?:\\.\\d+)*)\\.?\\s(.*)$");

    private static String read(Path file) throws IOException {
        return String.join("\n", Files.readAllLines(file, StandardCharsets.UTF_8));
    }

    /** 절 번호에서 그 절의 본문(제목 줄부터 다음 제목 줄 앞까지)으로. 하위 절의 본문은 포함하지 않는다. */
    private static Map<String, String> sections() throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        String current = null;
        StringBuilder body = new StringBuilder();
        for (String line : Files.readAllLines(SPEC, StandardCharsets.UTF_8)) {
            Matcher m = HEADING.matcher(line);
            if (m.matches()) {
                if (current != null) {
                    result.put(current, body.toString());
                }
                current = m.group(2);
                body = new StringBuilder();
            }
            body.append(line).append('\n');
        }
        result.put(current, body.toString());
        return result;
    }

    private static String section(String number) throws IOException {
        String text = sections().get(number);
        assertThat(text).as("%s절", number).isNotNull();
        return text;
    }

    /** 이 문자열이 본문에 나오는 절 번호들. */
    private static List<String> sectionsContaining(String needle) throws IOException {
        List<String> found = new java.util.ArrayList<>();
        for (Map.Entry<String, String> e : sections().entrySet()) {
            if (e.getValue().contains(needle)) {
                found.add(e.getKey());
            }
        }
        return found;
    }

    // ---- 규격 ----

    @Test
    void authFrameTypeIsDefinedOnceAndTheReservedRangeMovesUp() throws IOException {
        String spec = read(SPEC);
        assertThat(section("3.3.1")).contains("AUTH").contains("`0x0005`");
        assertThat(section("2.2.2")).contains("`0x0000`").contains("`0x0006`");
        assertThat(spec).doesNotContain("`0x0005`\\~`0x00FF`");
        assertThat(sectionsContaining("mechanism_length")).containsExactly("3.3.1");
    }

    @Test
    void authSectionDefinesPayloadsMechanismsOutcomesAndTheSaslDisclaimer() throws IOException {
        String auth = section("3.3.1");
        assertThat(auth)
                .contains("mechanism_length")
                .contains("credentials")
                .contains("credential_lifetime")
                .contains("`0x0001`")
                .contains("ANONYMOUS")
                .contains("SASL")
                .contains("프로필")
                .contains("UNAUTHENTICATED")
                .contains("INTERNAL_ERROR")
                .contains("CLOSE(NO_ERROR)")
                .contains("[^RFC4422]");
        // 단일 왕복이 되는 메커니즘과 안 되는 메커니즘의 비규범 표
        assertThat(auth).contains("PLAIN", "EXTERNAL", "OAUTHBEARER", "SCRAM", "GSSAPI");
    }

    @Test
    void establishmentSectionPutsTheAuthGateOnTopOfTheAckGate() throws IOException {
        String establishment = section("3.3");
        assertThat(establishment)
                .contains("AUTH 관문")
                .contains("CLOSE(UNAUTHENTICATED)")
                .contains("AUTH(ACK)")
                .contains("HELLO·AUTH·CLOSE")
                .contains("sequenceDiagram");
    }

    @Test
    void closeSectionAndAuthSectionAgreeOnWhenUnauthenticatedIsSent() throws IOException {
        // 3.5절은 UNAUTHENTICATED를 보내는 조건을 직접 정하지 않고 정의한 절을 가리킨다(#163 후속).
        String close = section("3.5");
        assertThat(close).contains("UNAUTHENTICATED").contains("3.3.1절").contains("자격 증명을 폐기했다면");
    }

    @Test
    void connectionErrorSetListsTheAuthItems() throws IOException {
        String checks = section("5.1");
        assertThat(checks).contains("AUTH 관문").contains("AUTH 형태").contains("`0x0006`");
        assertThat(checks).doesNotContain("`0x0005`\\~`0x00FF`");
    }

    @Test
    void streamIdZeroListAndFlagsTableIncludeAuth() throws IOException {
        assertThat(section("2.2.5")).contains("HELLO·AUTH·PING·CLOSE");
        assertThat(section("2.3")).contains("HELLO, PING, AUTH");
        assertThat(section("1.3")).contains("| **확인 대기 (Ack Wait)** | HELLO·PING·AUTH");
    }

    @Test
    void channelRequirementsReplaceTheNoProtectionSection() throws IOException {
        String channel = section("6.1");
        assertThat(channel.split("\n")[0]).contains("채널 요구");
        assertThat(read(SPEC)).doesNotContain("보호 수단 없음");
        assertThat(channel)
                .contains("기밀성")
                .contains("무결성")
                .contains("서버 인증")
                .contains("TLS 1.3")
                .contains("BCP 195")
                .contains("ALPN")
                .contains("`strand`")
                .contains("1.2.1절")
                .contains("[^RFC8446]")
                .contains("[^RFC9325]")
                .contains("[^RFC7301]");
        // 조건 없는 MUST: 신뢰할 수 있는 네트워크라는 예외를 두지 않는다.
        assertThat(channel).contains("(MUST)").doesNotContain("신뢰할 수 없는 네트워크");
    }

    @Test
    void subprotocolRuleIsDefinedOnlyInSection121() throws IOException {
        String sub = section("1.2.1");
        assertThat(sub).contains("Sec-WebSocket-Protocol: strand").contains("MUST");
        assertThat(sectionsContaining("Sec-WebSocket-Protocol")).containsExactly("1.2.1");
        assertThat(sectionsContaining("ALPN")).containsExactly("6.1");
    }

    @Test
    void scopeSentenceReplacesTheProtectionDisclaimer() throws IOException {
        String scope = section("1.1");
        assertThat(scope).contains("오래 유지되는 단일 연결").doesNotContain("연결을 어떻게 보호할지");
    }

    @Test
    void termsDefineConnectionAuthenticationAndAuthenticatedPeer() throws IOException {
        String terms = section("1.3");
        assertThat(terms).contains("**연결 인증 (Connection Authentication)**");
        assertThat(terms).contains("**인증된 상대 (Authenticated Peer)**");
        assertThat(section("6.2")).contains("인증된 상대").contains("1.3절");
    }

    @Test
    void everyReferenceIsCitedAndTheFourNewOnesAreDefined() throws IOException {
        String spec = read(SPEC);
        for (String tag : new String[] {"RFC4422", "RFC7301", "RFC8446", "RFC9325"}) {
            assertThat(spec).contains("[^" + tag + "]:");
        }
        Matcher def =
                Pattern.compile("^\\[\\^([A-Za-z0-9-]+)\\]:", Pattern.MULTILINE).matcher(spec);
        int count = 0;
        while (def.find()) {
            count++;
            String tag = def.group(1);
            String references = spec.replaceAll("(?m)^\\[\\^" + tag + "\\]:.*$", "");
            assertThat(references).as("인용하지 않는 각주 %s", tag).contains("[^" + tag + "]");
        }
        assertThat(count).isGreaterThanOrEqualTo(9);
    }

    // ---- 용어집 ----

    @Test
    void glossaryPointsTheTwoTermsToSection13() throws IOException {
        String glossary = read(GLOSSARY);
        for (String term : new String[] {"**연결 인증 (Connection Authentication)**", "**인증된 상대 (Authenticated Peer)**"}) {
            int at = glossary.indexOf(term);
            assertThat(at).as(term).isNotNegative();
            String entry = glossary.substring(at, glossary.indexOf("_Avoid_", at));
            assertThat(entry).contains("STRAND_SPECIFICATION.md#13-용어").doesNotContain("규격에 추가 예정");
        }
    }

    // ---- 벡터 형식 문서와 스키마 ----

    @Test
    void formatDocumentDefinesSetupAuthTheTimeoutAndTheAuthSendItem() throws IOException {
        String format = read(FORMAT);
        assertThat(format)
                .contains("| `auth` |")
                .contains("`auth_timeout_ms`")
                .contains("| `AUTH` |")
                .contains("AUTH 왕복")
                .contains("`auth`");
        // ack_timeout_ms는 AUTH(ACK) 대기도 다룬다.
        Matcher row = Pattern.compile("^\\| `ack_timeout_ms` \\|.*$", Pattern.MULTILINE)
                .matcher(format);
        assertThat(row.find()).isTrue();
        assertThat(row.group()).contains("AUTH(ACK)");
    }

    @Test
    void formatDocumentSaysEstablishedStartsAfterTheAuthRoundTripAndPingNeedsTheGate() throws IOException {
        String format = read(FORMAT);
        assertThat(format).contains("`established`가 `true`이면 러너가 표준 수립 순서(3.3절)를 실행하고");
        int precondition = format.indexOf("### 8.1 전제 조건");
        int settings = format.indexOf("### 8.2 설정 항목");
        assertThat(format.substring(precondition, settings))
                .contains("AUTH")
                .doesNotContain("상대 HELLO에 `ACK`를 보내기 전에 `ping`");
    }

    @Test
    void schemaFileKnowsTheAuthSetupTheTimeoutAndTheAuthFrame() throws IOException {
        String schema = read(SCHEMA);
        assertThat(schema)
                .contains("\"auth_timeout_ms\"")
                .contains("\"frame_auth\"")
                .contains("\"auth\"");
        assertThat(schema).contains("credential_lifetime");
    }
}
