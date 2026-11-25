package io.github.ppzxc.strand.quality;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.sourceforge.pmd.PMDConfiguration;
import net.sourceforge.pmd.PmdAnalysis;
import net.sourceforge.pmd.lang.document.FileId;
import net.sourceforge.pmd.reporting.Report;
import org.junit.jupiter.api.Test;

class JavaStyleRulesTest {
    @Test
    void allowsLicenseAndPublicApiJavadocs() {
        assertEquals(
                0,
                violations(
                        "Sample.java",
                        "/* SPDX-License-Identifier: Apache-2.0 */\n"
                                + "public class Sample {\n"
                                + "  /** Returns a value. */ public String value() { return \"ok\"; }\n"
                                + "}\n"));
    }

    @Test
    void rejectsImplementationCommentsAndSpotlessToggles() {
        assertEquals(1, violations("Sample.java", "public class Sample { // TODO\n}\n"));
        assertEquals(
                2,
                violations(
                        "Sample.java",
                        "public class Sample {\n" + "  // spotless:off\n" + "  // spotless:on\n" + "}\n"));
    }

    @Test
    void requiresJavadocOnPublicApiDeclarations() {
        assertEquals(
                2,
                violations(
                        "strand-api/src/main/java/p/Sample.java",
                        "package p;\n"
                                + "public class Sample {\n"
                                + "  public String value() { return \"ok\"; }\n"
                                + "}\n"));
    }

    @Test
    void excludesInternalPackagesFromPublicApiJavadocRule() {
        assertEquals(
                0,
                violations(
                        "strand-core/src/main/java/p/internal/Sample.java",
                        "package p.internal;\npublic class Sample {}\n"));
    }

    @Test
    void requiresJavadocForImplicitlyPublicInterfaceFields() {
        assertEquals(
                2,
                violations(
                        "strand-api/src/main/java/p/Sample.java",
                        "package p;\npublic interface Sample { String VALUE = \"ok\"; }\n"));
    }

    @Test
    void allowsPackageInfoJavadoc() {
        assertEquals(
                0,
                violations(
                        "strand-api/src/main/java/p/package-info.java",
                        "/** Public package documentation. */\npackage p;\n"));
    }

    @Test
    void rejectsWildcardImportsAndBadNames() {
        assertEquals(
                3,
                violations(
                        "Sample.java",
                        "import java.util.*;\n" + "class badName {\n" + "  void bad_method() {}\n" + "}\n"));
    }

    @Test
    void rejectsStaticWildcardImports() {
        assertEquals(1, violations("Sample.java", "import static java.util.Collections.*;\nclass Sample {}\n"));
    }

    private static int violations(String path, String source) {
        return report(path, source).getViolations().size();
    }

    private static Report report(String path, String source) {
        PMDConfiguration configuration = new PMDConfiguration();
        try (PmdAnalysis pmd = PmdAnalysis.create(configuration)) {
            pmd.addRuleSet(pmd.newRuleSetLoader().loadFromResource("java-style-ruleset.xml"));
            pmd.files().addSourceFile(FileId.fromPathLikeString(path), source);
            return pmd.performAnalysisAndCollectReport();
        }
    }
}
