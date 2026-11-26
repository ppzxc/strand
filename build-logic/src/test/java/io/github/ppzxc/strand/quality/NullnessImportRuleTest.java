package io.github.ppzxc.strand.quality;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.sourceforge.pmd.PMDConfiguration;
import net.sourceforge.pmd.PmdAnalysis;
import net.sourceforge.pmd.lang.document.FileId;
import net.sourceforge.pmd.reporting.Report;
import org.junit.jupiter.api.Test;

class NullnessImportRuleTest {
    @Test
    void allowsJSpecifyNullnessImportsAndUnrelatedAnnotations() {
        assertEquals(
                0,
                violations("import org.jspecify.annotations.NullMarked;\n"
                        + "import org.jspecify.annotations.Nullable;\n"
                        + "import javax.annotation.processing.Generated;\n"
                        + "class Sample {}\n"));
    }

    @Test
    void rejectsLegacyNullableAndNotNullImports() {
        assertEquals(
                3,
                violations("import javax.annotation.Nullable;\n"
                        + "import org.jetbrains.annotations.NotNull;\n"
                        + "import org.springframework.lang.Nullable;\n"
                        + "class Sample {}\n"));
    }

    private static long violations(String source) {
        return report(source).getViolations().stream()
                .filter(violation -> violation.getRule().getName().equals("NUL-002"))
                .count();
    }

    private static Report report(String source) {
        PMDConfiguration configuration = new PMDConfiguration();
        try (PmdAnalysis pmd = PmdAnalysis.create(configuration)) {
            pmd.addRuleSet(pmd.newRuleSetLoader().loadFromResource("java-style-ruleset.xml"));
            pmd.files().addSourceFile(FileId.fromPathLikeString("Sample.java"), source);
            return pmd.performAnalysisAndCollectReport();
        }
    }
}
