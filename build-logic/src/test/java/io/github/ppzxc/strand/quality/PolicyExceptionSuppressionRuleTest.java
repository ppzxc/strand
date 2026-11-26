package io.github.ppzxc.strand.quality;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.sourceforge.pmd.PMDConfiguration;
import net.sourceforge.pmd.PmdAnalysis;
import net.sourceforge.pmd.lang.document.FileId;
import net.sourceforge.pmd.reporting.Report;
import org.junit.jupiter.api.Test;

class PolicyExceptionSuppressionRuleTest {
    @Test
    void matchingApprovalOnSameDeclarationPasses() {
        assertEquals(
                0,
                violations("@SuppressWarnings(\"PMD.Rule\")\n"
                        + "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Rule\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\")\n"
                        + "class Example {}"));
    }

    @Test
    void missingApprovalFails() {
        assertEquals(1, violations("@SuppressWarnings(\"PMD.Rule\") class Example {}"));
    }

    @Test
    void mismatchedApprovalFails() {
        assertEquals(
                2,
                violations("@SuppressWarnings(\"PMD.Rule\")\n"
                        + "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Other\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\")\n"
                        + "class Example {}"));
    }

    @Test
    void everySuppressionMustHaveExactlyOneApproval() {
        assertEquals(
                1,
                violations("@SuppressWarnings({\"PMD.Rule\", \"PMD.Other\"})\n"
                        + "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Rule\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\")\n"
                        + "class Example {}"));
    }

    @Test
    void duplicateApprovalsForOneSuppressionFail() {
        assertEquals(
                1,
                violations("@SuppressWarnings(\"PMD.Rule\")\n"
                        + "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Rule\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\")\n"
                        + "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Rule\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\")\n"
                        + "class Example {}"));
    }

    @Test
    void unusedApprovalFails() {
        assertEquals(
                1,
                violations(
                        "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Rule\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\") class Example {}"));
    }

    @Test
    void sameLineSuppressionFindingsRemainDistinct() {
        assertEquals(2, violations("@SuppressWarnings({\"PMD.Rule\", \"PMD.Other\"}) class Example {}"));
    }

    @Test
    void multipleSuppressionValuesCanHaveOneApprovalEach() {
        assertEquals(
                0,
                violations("@SuppressWarnings({\"PMD.Rule\", \"PMD.Other\"})\n"
                        + "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Rule\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\")\n"
                        + "@io.github.ppzxc.strand.api.PolicyException(rule=\"PMD.Other\", reason=\"needed\", approvedBy=\"reviewer\", reviewBy=\"2099-12-31\")\n"
                        + "class Example {}"));
    }

    @Test
    void violationsStayAtBlockingPriority() {
        assertEquals(
                1,
                report("@SuppressWarnings(\"PMD.Rule\") class Example {}")
                        .getViolations()
                        .get(0)
                        .getRule()
                        .getPriority()
                        .getPriority());
    }

    private static int violations(String source) {
        return report(source).getViolations().size();
    }

    private static Report report(String source) {
        PMDConfiguration configuration = new PMDConfiguration();
        try (PmdAnalysis pmd = PmdAnalysis.create(configuration)) {
            pmd.addRuleSet(pmd.newRuleSetLoader().loadFromResource("policy-exception-ruleset.xml"));
            pmd.files().addSourceFile(FileId.fromPathLikeString("Example.java"), source);
            return pmd.performAnalysisAndCollectReport();
        }
    }
}
