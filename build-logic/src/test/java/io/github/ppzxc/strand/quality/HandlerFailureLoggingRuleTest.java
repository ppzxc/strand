package io.github.ppzxc.strand.quality;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.sourceforge.pmd.PMDConfiguration;
import net.sourceforge.pmd.PmdAnalysis;
import net.sourceforge.pmd.lang.document.FileId;
import net.sourceforge.pmd.reporting.Report;
import org.junit.jupiter.api.Test;

class HandlerFailureLoggingRuleTest {
  @Test
  void approvedSanitizedHandlerLogPasses() {
    String source = handler("LOGGER.log(Level.WARNING, \"받은 요청 핸들러가 예외를 던졌다(type=\" + request.type() + \")\", sanitizedFailure(failure));");
    assertEquals(0, violations(source, "LOG-001"));
    assertEquals(0, violations(source, "LOG-002"));
  }

  @Test
  void addressAndOriginalThrowableInHandlerLogFail() {
    String source = handler("LOGGER.log(Level.WARNING, \"받은 요청 핸들러가 예외를 던졌다(type=\" + request.type() + \", remote=\" + p.remoteAddress + \")\", failure);");
    assertEquals(1, violations(source, "LOG-001"));
    assertEquals(1, violations(source, "LOG-002"));
  }

  @Test
  void requestPayloadInHandlerLogFails() {
    String source = handler("LOGGER.log(Level.WARNING, \"받은 요청 핸들러가 예외를 던졌다(type=\" + request.type() + \", payload=\" + request.payload() + \")\", sanitizedFailure(failure));");
    assertEquals(1, violations(source, "LOG-001"));
  }

  @Test
  void exceptionMessageAppendedToOtherwiseApprovedLogFails() {
    String source = handler("LOGGER.log(Level.WARNING, \"받은 요청 핸들러가 예외를 던졌다(type=\" + request.type() + \")\" + \" \" + failure.getMessage(), sanitizedFailure(failure));");
    assertEquals(1, violations(source, "LOG-001"));
  }

  @Test
  void requestSerializationAppendedToOtherwiseApprovedLogFails() {
    String source = handler("LOGGER.log(Level.WARNING, \"받은 요청 핸들러가 예외를 던졌다(type=\" + request.type() + \")\" + \" \" + request.toString(), sanitizedFailure(failure));");
    assertEquals(1, violations(source, "LOG-001"));
  }

  private static String handler(String loggingCall) {
    return "class ConnectionEngine {\n"
        + " void diagnoseHandlerFailure(Request request, Throwable failure, Params p) {\n"
        + "  " + loggingCall + "\n"
        + " }\n"
        + "}\n";
  }

  private static long violations(String source, String ruleName) {
    return report(source).getViolations().stream().filter(violation -> violation.getRule().getName().equals(ruleName)).count();
  }

  private static Report report(String source) {
    PMDConfiguration configuration = new PMDConfiguration();
    try (PmdAnalysis pmd = PmdAnalysis.create(configuration)) {
      pmd.addRuleSet(pmd.newRuleSetLoader().loadFromResource("handler-logging-ruleset.xml"));
      pmd.files().addSourceFile(FileId.fromPathLikeString("ConnectionEngine.java"), source);
      return pmd.performAnalysisAndCollectReport();
    }
  }
}
