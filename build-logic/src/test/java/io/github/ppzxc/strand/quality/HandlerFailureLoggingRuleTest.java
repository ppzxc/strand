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
    assertEquals(0, violations(handler("LOGGER.log(Level.WARNING, \"handler failed(type=\" + request.type() + \")\", sanitizedFailure(failure));")));
  }

  @Test
  void addressAndOriginalThrowableInHandlerLogFail() {
    assertEquals(1, violations(handler("LOGGER.log(Level.WARNING, \"handler failed(type=\" + request.type() + \", remote=\" + p.remoteAddress + \")\", failure);")));
  }

  @Test
  void requestPayloadInHandlerLogFails() {
    assertEquals(1, violations(handler("LOGGER.log(Level.WARNING, \"handler failed(type=\" + request.type() + \", payload=\" + request.payload() + \")\", sanitizedFailure(failure));")));
  }

  private static String handler(String loggingCall) {
    return "class ConnectionEngine {\n"
        + " void diagnoseHandlerFailure(Request request, Throwable failure, Params p) {\n"
        + "  " + loggingCall + "\n"
        + " }\n"
        + "}\n";
  }

  private static int violations(String source) {
    return report(source).getViolations().size();
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
