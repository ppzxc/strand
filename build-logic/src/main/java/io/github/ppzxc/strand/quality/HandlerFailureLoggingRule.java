package io.github.ppzxc.strand.quality;

import net.sourceforge.pmd.lang.java.ast.ASTCompilationUnit;
import net.sourceforge.pmd.lang.java.ast.ASTClassDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTMethodDeclaration;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;
import net.sourceforge.pmd.reporting.RuleContext;

public final class HandlerFailureLoggingRule extends AbstractJavaRule {
  @Override
  public Object visit(ASTCompilationUnit compilationUnit, Object data) {
    RuleContext context = (RuleContext) data;
    for (ASTMethodDeclaration method : compilationUnit.descendants(ASTMethodDeclaration.class)) {
      ASTClassDeclaration owner = method.ancestors(ASTClassDeclaration.class).first();
      if ("ConnectionEngine".equals(owner == null ? null : owner.getSimpleName())
          && "diagnoseHandlerFailure".equals(method.getName())
          && !isApprovedFormat(source(method))) {
        context.addViolation(method);
      }
    }
    return super.visit(compilationUnit, data);
  }

  private static String source(ASTMethodDeclaration method) {
    return method.getTextDocument().sliceOriginalText(method.getTextRegion()).toString();
  }

  private static boolean isApprovedFormat(String source) {
    return occurrences(source, "LOGGER.log(") == 1
        && source.contains("Level.WARNING")
        && source.contains("type=\" + request.type() + \")\"")
        && source.contains("sanitizedFailure(failure)")
        && !source.contains("remoteAddress")
        && !source.contains("payload()");
  }

  private static int occurrences(String source, String target) {
    int count = 0;
    for (int offset = 0; (offset = source.indexOf(target, offset)) >= 0; offset += target.length()) {
      count++;
    }
    return count;
  }
}
