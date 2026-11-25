package io.github.ppzxc.strand.quality;

import java.util.List;
import java.util.Set;
import net.sourceforge.pmd.lang.ast.Node;
import net.sourceforge.pmd.lang.java.ast.ASTCompilationUnit;
import net.sourceforge.pmd.lang.java.ast.ASTClassDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTMethodCall;
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
          && !isApprovedFormat(method)) {
        context.addViolation(method);
      }
    }
    return super.visit(compilationUnit, data);
  }

  private static String source(Node node) {
    return node.getTextDocument().sliceOriginalText(node.getTextRegion()).toString().replaceAll("\\s+", " ").trim();
  }

  private static boolean isApprovedFormat(ASTMethodDeclaration method) {
    List<ASTMethodCall> calls = method.descendants(ASTMethodCall.class).toList();
    if (calls.size() != 3) {
      return false;
    }
    ASTMethodCall loggerCall = calls.stream()
        .filter(call -> call.getMethodName().equals("log"))
        .findFirst()
        .orElse(null);
    if (loggerCall == null || !"LOGGER".equals(source(loggerCall.getQualifier()))) {
      return false;
    }
    List<String> arguments = loggerCall.getArguments().toStream().toList(HandlerFailureLoggingRule::source);
    return arguments.equals(List.of(
        "Level.WARNING",
        "\"받은 요청 핸들러가 예외를 던졌다(type=\" + request.type() + \")\"",
        "sanitizedFailure(failure)"))
        && calls.stream().map(ASTMethodCall::getMethodName).collect(java.util.stream.Collectors.toSet())
            .equals(Set.of("log", "type", "sanitizedFailure"));
  }
}
