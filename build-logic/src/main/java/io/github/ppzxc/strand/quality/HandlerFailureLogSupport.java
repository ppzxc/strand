package io.github.ppzxc.strand.quality;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import net.sourceforge.pmd.lang.ast.Node;
import net.sourceforge.pmd.lang.java.ast.ASTMethodCall;
import net.sourceforge.pmd.lang.java.ast.ASTMethodDeclaration;

final class HandlerFailureLogSupport {
  private HandlerFailureLogSupport() {}

  static boolean hasApprovedMessage(ASTMethodDeclaration method) {
    ASTMethodCall loggerCall = loggerCall(method);
    if (loggerCall == null || loggerCall.getArguments().size() != 3) {
      return false;
    }
    List<String> arguments = loggerCall.getArguments().toStream().toList(HandlerFailureLogSupport::source);
    Set<String> calls = method.descendants(ASTMethodCall.class).toStream()
        .map(ASTMethodCall::getMethodName)
        .collect(Collectors.toSet());
    return arguments.get(0).equals("Level.WARNING")
        && arguments.get(1).equals("\"받은 요청 핸들러가 예외를 던졌다(type=\" + request.type() + \")\"")
        && calls.stream().allMatch(Set.of("log", "type", "sanitizedFailure")::contains)
        && calls.containsAll(Set.of("log", "type"));
  }

  static boolean hasSanitizedThrowable(ASTMethodDeclaration method) {
    ASTMethodCall loggerCall = loggerCall(method);
    if (loggerCall == null || loggerCall.getArguments().size() != 3) {
      return false;
    }
    List<String> arguments = loggerCall.getArguments().toStream().toList(HandlerFailureLogSupport::source);
    return arguments.get(2).equals("sanitizedFailure(failure)");
  }

  private static ASTMethodCall loggerCall(ASTMethodDeclaration method) {
    List<ASTMethodCall> calls = method.descendants(ASTMethodCall.class)
        .filter(call -> call.getMethodName().equals("log") && "LOGGER".equals(source(call.getQualifier())))
        .toList();
    return calls.size() == 1 ? calls.get(0) : null;
  }

  private static String source(Node node) {
    return node.getTextDocument().sliceOriginalText(node.getTextRegion()).toString().replaceAll("\\s+", " ").trim();
  }
}
