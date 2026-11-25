package io.github.ppzxc.strand.quality;

import net.sourceforge.pmd.lang.java.ast.ASTMethodDeclaration;

public final class HandlerFailureFormatRule extends HandlerFailureLogRule {
  @Override
  protected boolean isValid(ASTMethodDeclaration method) {
    return HandlerFailureLogSupport.hasApprovedMessage(method);
  }
}
