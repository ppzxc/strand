package io.github.ppzxc.strand.quality;

import net.sourceforge.pmd.lang.java.ast.ASTMethodDeclaration;

public final class HandlerFailureThrowableRule extends HandlerFailureLogRule {
    @Override
    protected boolean isValid(ASTMethodDeclaration method) {
        return HandlerFailureLogSupport.hasSanitizedThrowable(method);
    }
}
