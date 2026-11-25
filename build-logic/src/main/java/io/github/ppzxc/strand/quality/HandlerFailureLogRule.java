package io.github.ppzxc.strand.quality;

import net.sourceforge.pmd.lang.java.ast.ASTClassDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTCompilationUnit;
import net.sourceforge.pmd.lang.java.ast.ASTMethodDeclaration;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;
import net.sourceforge.pmd.reporting.RuleContext;

abstract class HandlerFailureLogRule extends AbstractJavaRule {
    protected abstract boolean isValid(ASTMethodDeclaration method);

    @Override
    public Object visit(ASTCompilationUnit compilationUnit, Object data) {
        RuleContext context = (RuleContext) data;
        for (ASTMethodDeclaration method : compilationUnit.descendants(ASTMethodDeclaration.class)) {
            ASTClassDeclaration owner =
                    method.ancestors(ASTClassDeclaration.class).first();
            if ("ConnectionEngine".equals(owner == null ? null : owner.getSimpleName())
                    && "diagnoseHandlerFailure".equals(method.getName())
                    && !isValid(method)) {
                context.addViolation(method);
            }
        }
        return super.visit(compilationUnit, data);
    }
}
