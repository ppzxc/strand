package io.github.ppzxc.strand.quality;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.sourceforge.pmd.lang.ast.Node;
import net.sourceforge.pmd.lang.java.ast.ASTAnnotation;
import net.sourceforge.pmd.lang.java.ast.ASTBodyDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTCompilationUnit;
import net.sourceforge.pmd.lang.java.ast.ASTMemberValue;
import net.sourceforge.pmd.lang.java.ast.ASTStringLiteral;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;
import net.sourceforge.pmd.reporting.RuleContext;

public final class PolicyExceptionSuppressionRule extends AbstractJavaRule {
    private static final String POLICY_EXCEPTION = "PolicyException";
    private static final String POLICY_EXCEPTIONS = "PolicyExceptions";
    private static final String SUPPRESS_WARNINGS = "SuppressWarnings";

    @Override
    public Object visit(ASTCompilationUnit compilationUnit, Object data) {
        Set<ASTBodyDeclaration> declarations = compilationUnit
                .descendants(ASTAnnotation.class)
                .toStream()
                .filter(annotation -> annotation.getSimpleName().equals(SUPPRESS_WARNINGS)
                        || annotation.getSimpleName().equals(POLICY_EXCEPTION)
                        || annotation.getSimpleName().equals(POLICY_EXCEPTIONS))
                .map(PolicyExceptionSuppressionRule::enclosingDeclaration)
                .filter(declaration -> declaration != null)
                .collect(Collectors.toSet());

        for (ASTBodyDeclaration declaration : declarations) {
            checkDeclaration(declaration, (RuleContext) data);
        }
        return super.visit(compilationUnit, data);
    }

    private static void checkDeclaration(ASTBodyDeclaration declaration, RuleContext context) {
        List<ASTAnnotation> annotations =
                declaration.descendants(ASTAnnotation.class).toList();
        Map<String, Integer> suppressions = new HashMap<>();
        Map<String, Integer> exceptions = new HashMap<>();
        boolean invalidException = false;

        for (ASTAnnotation annotation : annotations) {
            if (enclosingDeclaration(annotation) != declaration) {
                continue;
            }
            if (annotation.getSimpleName().equals(SUPPRESS_WARNINGS)) {
                for (ASTMemberValue value : annotation.getFlatValues()) {
                    if (value instanceof ASTStringLiteral) {
                        increment(suppressions, ((ASTStringLiteral) value).getConstValue());
                    } else {
                        increment(suppressions, "<non-literal>");
                    }
                }
            } else if (annotation.getSimpleName().equals(POLICY_EXCEPTION)) {
                String rule = stringAttribute(annotation, "rule");
                String reason = stringAttribute(annotation, "reason");
                String approvedBy = stringAttribute(annotation, "approvedBy");
                String reviewBy = stringAttribute(annotation, "reviewBy");
                if (rule == null
                        || reason == null
                        || reason.trim().isEmpty()
                        || approvedBy == null
                        || approvedBy.trim().isEmpty()
                        || reviewBy == null
                        || reviewBy.trim().isEmpty()) {
                    invalidException = true;
                }
                if (reviewBy != null) {
                    try {
                        LocalDate.parse(reviewBy);
                    } catch (RuntimeException invalidDate) {
                        invalidException = true;
                    }
                }
                if (rule != null) {
                    increment(exceptions, rule);
                }
            }
        }

        if (invalidException || !suppressions.equals(exceptions)) {
            context.addViolation(declaration);
        }
    }

    private static ASTBodyDeclaration enclosingDeclaration(ASTAnnotation annotation) {
        Node parent = annotation.getParent();
        while (parent != null && !(parent instanceof ASTBodyDeclaration)) {
            parent = parent.getParent();
        }
        return (ASTBodyDeclaration) parent;
    }

    private static String stringAttribute(ASTAnnotation annotation, String name) {
        ASTMemberValue value = annotation.getAttribute(name);
        return value instanceof ASTStringLiteral ? ((ASTStringLiteral) value).getConstValue() : null;
    }

    private static void increment(Map<String, Integer> values, String value) {
        values.put(value, values.getOrDefault(value, 0) + 1);
    }
}
