package io.github.ppzxc.strand.quality;

import java.util.List;
import net.sourceforge.pmd.lang.ast.Node;
import net.sourceforge.pmd.lang.java.ast.ASTCompilationUnit;
import net.sourceforge.pmd.lang.java.ast.ASTFieldDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTTypeDeclaration;
import net.sourceforge.pmd.lang.java.ast.JavaComment;
import net.sourceforge.pmd.lang.java.ast.JavadocComment;
import net.sourceforge.pmd.lang.java.ast.JavadocCommentOwner;
import net.sourceforge.pmd.lang.java.ast.ModifierOwner;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;
import net.sourceforge.pmd.reporting.RuleContext;

public final class AllowedJavaCommentsRule extends AbstractJavaRule {
    @Override
    public Object visit(ASTCompilationUnit compilationUnit, Object data) {
        List<JavaComment> comments = compilationUnit.getComments();
        for (int index = 0; index < comments.size(); index++) {
            JavaComment comment = comments.get(index);
            if (allowed(comment, index, compilationUnit)) {
                continue;
            }
            RuleContext context = (RuleContext) data;
            context.addViolationWithPosition(
                    comment,
                    compilationUnit.getAstInfo(),
                    comment.getReportLocation(),
                    "Only a leading license notice or Javadoc on an exposed declaration is allowed.");
        }
        return super.visit(compilationUnit, data);
    }

    private static boolean allowed(JavaComment comment, int index, ASTCompilationUnit root) {
        if (comment instanceof JavadocComment) {
            JavadocCommentOwner owner = ((JavadocComment) comment).getOwner();
            if (owner == null) {
                return false;
            }
            if (owner instanceof net.sourceforge.pmd.lang.java.ast.ASTPackageDeclaration) {
                return root.getAstInfo()
                        .getTextDocument()
                        .getFileId()
                        .getFileName()
                        .equals("package-info.java");
            }
            return isVisibleDeclaration(owner) && allEnclosingTypesVisible((Node) owner);
        }
        return index == 0 && isLeadingLicense(comment, root);
    }

    private static boolean isVisibleDeclaration(JavadocCommentOwner owner) {
        if (owner instanceof ModifierOwner) {
            ModifierOwner.Visibility visibility = ((ModifierOwner) owner).getVisibility();
            return visibility == ModifierOwner.Visibility.V_PUBLIC
                    || visibility == ModifierOwner.Visibility.V_PROTECTED;
        }
        if (owner instanceof ASTFieldDeclaration) {
            ASTFieldDeclaration field = (ASTFieldDeclaration) owner;
            net.sourceforge.pmd.lang.java.ast.ASTModifierList modifiers =
                    field.firstChild(net.sourceforge.pmd.lang.java.ast.ASTModifierList.class);
            return modifiers != null
                            && (modifiers.hasAny(net.sourceforge.pmd.lang.java.ast.JModifier.PUBLIC)
                                    || modifiers.hasAny(net.sourceforge.pmd.lang.java.ast.JModifier.PROTECTED))
                    || field.getEnclosingType() != null
                            && (field.getEnclosingType().isInterface()
                                    || field.getEnclosingType().isAnnotation());
        }
        return owner instanceof ASTTypeDeclaration;
    }

    private static boolean allEnclosingTypesVisible(Node node) {
        ASTTypeDeclaration type = node instanceof ASTTypeDeclaration
                ? (ASTTypeDeclaration) node
                : node instanceof net.sourceforge.pmd.lang.java.ast.JavaNode
                        ? ((net.sourceforge.pmd.lang.java.ast.JavaNode) node).getEnclosingType()
                        : null;
        while (type != null) {
            ModifierOwner.Visibility visibility = type.getVisibility();
            if (visibility != ModifierOwner.Visibility.V_PUBLIC && visibility != ModifierOwner.Visibility.V_PROTECTED) {
                return false;
            }
            type = type.getEnclosingType();
        }
        return true;
    }

    private static boolean isLeadingLicense(JavaComment comment, ASTCompilationUnit root) {
        if (!root.getAstInfo()
                .getTextDocument()
                .getText()
                .subSequence(0, comment.getToken().getRegion().getStartOffset())
                .toString()
                .isBlank()) {
            return false;
        }
        String text = comment.getText().toString().toLowerCase(java.util.Locale.ROOT);
        return text.contains("copyright")
                || text.contains("license")
                || text.contains("spdx-license-identifier")
                || text.contains("all rights reserved");
    }
}
