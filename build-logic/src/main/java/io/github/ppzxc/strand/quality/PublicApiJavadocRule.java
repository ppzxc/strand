package io.github.ppzxc.strand.quality;

import java.util.List;
import net.sourceforge.pmd.lang.ast.Node;
import net.sourceforge.pmd.lang.java.ast.ASTCompilationUnit;
import net.sourceforge.pmd.lang.java.ast.ASTConstructorDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTFieldDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTMethodDeclaration;
import net.sourceforge.pmd.lang.java.ast.ASTTypeDeclaration;
import net.sourceforge.pmd.lang.java.ast.JavadocCommentOwner;
import net.sourceforge.pmd.lang.java.ast.ModifierOwner;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;
import net.sourceforge.pmd.reporting.RuleContext;

public final class PublicApiJavadocRule extends AbstractJavaRule {
    @Override
    public Object visit(ASTCompilationUnit compilationUnit, Object data) {
        String path = compilationUnit.getAstInfo().getTextDocument().getFileId().getOriginalPath();
        if (!isPublishedSource(path) || compilationUnit.getPackageName().contains(".internal")) {
            return super.visit(compilationUnit, data);
        }

        List<JavadocCommentOwner> declarations = new java.util.ArrayList<>();
        declarations.addAll(
                compilationUnit.descendants(ASTTypeDeclaration.class).toList());
        declarations.addAll(
                compilationUnit.descendants(ASTMethodDeclaration.class).toList());
        declarations.addAll(
                compilationUnit.descendants(ASTConstructorDeclaration.class).toList());
        declarations.addAll(
                compilationUnit.descendants(ASTFieldDeclaration.class).toList());
        RuleContext context = (RuleContext) data;
        for (JavadocCommentOwner declaration : declarations) {
            if (isExposed(declaration) && declaration.getJavadocComment() == null) {
                context.addViolationWithMessage((Node) declaration, "Public API declaration requires Javadoc.");
            }
        }
        return super.visit(compilationUnit, data);
    }

    private static boolean isPublishedSource(String path) {
        return (path.contains("strand-api/src/main/java/")
                        || path.contains("strand-core/src/main/java/")
                        || path.contains("strand-nio/src/main/java/")
                        || path.contains("strand-netty/src/main/java/"))
                && path.contains("src/main/java/");
    }

    private static boolean isExposed(JavadocCommentOwner declaration) {
        if (declaration instanceof ASTTypeDeclaration) {
            ASTTypeDeclaration type = (ASTTypeDeclaration) declaration;
            return visible(type) && allEnclosingTypesVisible(type.getEnclosingType());
        }
        if (declaration instanceof ModifierOwner) {
            ModifierOwner owner = (ModifierOwner) declaration;
            return visible(owner)
                    && allEnclosingTypesVisible(
                            ((net.sourceforge.pmd.lang.java.ast.JavaNode) owner).getEnclosingType());
        }
        if (declaration instanceof ASTFieldDeclaration) {
            ASTFieldDeclaration field = (ASTFieldDeclaration) declaration;
            net.sourceforge.pmd.lang.java.ast.ASTModifierList modifiers =
                    field.firstChild(net.sourceforge.pmd.lang.java.ast.ASTModifierList.class);
            return ((modifiers != null
                                    && (modifiers.hasAny(net.sourceforge.pmd.lang.java.ast.JModifier.PUBLIC)
                                            || modifiers.hasAny(net.sourceforge.pmd.lang.java.ast.JModifier.PROTECTED)))
                            || field.getEnclosingType() != null
                                    && (field.getEnclosingType().isInterface()
                                            || field.getEnclosingType().isAnnotation()))
                    && allEnclosingTypesVisible(field.getEnclosingType());
        }
        return false;
    }

    private static boolean visible(ModifierOwner owner) {
        ModifierOwner.Visibility visibility = owner.getVisibility();
        return visibility == ModifierOwner.Visibility.V_PUBLIC || visibility == ModifierOwner.Visibility.V_PROTECTED;
    }

    private static boolean allEnclosingTypesVisible(ASTTypeDeclaration type) {
        while (type != null) {
            if (!visible(type)) {
                return false;
            }
            type = type.getEnclosingType();
        }
        return true;
    }
}
