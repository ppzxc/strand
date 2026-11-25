package io.github.ppzxc.strand.quality;

import java.util.Set;
import net.sourceforge.pmd.lang.java.ast.ASTImportDeclaration;
import net.sourceforge.pmd.lang.java.rule.AbstractJavaRule;
import net.sourceforge.pmd.reporting.RuleContext;

/** Requires JSpecify for nullness annotations. */
public final class NullnessImportRule extends AbstractJavaRule {
  private static final Set<String> NULLNESS_ANNOTATIONS =
      Set.of(
          "CheckForNull",
          "NonNull",
          "Nonnull",
          "NonNullApi",
          "NonNullFields",
          "NotNull",
          "NullMarked",
          "NullUnmarked",
          "Nullable",
          "ParametersAreNonnullByDefault",
          "UnknownNullability");

  @Override
  public Object visit(ASTImportDeclaration declaration, Object data) {
    String importedName = declaration.getImportedName();
    String simpleName = declaration.getImportedSimpleName();
    boolean jspecify = importedName.startsWith("org.jspecify.annotations.");
    boolean knownNullnessType = simpleName != null && NULLNESS_ANNOTATIONS.contains(simpleName);

    if (!jspecify && knownNullnessType) {
      ((RuleContext) data).addViolation(declaration);
    }
    return super.visit(declaration, data);
  }
}
