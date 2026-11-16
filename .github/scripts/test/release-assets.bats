#!/usr/bin/env bats
# release.yml이 core·nio 두 모듈의 SBOM과 자산을 같은 모양으로 담는지 정적으로 본다. 실행: bats .github/scripts/test

setup() {
  workflow="$BATS_TEST_DIRNAME/../../workflows/release.yml"
}

@test "SBOM을 core와 nio 두 모듈에서 만든다" {
  grep -qF './gradlew :strand-core:cyclonedxBom :strand-nio:cyclonedxBom' "$workflow"
}

@test "모듈마다 jar·sources·javadoc·SBOM을 자산으로 복사한다" {
  for m in strand-core strand-nio; do
    grep -qF "\"$m/build/libs/$m-\$VERSION.jar\"" "$workflow"
    grep -qF "\"$m/build/libs/$m-\$VERSION-sources.jar\"" "$workflow"
    grep -qF "\"$m/build/libs/$m-\$VERSION-javadoc.jar\"" "$workflow"
    grep -qF "$m/build/reports/cyclonedx/bom.json \"release-assets/$m-\$VERSION-cyclonedx.json\"" "$workflow"
  done
}

@test "draft Release 노트가 두 모듈을 말한다" {
  grep -qF 'strand-core·strand-nio $VERSION' "$workflow"
}
