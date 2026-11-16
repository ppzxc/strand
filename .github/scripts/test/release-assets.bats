#!/usr/bin/env bats
# release.yml이 core·nio 두 모듈의 SBOM과 자산을 같은 모양으로 담는지 본다. 실행: bats .github/scripts/test

setup() {
  workflow="$BATS_TEST_DIRNAME/../../workflows/release.yml"
  collect="$BATS_TEST_DIRNAME/../collect-release-assets.sh"
}

@test "SBOM을 core와 nio 두 모듈에서 만든다" {
  grep -qE '^ +run: \./gradlew :strand-core:cyclonedxBom :strand-nio:cyclonedxBom$' "$workflow"
}

@test "워크플로가 수집 스크립트를 부르고 서명 단계가 그 디렉터리를 서명한다" {
  grep -qE '^ +\.github/scripts/collect-release-assets\.sh "\$VERSION"$' "$workflow"
  grep -qE '^ +\.\./\.github/scripts/sign-artifacts\.sh \./\*$' "$workflow"
}

@test "수집 스크립트를 실제로 돌리면 두 모듈의 jar·sources·javadoc·SBOM 8개가 모인다" {
  export VERSION=9.9.9
  cd "$BATS_TEST_TMPDIR"
  for m in strand-core strand-nio; do
    mkdir -p "$m/build/libs" "$m/build/reports/cyclonedx"
    for s in "" -sources -javadoc; do : > "$m/build/libs/$m-$VERSION$s.jar"; done
    : > "$m/build/reports/cyclonedx/bom.json"
  done
  run "$collect" "$VERSION"
  [ "$status" -eq 0 ]
  run ls release-assets
  [ "$output" = "strand-core-9.9.9-cyclonedx.json
strand-core-9.9.9-javadoc.jar
strand-core-9.9.9-sources.jar
strand-core-9.9.9.jar
strand-nio-9.9.9-cyclonedx.json
strand-nio-9.9.9-javadoc.jar
strand-nio-9.9.9-sources.jar
strand-nio-9.9.9.jar" ]
}

@test "draft Release 노트가 두 모듈을 말한다" {
  grep -qF -- '--notes "strand-core·strand-nio $VERSION.' "$workflow"
}
