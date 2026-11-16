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

# 스크립트는 자기 위치 기준으로 저장소 루트를 찾으므로, 임시 저장소 모양에 복사해 돌린다.
make_repo() {
  repo="$BATS_TEST_TMPDIR/repo"
  mkdir -p "$repo/.github/scripts"
  cp "$collect" "$repo/.github/scripts/"
  for m in "$@"; do
    mkdir -p "$repo/$m/build/libs" "$repo/$m/build/reports/cyclonedx"
    for s in "" -sources -javadoc; do : > "$repo/$m/build/libs/$m-9.9.9$s.jar"; done
    echo "sbom of $m" > "$repo/$m/build/reports/cyclonedx/bom.json"
  done
}

@test "수집 스크립트를 실제로 돌리면 두 모듈의 jar·sources·javadoc·SBOM 8개가 모인다" {
  make_repo strand-core strand-nio
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -eq 0 ]
  run ls "$repo/release-assets"
  [ "$output" = "strand-core-9.9.9-cyclonedx.json
strand-core-9.9.9-javadoc.jar
strand-core-9.9.9-sources.jar
strand-core-9.9.9.jar
strand-nio-9.9.9-cyclonedx.json
strand-nio-9.9.9-javadoc.jar
strand-nio-9.9.9-sources.jar
strand-nio-9.9.9.jar" ]
}

@test "각 SBOM은 같은 모듈의 bom.json이다" {
  make_repo strand-core strand-nio
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -eq 0 ]
  [ "$(cat "$repo/release-assets/strand-core-9.9.9-cyclonedx.json")" = "sbom of strand-core" ]
  [ "$(cat "$repo/release-assets/strand-nio-9.9.9-cyclonedx.json")" = "sbom of strand-nio" ]
}

@test "다른 디렉터리에서 불러도 저장소 루트에 모은다" {
  make_repo strand-core strand-nio
  cd /
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -eq 0 ]
  [ -f "$repo/release-assets/strand-nio-9.9.9.jar" ]
}

@test "release-assets가 이미 있으면 실패하고 안의 파일을 건드리지 않는다" {
  make_repo strand-core strand-nio
  mkdir "$repo/release-assets"
  echo stale > "$repo/release-assets/old.jar"
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -ne 0 ]
  [[ "$output" == *"이미 있다"* ]]
  [ "$(ls "$repo/release-assets")" = "old.jar" ]
}

@test "한 모듈의 산출물이 없으면 모듈과 경로를 알리고 실패하며 release-assets를 만들지 않는다" {
  make_repo strand-core
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -ne 0 ]
  [[ "$output" == *"strand-nio"* ]]
  [ ! -e "$repo/release-assets" ]
}

@test "draft Release 노트가 두 모듈을 말한다" {
  grep -qF -- '--notes "strand-core·strand-nio $VERSION.' "$workflow"
}
