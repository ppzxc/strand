#!/usr/bin/env bats
# release.yml이 core·api·nio·netty 네 모듈의 SBOM과 자산을 같은 모양으로 담는지 본다. 실행: bats .github/scripts/test

setup() {
  workflow="$BATS_TEST_DIRNAME/../../workflows/release.yml"
  collect="$BATS_TEST_DIRNAME/../collect-release-assets.sh"
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

@test "수집 스크립트를 실제로 돌리면 네 모듈의 jar·sources·javadoc·SBOM 16개가 모인다" {
  make_repo strand-core strand-api strand-nio strand-netty
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -eq 0 ]
  LC_ALL=C run ls "$repo/release-assets"
  [ "$output" = "strand-api-9.9.9-cyclonedx.json
strand-api-9.9.9-javadoc.jar
strand-api-9.9.9-sources.jar
strand-api-9.9.9.jar
strand-core-9.9.9-cyclonedx.json
strand-core-9.9.9-javadoc.jar
strand-core-9.9.9-sources.jar
strand-core-9.9.9.jar
strand-netty-9.9.9-cyclonedx.json
strand-netty-9.9.9-javadoc.jar
strand-netty-9.9.9-sources.jar
strand-netty-9.9.9.jar
strand-nio-9.9.9-cyclonedx.json
strand-nio-9.9.9-javadoc.jar
strand-nio-9.9.9-sources.jar
strand-nio-9.9.9.jar" ]
}

@test "각 SBOM은 같은 모듈의 bom.json이다" {
  make_repo strand-core strand-api strand-nio strand-netty
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -eq 0 ]
  [ "$(cat "$repo/release-assets/strand-core-9.9.9-cyclonedx.json")" = "sbom of strand-core" ]
  [ "$(cat "$repo/release-assets/strand-api-9.9.9-cyclonedx.json")" = "sbom of strand-api" ]
  [ "$(cat "$repo/release-assets/strand-nio-9.9.9-cyclonedx.json")" = "sbom of strand-nio" ]
  [ "$(cat "$repo/release-assets/strand-netty-9.9.9-cyclonedx.json")" = "sbom of strand-netty" ]
}

@test "다른 디렉터리에서 불러도 저장소 루트에 모은다" {
  make_repo strand-core strand-api strand-nio strand-netty
  cd /
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -eq 0 ]
  [ -f "$repo/release-assets/strand-nio-9.9.9.jar" ]
}

@test "release-assets가 이미 있으면 실패하고 안의 파일을 건드리지 않는다" {
  make_repo strand-core strand-api strand-nio strand-netty
  mkdir "$repo/release-assets"
  echo stale > "$repo/release-assets/old.jar"
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -ne 0 ]
  [[ "$output" == *"이미 있다"* ]]
  [ "$(ls "$repo/release-assets")" = "old.jar" ]
}

@test "한 모듈의 산출물이 없으면 모듈과 경로를 알리고 실패하며 release-assets를 만들지 않는다" {
  make_repo strand-core strand-api strand-nio
  run "$repo/.github/scripts/collect-release-assets.sh" 9.9.9
  [ "$status" -ne 0 ]
  [[ "$output" == *"strand-netty"* ]]
  [ ! -e "$repo/release-assets" ]
}

# 로컬에 PyYAML이 없으면 건너뛰되, CI에서는 조용히 건너뛰지 않고 실패시킨다.
require_yaml() {
  python3 -c 'import yaml' 2>/dev/null && return 0
  [ -z "${CI-}" ] || { echo "CI에는 python3-yaml이 있어야 한다" >&2; return 1; }
  skip "python3-yaml이 없다"
}

# release.yml을 파싱해 단계를 이름으로 찾아, 수집 스크립트의 모듈 목록과 대조한다.
# 모듈 목록의 출처는 수집 스크립트의 modules=(...) 한 곳이다.
check_workflow() {
  python3 - "$workflow" "$collect" <<'PY'
import re, sys, yaml
wf, script = sys.argv[1:3]
modules = re.search(r'^modules=\((.*?)\)$', open(script, encoding='utf-8').read(), re.M).group(1).split()
assert modules, "modules=() 를 찾지 못했다"
steps = {s["name"]: s for s in yaml.safe_load(open(wf, encoding='utf-8'))["jobs"]["release"]["steps"] if "name" in s}

sbom = set(re.findall(r':([\w-]+):cyclonedxBom', steps["Generate SBOM"]["run"]))
assert sbom == set(modules), f"SBOM 태스크 {sorted(sbom)} != 모듈 {modules}"

collect = steps["Collect and sign release assets"]["run"]
assert "collect-release-assets.sh" in collect and "sign-artifacts.sh" in collect

notes = re.search(r'--notes "([^"]*)"', steps["Create draft GitHub Release"]["run"]).group(1)
missing = [m for m in modules if not re.search(r'(?<![\w-])' + re.escape(m) + r'(?![\w-])', notes)]
assert not missing, f"노트가 {missing}를 말하지 않는다: {notes}"
PY
}

@test "워크플로의 SBOM 태스크·서명 단계·Release 노트가 수집 스크립트의 모듈 목록과 일치한다" {
  require_yaml
  run check_workflow
  [ "$status" -eq 0 ]
}

@test "수집 스크립트에 모듈을 더하고 워크플로를 그대로 두면 위 대조가 실패한다" {
  require_yaml
  repo="$BATS_TEST_TMPDIR/drift"
  mkdir -p "$repo"
  sed 's/^modules=(strand-core strand-api strand-nio strand-netty)$/modules=(strand-core strand-api strand-nio strand-netty strand-xyz)/' "$collect" > "$repo/collect.sh"
  collect="$repo/collect.sh"
  run check_workflow
  [ "$status" -ne 0 ]
  [[ "$output" == *"strand-xyz"* ]]
}
