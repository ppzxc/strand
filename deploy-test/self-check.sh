#!/usr/bin/env bash
# 하네스 자체 스모크. mavenLocal에 배포한 core의 POM 메타데이터와, 기대 버전이 틀리면 하네스가 실패하는지 확인한다.
# 사용법: deploy-test/self-check.sh <버전>   (먼저 ./gradlew publishToMavenLocal)
set -euo pipefail

version="${1:?usage: self-check.sh <버전>}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy-test/origin-check.sh
source "$here/origin-check.sh"
pom="$HOME/.m2/repository/io/github/ppzxc/strand-core/$version/strand-core-$version.pom"

for tag in "<groupId>io.github.ppzxc</groupId>" "<artifactId>strand-core</artifactId>" "<name>" "<description>" \
  "<url>https://github.com/ppzxc/strand</url>" "<scm>" "<developer>" "<license>" "Apache License"; do
  grep -qF -- "$tag" "$pom" || { echo "POM에 $tag 가 없다: $pom" >&2; exit 1; }
done
echo "POM 메타데이터 확인"

# 소비자 하나만 돌린다(전체 4개는 하네스 본 스텝이 이미 돌렸다). 실패 사유가 버전 불일치일 때만 의도한 실패로 본다.
if output="$(STRAND_CONSUMERS=gradle-classpath STRAND_EXPECTED_VERSION="0.0.0-wrong" "$here/run.sh" "$version" 2>&1)"; then
  echo "기대 버전이 틀렸는데 하네스가 통과했다" >&2
  exit 1
fi
if ! grep -q "버전 불일치" <<<"$output" || ! grep -q "expected=0.0.0-wrong" <<<"$output"; then
  echo "하네스가 버전 불일치가 아닌 다른 사유로 실패했다:" >&2
  echo "$output" >&2
  exit 1
fi
echo "틀린 기대 버전에서 하네스가 버전 불일치로 실패 확인"

# Maven 3.9.x와 3.10의 출처 기록 형식을 모두 인정하고, 다른 저장소나 빈 기록은 거부하는지 확인한다.
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
check_origin() { # <기대: ok|no> <기록 내용>
  printf '%s\n' "$2" > "$tmp"
  if recorded_from_input "$tmp"; then got=ok; else got=no; fi
  [ "$got" = "$1" ] || { echo "출처 판별이 틀렸다(기대 $1, 실제 $got): $2" >&2; exit 1; }
}
check_origin ok 'strand-core-0.0.1.jar>strand-input='
check_origin ok 'strand-core-0.0.1.jar>strand-input-5daa61b217292085aa248281e11a84e126668d37='
check_origin no 'strand-core-0.0.1.jar>central='
check_origin no 'strand-core-0.0.1.jar>other-strand-input='
check_origin no ''
echo "출처 기록 판별 확인"
