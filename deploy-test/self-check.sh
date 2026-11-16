#!/usr/bin/env bash
# 하네스 자체 스모크. mavenLocal에 배포한 core와 nio의 POM 메타데이터와, 기대 버전이 틀리면 하네스가 실패하는지 확인한다.
# 사용법: deploy-test/self-check.sh <버전>   (먼저 ./gradlew publishToMavenLocal)
set -euo pipefail

version="${1:?usage: self-check.sh <버전>}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy-test/origin-check.sh
source "$here/origin-check.sh"
m2="$HOME/.m2/repository/io/github/ppzxc"

# POM에 발행 메타데이터가 모두 있는지 본다.
check_pom_metadata() { # <모듈 artifactId> <pom 경로>
  local module="$1" pom="$2"
  for tag in "<groupId>io.github.ppzxc</groupId>" "<artifactId>$module</artifactId>" "<name>" "<description>" \
    "<url>https://github.com/ppzxc/strand</url>" "<scm>" "<developer>" "<license>" "Apache License"; do
    grep -qF -- "$tag" "$pom" || { echo "POM에 $tag 가 없다: $pom" >&2; return 1; }
  done
}

pom_description_line() { grep -m1 -F "<description>" "$1" | sed 's/^[[:space:]]*//'; }

# <dependencies> 안의 artifactId 목록(정렬).
pom_dependency_artifacts() {
  sed -n '/<dependencies>/,/<\/dependencies>/p' "$1" | sed -n 's:.*<artifactId>\(.*\)</artifactId>.*:\1:p' | sort
}

# nio POM 검사: 메타데이터, core와 다른 description, core 외 의존이 없음.
# jspecify는 core도 POM에 compile 범위로 싣는 애너테이션 전용(compileOnlyApi) 의존이라 core와 같은 수준으로 허용한다.
check_nio_pom() { # <nio pom 경로> <core pom 경로>
  local nio="$1" core="$2" deps
  check_pom_metadata strand-nio "$nio" || return 1
  if [ "$(pom_description_line "$nio")" = "$(pom_description_line "$core")" ]; then
    echo "nio description이 core와 같다: $(pom_description_line "$nio")" >&2
    return 1
  fi
  deps="$(pom_dependency_artifacts "$nio" | tr '\n' ' ')"
  if [ "$deps" != "jspecify strand-core " ]; then
    echo "nio POM의 의존이 strand-core(와 애너테이션 jspecify)뿐이 아니다: $deps" >&2
    return 1
  fi
}

core_pom="$m2/strand-core/$version/strand-core-$version.pom"
nio_pom="$m2/strand-nio/$version/strand-nio-$version.pom"
check_pom_metadata strand-core "$core_pom" || exit 1
echo "POM 메타데이터 확인: strand-core"
check_nio_pom "$nio_pom" "$core_pom" || exit 1
echo "nio POM 메타데이터, core와 다른 description, 의존(strand-core) 확인"

# nio POM이 어긋나면 검사가 실패하는지 변조한 사본으로 확인한다.
pomdir="$(mktemp -d)"
trap 'rm -rf "$pomdir"' EXIT
expect_nio_pom_rejected() { # <사유> <변조한 nio pom>
  if (check_nio_pom "$2" "$core_pom") 2>/dev/null; then
    echo "nio POM이 $1 인데 검사가 통과했다" >&2
    exit 1
  fi
}
core_description="$(pom_description_line "$core_pom")"
awk -v d="$core_description" '/<description>/ { print d; next } { print }' "$nio_pom" > "$pomdir/same-description.pom"
expect_nio_pom_rejected "core와 같은 description" "$pomdir/same-description.pom"
sed 's:</dependencies>:<dependency><groupId>x</groupId><artifactId>extra</artifactId></dependency></dependencies>:' "$nio_pom" > "$pomdir/extra-dependency.pom"
expect_nio_pom_rejected "strand-core 외 의존을 가짐" "$pomdir/extra-dependency.pom"
grep -v "<artifactId>strand-core</artifactId>" "$nio_pom" > "$pomdir/no-core.pom"
expect_nio_pom_rejected "strand-core 의존이 없음" "$pomdir/no-core.pom"
echo "nio POM 변조(같은 description, 추가 의존, core 의존 누락) 거부 확인"

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
