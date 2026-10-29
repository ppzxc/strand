#!/usr/bin/env bash
# 하네스 자체 스모크. mavenLocal에 배포한 core의 POM 메타데이터와, 기대 버전이 틀리면 하네스가 실패하는지 확인한다.
# 사용법: deploy-test/self-check.sh <버전>   (먼저 ./gradlew publishToMavenLocal)
set -euo pipefail

version="${1:?usage: self-check.sh <버전>}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pom="$HOME/.m2/repository/io/github/ppzxc/strand-core/$version/strand-core-$version.pom"

for tag in "<groupId>io.github.ppzxc</groupId>" "<artifactId>strand-core</artifactId>" "<name>" "<description>" \
  "<url>https://github.com/ppzxc/strand</url>" "<scm>" "<developer>" "<license>" "Apache License"; do
  grep -qF -- "$tag" "$pom" || { echo "POM에 $tag 가 없다: $pom" >&2; exit 1; }
done
echo "POM 메타데이터 확인"

if STRAND_EXPECTED_VERSION="0.0.0-wrong" "$here/run.sh" "$version" >/dev/null 2>&1; then
  echo "기대 버전이 틀렸는데 하네스가 통과했다" >&2
  exit 1
fi
echo "틀린 기대 버전에서 하네스 실패 확인"
