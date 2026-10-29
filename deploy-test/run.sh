#!/usr/bin/env bash
# 배포 테스트 하네스. 외부 소비자 4개(Gradle·Maven x classpath·modulepath)를 차례로 돌린다.
# 사용법: deploy-test/run.sh <기대 버전> [저장소 URL]
#   저장소 URL을 생략하면 mavenLocal(a). Portal 스테이징(b)과 Central(c)은 URL만 바꿔 같은 스크립트를 쓴다.
#   환경 변수 STRAND_REPO_TOKEN이 있으면 저장소 요청에 Bearer 헤더를 붙인다. 토큰은 인자나 파일에 쓰지 않는다.
# 환경 변수 STRAND_EXPECTED_VERSION은 하네스 자체 시험용이다. 주면 받아 온 jar가 아니라 이 값과 비교한다(불일치 실패 확인).
# 환경 변수 STRAND_CONSUMERS는 소비자를 공백으로 구분해 일부만 고른다(기본은 4개 전체). self-check가 쓴다.
# 각 소비자는 core만 의존하고, 실행 중인 core의 StrandVersion.get()이 기대 버전과 같을 때만 통과한다.
# 소비자 JDK는 25다. 하나라도 실패하면 비정상 종료한다.
set -euo pipefail

version="${1:?usage: run.sh <기대 버전> [저장소 URL]}"
repo="${2:-mavenLocal}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"

MAVEN_VERSION=3.9.11
MAVEN_SHA512=bcfe4fe305c962ace56ac7b5fc7a08b87d5abd8b7e89027ab251069faebee516b0ded8961445d6d91ec1985dfe30f8153268843c89aa392733d1a3ec956c9978

java_major="$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java.specification.version = //p')"
if [ "$java_major" != "25" ]; then
  echo "소비자 JDK는 25여야 한다(현재 $java_major). JAVA_HOME 또는 PATH를 확인한다." >&2
  exit 1
fi

# mvn이 없으면 Maven Central에서 고정 버전을 받아 SHA-512를 검증한 뒤 쓴다(로컬·CI 공통).
resolve_mvn() {
  if command -v mvn >/dev/null 2>&1; then
    command -v mvn
    return
  fi
  local cache="${XDG_CACHE_HOME:-$HOME/.cache}/strand-deploy-test"
  local dir="$cache/apache-maven-$MAVEN_VERSION"
  if [ ! -x "$dir/bin/mvn" ]; then
    mkdir -p "$cache"
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT # $(...) 서브셸 안이라 아래 전역 trap과 별개다.
    curl -fsSL -o "$tmp/maven.tar.gz" \
      "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$MAVEN_VERSION/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    echo "$MAVEN_SHA512  $tmp/maven.tar.gz" | sha512sum -c - >&2
    tar -xzf "$tmp/maven.tar.gz" -C "$cache"
  fi
  echo "$dir/bin/mvn"
}

maven_local_repo=""
trap 'rm -rf "$maven_local_repo"' EXIT

gradle_args=("-PstrandVersion=$version")
maven_args=("-DstrandVersion=$version")
if [ -n "${STRAND_EXPECTED_VERSION:-}" ]; then
  gradle_args+=("-PexpectedVersion=$STRAND_EXPECTED_VERSION")
  maven_args+=("-DexpectedVersion=$STRAND_EXPECTED_VERSION")
fi
maven_opts=()
if [ "$repo" != "mavenLocal" ]; then
  gradle_args+=("-PrepoUrl=$repo")
  maven_args+=("-DrepoUrl=$repo")
  # ~/.m2에 로컬 설치본이 있으면 원격 검증이 가려진다. 원격 모드는 빈 로컬 저장소를 쓴다.
  maven_local_repo="$(mktemp -d)"
  maven_opts+=("-Dmaven.repo.local=$maven_local_repo")
  if [ -n "${STRAND_REPO_TOKEN:-}" ]; then
    maven_opts+=(-s "$here/maven-settings.xml")
  fi
fi

check_output() {
  tr -d "\r" < "$here/$1/output.log" | grep -qx "OK"
}

run_gradle() {
  echo "=== $1 ==="
  # 소비자는 strand 빌드와 분리돼 있어도 같은 Gradle 래퍼를 쓴다.
  "$root/gradlew" -p "$here/$1" run --no-daemon --console=plain "${gradle_args[@]}" | tee "$here/$1/output.log"
  check_output "$1"
}

run_maven() {
  echo "=== $1 ==="
  "$mvn" -B -ntp ${maven_opts[@]+"${maven_opts[@]}"} -f "$here/$1/pom.xml" compile exec:exec "${maven_args[@]}" | tee "$here/$1/output.log"
  check_output "$1"
  if [ "$repo" != "mavenLocal" ]; then
    # Central에도 같은 좌표가 있을 수 있다. strand-core가 지정 저장소에서 왔는지 로컬 저장소의 출처 기록으로 확인한다.
    recorded_from_input "$maven_local_repo/io/github/ppzxc/strand-core/$version/_remote.repositories" || {
      echo "strand-core가 지정 저장소($repo)에서 오지 않았다" >&2
      exit 1
    }
  fi
}

# shellcheck source=deploy-test/origin-check.sh
source "$here/origin-check.sh"

mvn="$(resolve_mvn)"

for consumer in ${STRAND_CONSUMERS:-gradle-classpath gradle-modulepath maven-classpath maven-modulepath}; do
  case "$consumer" in
    gradle-*) run_gradle "$consumer" ;;
    maven-*) run_maven "$consumer" ;;
    *) echo "알 수 없는 소비자: $consumer" >&2; exit 2 ;;
  esac
done
echo "배포 테스트 소비자 통과: strand-core $version ($repo)"
