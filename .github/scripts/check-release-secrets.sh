#!/usr/bin/env bash
# 릴리스 시크릿 5개가 비어 있지 않은지 본다. 사용법: check-release-secrets.sh (값은 환경 변수로 받는다)
# 값은 절대 출력하지 않는다. 길이만 검사하고, 비어 있는 이름만 알린다.
# 시크릿이 없으면 Actions는 빈 문자열을 넘기므로, 업로드(수십 분) 전에 여기서 빨리 실패시킨다.
set -euo pipefail

names=(
  ORG_GRADLE_PROJECT_mavenCentralUsername
  ORG_GRADLE_PROJECT_mavenCentralPassword
  ORG_GRADLE_PROJECT_signingInMemoryKey
  ORG_GRADLE_PROJECT_signingInMemoryKeyId
  ORG_GRADLE_PROJECT_signingInMemoryKeyPassword
)

missing=0
for name in "${names[@]}"; do
  value="${!name-}"
  if [ "${#value}" -eq 0 ]; then
    echo "::error::시크릿 $name 이(가) 비어 있다" >&2
    missing=1
  fi
done

[ "$missing" -eq 0 ] || exit 1
echo "릴리스 시크릿 ${#names[@]}개 확인"
