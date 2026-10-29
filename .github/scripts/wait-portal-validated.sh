#!/usr/bin/env bash
# Portal 배포가 VALIDATED가 될 때까지 기다린다. 사용법: wait-portal-validated.sh <deployment id>
# vanniktech 0.37.0은 automaticRelease=false이면 검증을 기다리지 않는다("Skipping deployment validation!").
# 환경 변수: STRAND_REPO_TOKEN(사용자 토큰 username:password의 base64, 필수), TIMEOUT_SECONDS(기본 1800),
#   INTERVAL_SECONDS(기본 10), PORTAL_BASE(기본 https://central.sonatype.com).
# FAILED이거나 타임아웃이면 실패한다. 일시적 HTTP 오류는 다음 주기에 다시 시도한다.
set -euo pipefail

id="${1:?usage: wait-portal-validated.sh <deployment id>}"
: "${STRAND_REPO_TOKEN:?STRAND_REPO_TOKEN이 없다}"
timeout_s="${TIMEOUT_SECONDS:-1800}"
interval="${INTERVAL_SECONDS:-10}"
base="${PORTAL_BASE:-https://central.sonatype.com}"

deadline=$((SECONDS + timeout_s))
while :; do
  # 토큰은 인자(ps에 보임) 대신 curl 설정을 표준 입력으로 넘긴다.
  state="$(curl -fsS -X POST -K - "$base/api/v1/publisher/status?id=$id" <<<"header = \"Authorization: Bearer $STRAND_REPO_TOKEN\"" | jq -r '.deploymentState // empty')" || state=""
  echo "deployment $id: ${state:-조회 실패}"
  case "$state" in
    VALIDATED) exit 0 ;;
    FAILED) echo "deployment $id 검증 실패(FAILED)" >&2; exit 1 ;;
  esac
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "deployment $id가 ${timeout_s}초 안에 VALIDATED가 되지 못했다(타임아웃, 마지막 상태: ${state:-없음})" >&2
    exit 1
  fi
  sleep "$interval"
done
