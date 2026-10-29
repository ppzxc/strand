#!/usr/bin/env bash
# Portal 배포가 VALIDATED가 될 때까지 기다린다. 사용법: wait-portal-validated.sh <deployment id>
# vanniktech 0.37.0은 automaticRelease=false이면 검증을 기다리지 않는다("Skipping deployment validation!").
# 환경 변수: STRAND_REPO_TOKEN(사용자 토큰 username:password의 base64, 필수), TIMEOUT_SECONDS(기본 1800),
#   INTERVAL_SECONDS(기본 10), PORTAL_BASE(기본 https://central.sonatype.com).
# FAILED, PUBLISHING·PUBLISHED(이미 공개됨), 타임아웃이면 실패한다. 일시적 HTTP 오류는 다음 주기에 다시 시도한다.
set -euo pipefail

id="${1:?usage: wait-portal-validated.sh <deployment id>}"
: "${STRAND_REPO_TOKEN:?STRAND_REPO_TOKEN이 없다}"
timeout_s="${TIMEOUT_SECONDS:-1800}"
interval="${INTERVAL_SECONDS:-10}"
base="${PORTAL_BASE:-https://central.sonatype.com}"

for v in "$timeout_s" "$interval"; do
  [[ "$v" =~ ^[0-9]+$ ]] || { echo "TIMEOUT_SECONDS·INTERVAL_SECONDS는 0 이상의 정수여야 한다: '$v'" >&2; exit 2; }
done

deadline=$((SECONDS + timeout_s))
while :; do
  # 토큰은 인자(ps에 보임) 대신 curl 설정을 표준 입력으로 넘긴다.
  state="$(curl -fsS --connect-timeout 10 --max-time 30 -X POST -K - "$base/api/v1/publisher/status?id=$id" <<<"header = \"Authorization: Bearer $STRAND_REPO_TOKEN\"" | jq -r '.deploymentState // empty')" || state=""
  echo "deployment $id: ${state:-조회 실패}"
  case "$state" in
    VALIDATED) exit 0 ;;
    # 이미 Publish된 배포는 VALIDATED를 지났지만, 하네스 (b)는 공개 전 스테이징을 검증하는 단계라 성공으로 치지 않는다.
    # 누군가 먼저 Publish했거나 재실행이다. 사람이 상태를 확인해야 한다.
    PUBLISHING|PUBLISHED) echo "deployment $id가 이미 $state다. 하네스 (b) 전에 Publish된 배포는 검증하지 않는다(Portal에서 상태 확인)" >&2; exit 1 ;;
    FAILED) echo "deployment $id 검증 실패(FAILED)" >&2; exit 1 ;;
  esac
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "deployment $id가 ${timeout_s}초 안에 VALIDATED가 되지 못했다(타임아웃, 마지막 상태: ${state:-없음})" >&2
    exit 1
  fi
  sleep "$interval"
done
