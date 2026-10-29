#!/usr/bin/env bash
# vanniktech 업로드 로그(표준 입력)에서 Portal deployment id를 출력한다.
# vanniktech 0.37.0은 "Uploaded bundle to Central Portal as <TYPE>, deployment id: <id>"를 lifecycle로 남긴다.
set -euo pipefail

id="$(sed -n 's/.*Uploaded bundle to Central Portal as [A-Z_]*, deployment id: \([0-9a-zA-Z-]*\).*/\1/p' | head -n1)"
if [ -z "$id" ]; then
  echo "로그에서 deployment id를 찾지 못했다" >&2
  exit 1
fi
echo "$id"
