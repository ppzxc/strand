#!/usr/bin/env bash
# 파일마다 분리 서명(.asc)을 만든다. 사용법: sign-artifacts.sh <파일>...
# 환경 변수: SIGNING_KEY(비밀키를 `gpg --export-secret-keys | base64 -w0`로 인코딩한 한 줄), SIGNING_PASSWORD, SIGNING_KEY_ID(vanniktech의 signingInMemoryKeyId와 같은 값. --local-user로 서명 키를 지정한다).
# 키는 임시 GNUPGHOME에만 가져오고 종료할 때 지운다.
set -euo pipefail

: "${SIGNING_KEY:?SIGNING_KEY가 없다}"
: "${SIGNING_PASSWORD:?SIGNING_PASSWORD가 없다}"
: "${SIGNING_KEY_ID:?SIGNING_KEY_ID가 없다}"
[ "$#" -gt 0 ] || { echo "usage: sign-artifacts.sh <파일>..." >&2; exit 2; }

if [[ "$SIGNING_KEY" == -----BEGIN* ]]; then
  echo "SIGNING_KEY는 armor 문자열이 아니라 base64 인코딩 값이어야 한다" >&2
  exit 1
fi
for f in "$@"; do
  [ -f "$f" ] || { echo "서명할 파일이 없다: $f" >&2; exit 1; }
done

GNUPGHOME="$(mktemp -d)"
export GNUPGHOME
trap 'gpgconf --kill gpg-agent 2>/dev/null || true; rm -rf "$GNUPGHOME"' EXIT

printf '%s' "$SIGNING_KEY" | base64 -d | gpg --batch --quiet --import
for f in "$@"; do
  gpg --batch --quiet --pinentry-mode loopback --passphrase-fd 3 \
    --local-user "$SIGNING_KEY_ID" --armor --detach-sign --output "$f.asc.tmp" "$f" 3<<<"$SIGNING_PASSWORD"
  mv "$f.asc.tmp" "$f.asc"
  echo "서명: $f.asc"
done
