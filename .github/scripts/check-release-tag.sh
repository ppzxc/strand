#!/usr/bin/env bash
# 릴리스 태그 검사. 사용법: check-release-tag.sh <태그> [gradle.properties 경로]
# 1) 태그가 정규식 ^[0-9]+\.[0-9]+\.[0-9]+$ 와 맞아야 한다. on.push.tags 필터는 glob이라 0.0.1-rc1 같은 태그가 통과할 수 있다.
# 2) 태그가 gradle.properties의 version과 같아야 한다.
set -euo pipefail

tag="${1-}"
props="${2:-gradle.properties}"

if ! [[ "$tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "태그 '$tag'는 X.Y.Z 형식이 아니다(v 접두사, pre-release, 스냅샷 불가)" >&2
  exit 1
fi

version="$(sed -n 's/^version=//p' "$props" | head -n1)"
if [ -z "$version" ]; then
  echo "$props에서 version을 찾지 못했다" >&2
  exit 1
fi

if [ "$tag" != "$version" ]; then
  echo "태그 '$tag'와 $props의 version '$version'이 다르다" >&2
  exit 1
fi
echo "태그 $tag 확인"
