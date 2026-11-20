#!/usr/bin/env bash
# 릴리스 자산 수집. 사용법: collect-release-assets.sh <버전> (저장소 루트의 release-assets/ 를 만든다. 이미 있으면 낡은 자산이 섞이지 않게 실패한다)
# 모듈마다 jar·sources·javadoc과 CycloneDX SBOM(<모듈>-<버전>-cyclonedx.json)을 담는다.
set -euo pipefail

version="${1:?버전이 필요하다}"
# 경로가 저장소 루트 기준이라 어디서 불러도 루트로 옮겨 실행한다.
cd "$(dirname "$0")/../.."
# 모듈을 더하면 release.yml의 Generate SBOM 단계(cyclonedxBom 태스크)와 노트 문구도 함께 고친다.
modules=(strand-core strand-api strand-nio strand-netty)

[ ! -e release-assets ] || { echo "release-assets가 이미 있다. 낡은 자산이 섞이지 않게 지우고 다시 돌린다" >&2; exit 1; }

# 하나라도 없으면 release-assets를 만들기 전에 어느 모듈의 어느 파일인지 알리고 실패한다.
for m in "${modules[@]}"; do
  for f in "$m/build/libs/$m-$version.jar" "$m/build/libs/$m-$version-sources.jar" \
           "$m/build/libs/$m-$version-javadoc.jar" "$m/build/reports/cyclonedx/bom.json"; do
    [ -f "$f" ] || { echo "$m 산출물이 없다: $f (Gradle 빌드와 cyclonedxBom을 먼저 돌렸는지 본다)" >&2; exit 1; }
  done
done

mkdir release-assets
for m in "${modules[@]}"; do
  cp "$m/build/libs/$m-$version.jar" \
     "$m/build/libs/$m-$version-sources.jar" \
     "$m/build/libs/$m-$version-javadoc.jar" release-assets/
  cp "$m/build/reports/cyclonedx/bom.json" "release-assets/$m-$version-cyclonedx.json"
done
