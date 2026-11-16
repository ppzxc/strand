#!/usr/bin/env bash
# 릴리스 자산 수집. 사용법: collect-release-assets.sh <버전> (저장소 루트에서 실행, release-assets/ 를 만든다)
# 모듈마다 jar·sources·javadoc과 CycloneDX SBOM(<모듈>-<버전>-cyclonedx.json)을 담는다.
set -euo pipefail

version="${1:?버전이 필요하다}"
modules=(strand-core strand-nio)

mkdir release-assets
for m in "${modules[@]}"; do
  cp "$m/build/libs/$m-$version.jar" \
     "$m/build/libs/$m-$version-sources.jar" \
     "$m/build/libs/$m-$version-javadoc.jar" release-assets/
  cp "$m/build/reports/cyclonedx/bom.json" "release-assets/$m-$version-cyclonedx.json"
done
