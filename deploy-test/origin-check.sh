#!/usr/bin/env bash
# Maven 로컬 저장소의 _remote.repositories에서 strand-core가 `strand-input` 저장소에서 왔는지 판별한다.
# Maven 3.9.x는 `파일>strand-input=`, 3.10(GitHub 러너)은 `파일>strand-input-<해시>=`로 기록한다. 둘 다 받는다.
# 사용법: source deploy-test/origin-check.sh; recorded_from_input <_remote.repositories 경로>
recorded_from_input() {
  grep -Eq '>strand-input(-[0-9a-f]+)?=' "$1"
}
