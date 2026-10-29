#!/usr/bin/env bats
# release.yml의 on.push.tags 필터가 glob이라는 점을 확인한다. GitHub 필터에서 `*`는 `/`를 제외한 모든 문자와 맞는다.
# 여기서는 같은 패턴을 bash 패턴 매칭으로 시험한다. `/`가 들어간 태그는 필터 구현 차이가 있어 제외한다.
# 필터가 통과시키는 비 X.Y.Z 태그는 check-release-tag.sh가 첫 잡에서 걸러 낸다.

filter='[0-9]*.[0-9]*.[0-9]*'

@test "필터: v 접두사 태그는 걸러진다" {
  [[ "v0.0.1" != $filter ]]
}

@test "필터: X.Y.Z 태그는 통과한다" {
  [[ "0.0.1" == $filter ]]
  [[ "12.34.56" == $filter ]]
}

@test "필터 한계: 0.0.1-rc1은 필터를 통과한다(첫 잡의 정규식이 막는다)" {
  [[ "0.0.1-rc1" == $filter ]]
  run "$BATS_TEST_DIRNAME/../check-release-tag.sh" 0.0.1-rc1 <(printf 'version=0.0.1-rc1\n')
  [ "$status" -ne 0 ]
}
