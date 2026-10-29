#!/usr/bin/env bats
# check-release-tag.sh의 시험. 실행: bats .github/scripts/test

setup() {
  script="$BATS_TEST_DIRNAME/../check-release-tag.sh"
  props="$BATS_TEST_TMPDIR/gradle.properties"
  printf 'group=io.github.ppzxc\nversion=0.0.1\n' > "$props"
}

@test "태그와 version이 같으면 통과" {
  run "$script" 0.0.1 "$props"
  [ "$status" -eq 0 ]
}

@test "태그와 version이 다르면 실패" {
  run "$script" 0.0.2 "$props"
  [ "$status" -ne 0 ]
  [[ "$output" == *"0.0.2"* ]]
}

@test "v 접두사 태그는 실패" {
  printf 'version=0.0.1\n' > "$props"
  run "$script" v0.0.1 "$props"
  [ "$status" -ne 0 ]
}

@test "pre-release 태그는 version이 같아도 실패" {
  printf 'version=0.0.1-rc1\n' > "$props"
  run "$script" 0.0.1-rc1 "$props"
  [ "$status" -ne 0 ]
}

@test "SNAPSHOT·네 자리·빈 태그는 실패" {
  for tag in 0.0.1-SNAPSHOT 1.2.3.4 1.2 "" 1.2.x; do
    run "$script" "$tag" "$props"
    [ "$status" -ne 0 ]
  done
}

@test "여러 자리 숫자 태그도 version이 같으면 통과" {
  printf 'version=10.20.30\n' > "$props"
  run "$script" 10.20.30 "$props"
  [ "$status" -eq 0 ]
}

@test "version 줄이 없으면 실패" {
  printf 'group=x\n' > "$props"
  run "$script" 0.0.1 "$props"
  [ "$status" -ne 0 ]
}
