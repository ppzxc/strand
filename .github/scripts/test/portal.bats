#!/usr/bin/env bats
# parse-deployment-id.sh, wait-portal-validated.sh의 시험. 실행: bats .github/scripts/test
# curl은 PATH 앞에 둔 스텁으로 대체한다. 스텁은 $STUB_STATES 파일의 첫 줄을 응답 상태로 쓰고 그 줄을 소비한다(마지막 줄은 유지).

setup() {
  scripts="$BATS_TEST_DIRNAME/.."
  mkdir "$BATS_TEST_TMPDIR/bin"
  cat > "$BATS_TEST_TMPDIR/bin/curl" <<'STUB'
#!/usr/bin/env bash
cat >/dev/null  # -K - 로 받은 설정(토큰)은 버린다
states="$STUB_STATES"
state="$(head -n1 "$states")"
[ "$(wc -l < "$states")" -gt 1 ] && sed -i 1d "$states"
[ "$state" = "HTTP_ERROR" ] && exit 22
printf '{"deploymentId":"x","deploymentState":"%s"}' "$state"
STUB
  chmod +x "$BATS_TEST_TMPDIR/bin/curl"
  export PATH="$BATS_TEST_TMPDIR/bin:$PATH"
  export STUB_STATES="$BATS_TEST_TMPDIR/states"
  export STRAND_REPO_TOKEN=tok INTERVAL_SECONDS=0 TIMEOUT_SECONDS=2
}

@test "gradle 로그에서 deployment id를 뽑는다" {
  log=$'> Task :core:publishToMavenCentral\nUploaded bundle to Central Portal as USER_MANAGED, deployment id: 1b2c3d4e-0000-4aaa-bbbb-123456789abc\nBUILD SUCCESSFUL'
  run "$scripts/parse-deployment-id.sh" <<<"$log"
  [ "$status" -eq 0 ]
  [ "$output" = "1b2c3d4e-0000-4aaa-bbbb-123456789abc" ]
}

@test "로그에 id가 없으면 실패" {
  run "$scripts/parse-deployment-id.sh" <<<"BUILD SUCCESSFUL"
  [ "$status" -ne 0 ]
}

@test "VALIDATED에 도달하면 통과(PENDING, VALIDATING을 거쳐도)" {
  printf 'PENDING\nVALIDATING\nVALIDATED\n' > "$STUB_STATES"
  run "$scripts/wait-portal-validated.sh" some-id
  [ "$status" -eq 0 ]
}

@test "FAILED면 즉시 실패" {
  printf 'FAILED\n' > "$STUB_STATES"
  run "$scripts/wait-portal-validated.sh" some-id
  [ "$status" -ne 0 ]
  [[ "$output" == *FAILED* ]]
}

@test "VALIDATED에 도달하지 못하고 타임아웃이면 실패" {
  printf 'VALIDATING\n' > "$STUB_STATES"
  run "$scripts/wait-portal-validated.sh" some-id
  [ "$status" -ne 0 ]
  [[ "$output" == *타임아웃* ]]
}

@test "일시적 HTTP 오류는 재시도한다" {
  printf 'HTTP_ERROR\nVALIDATED\n' > "$STUB_STATES"
  run "$scripts/wait-portal-validated.sh" some-id
  [ "$status" -eq 0 ]
}

@test "토큰이 없으면 실패" {
  unset STRAND_REPO_TOKEN
  run "$scripts/wait-portal-validated.sh" some-id
  [ "$status" -ne 0 ]
}
