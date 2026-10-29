#!/usr/bin/env bats
# check-release-secrets.sh의 시험. 실행: bats .github/scripts/test

setup() {
  script="$BATS_TEST_DIRNAME/../check-release-secrets.sh"
  export ORG_GRADLE_PROJECT_mavenCentralUsername="user-secret-value"
  export ORG_GRADLE_PROJECT_mavenCentralPassword="pass-secret-value"
  export ORG_GRADLE_PROJECT_signingInMemoryKey="key-secret-value"
  export ORG_GRADLE_PROJECT_signingInMemoryKeyId="keyid-secret-value"
  export ORG_GRADLE_PROJECT_signingInMemoryKeyPassword="keypass-secret-value"
}

@test "시크릿 5개가 모두 있으면 통과" {
  run "$script"
  [ "$status" -eq 0 ]
}

@test "빈 문자열인 시크릿이 있으면 실패하고 이름을 알려 준다" {
  export ORG_GRADLE_PROJECT_signingInMemoryKeyId=""
  run "$script"
  [ "$status" -ne 0 ]
  [[ "$output" == *"ORG_GRADLE_PROJECT_signingInMemoryKeyId"* ]]
}

@test "설정되지 않은 시크릿이 있으면 실패한다" {
  unset ORG_GRADLE_PROJECT_mavenCentralPassword
  run "$script"
  [ "$status" -ne 0 ]
  [[ "$output" == *"ORG_GRADLE_PROJECT_mavenCentralPassword"* ]]
}

@test "여러 개가 비어 있으면 모두 알려 준다" {
  export ORG_GRADLE_PROJECT_mavenCentralUsername=""
  export ORG_GRADLE_PROJECT_signingInMemoryKey=""
  run "$script"
  [ "$status" -ne 0 ]
  [[ "$output" == *"ORG_GRADLE_PROJECT_mavenCentralUsername"* ]]
  [[ "$output" == *"ORG_GRADLE_PROJECT_signingInMemoryKey"* ]]
}

@test "시크릿 값은 출력하지 않는다" {
  export ORG_GRADLE_PROJECT_mavenCentralUsername="SENTINEL-username-1"
  export ORG_GRADLE_PROJECT_mavenCentralPassword="SENTINEL-password-2"
  export ORG_GRADLE_PROJECT_signingInMemoryKey="SENTINEL-key-3"
  export ORG_GRADLE_PROJECT_signingInMemoryKeyId="SENTINEL-keyid-4"
  export ORG_GRADLE_PROJECT_signingInMemoryKeyPassword="SENTINEL-keypass-5"
  # 통과 경로와 실패 경로 모두에서 값이 새지 않는지 본다.
  run "$script"
  [[ "$output" != *"SENTINEL"* ]]
  export ORG_GRADLE_PROJECT_signingInMemoryKeyId=""
  run "$script"
  [ "$status" -ne 0 ]
  [[ "$output" != *"SENTINEL"* ]]
}
