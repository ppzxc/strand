#!/usr/bin/env bats
# sign-artifacts.sh의 시험. 실행: bats .github/scripts/test

setup_file() {
  export KEYHOME="$BATS_FILE_TMPDIR/keygen"
  mkdir -m 700 "$KEYHOME"
  GNUPGHOME="$KEYHOME" gpg --batch --pinentry-mode loopback --passphrase 'pw1' \
    --quick-gen-key 'strand test <test@example.com>' rsa3072 sign never 2>/dev/null
  GNUPGHOME="$KEYHOME" gpg --batch --pinentry-mode loopback --passphrase 'pw1' \
    --export-secret-keys | base64 -w0 > "$BATS_FILE_TMPDIR/key.b64"
  GNUPGHOME="$KEYHOME" gpg --export > "$BATS_FILE_TMPDIR/pub.gpg"
  # vanniktech의 signingInMemoryKeyId처럼 짧은 8자리 키 ID를 쓴다.
  GNUPGHOME="$KEYHOME" gpg --list-secret-keys --with-colons | awk -F: '/^sec/{print substr($5, 9)}' > "$BATS_FILE_TMPDIR/keyid"
}

setup() {
  script="$BATS_TEST_DIRNAME/../sign-artifacts.sh"
  cd "$BATS_TEST_TMPDIR"
  echo jar > a.jar
  echo sbom > b.json
}

@test "각 파일 옆에 분리 서명 .asc를 만들고 공개키로 검증된다" {
  export SIGNING_KEY="$(cat "$BATS_FILE_TMPDIR/key.b64")" SIGNING_PASSWORD=pw1 SIGNING_KEY_ID="$(cat "$BATS_FILE_TMPDIR/keyid")"
  run "$script" a.jar b.json
  [ "$status" -eq 0 ]
  [ -f a.jar.asc ] && [ -f b.json.asc ]
  verify="$BATS_TEST_TMPDIR/verify"
  mkdir -m 700 "$verify"
  GNUPGHOME="$verify" gpg --import "$BATS_FILE_TMPDIR/pub.gpg" 2>/dev/null
  GNUPGHOME="$verify" gpg --verify a.jar.asc a.jar
  GNUPGHOME="$verify" gpg --verify b.json.asc b.json
}

@test "armor 문자열 키는 거부한다" {
  export SIGNING_KEY="-----BEGIN PGP PRIVATE KEY BLOCK-----" SIGNING_PASSWORD=pw1 SIGNING_KEY_ID=00000000
  run "$script" a.jar
  [ "$status" -ne 0 ]
  [ ! -f a.jar.asc ]
}

@test "키나 비밀번호가 없으면 실패" {
  unset SIGNING_KEY SIGNING_PASSWORD SIGNING_KEY_ID
  run "$script" a.jar
  [ "$status" -ne 0 ]
}

@test "잘못된 비밀번호면 실패하고 .asc를 남기지 않는다" {
  export SIGNING_KEY="$(cat "$BATS_FILE_TMPDIR/key.b64")" SIGNING_PASSWORD=wrong SIGNING_KEY_ID="$(cat "$BATS_FILE_TMPDIR/keyid")"
  run "$script" a.jar
  [ "$status" -ne 0 ]
  [ ! -f a.jar.asc ]
}

@test "서명할 파일이 없으면 실패" {
  export SIGNING_KEY="$(cat "$BATS_FILE_TMPDIR/key.b64")" SIGNING_PASSWORD=pw1 SIGNING_KEY_ID="$(cat "$BATS_FILE_TMPDIR/keyid")"
  run "$script" missing.jar
  [ "$status" -ne 0 ]
}

@test "지정한 키 ID가 가져온 키와 다르면 실패하고 .asc를 남기지 않는다" {
  export SIGNING_KEY="$(cat "$BATS_FILE_TMPDIR/key.b64")" SIGNING_PASSWORD=pw1 SIGNING_KEY_ID=DEADBEEF
  run "$script" a.jar
  [ "$status" -ne 0 ]
  [ ! -f a.jar.asc ]
}

@test "서명 결과가 지정한 키 ID의 서명이다" {
  export SIGNING_KEY="$(cat "$BATS_FILE_TMPDIR/key.b64")" SIGNING_PASSWORD=pw1 SIGNING_KEY_ID="$(cat "$BATS_FILE_TMPDIR/keyid")"
  run "$script" a.jar
  [ "$status" -eq 0 ]
  verify="$BATS_TEST_TMPDIR/verify"
  mkdir -m 700 "$verify"
  GNUPGHOME="$verify" gpg --import "$BATS_FILE_TMPDIR/pub.gpg" 2>/dev/null
  run env GNUPGHOME="$verify" gpg --verify a.jar.asc a.jar
  [[ "$output" == *"$SIGNING_KEY_ID"* ]]
}
