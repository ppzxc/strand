#!/usr/bin/env bats

@test "CI scans PR changes and release tree with redacted findings" {
  run python3 - <<'PY'
import yaml

ci = yaml.safe_load(open(".github/workflows/ci.yml", encoding="utf-8"))
steps = ci["jobs"]["verify"]["steps"]
scan = next(step for step in steps if step.get("name") == "Scan secrets")
assert scan["uses"] == "gitleaks/gitleaks-action@e0c47f4f8be36e29cdc102c57e68cb5cbf0e8d1e"
assert scan["env"]["GITLEAKS_ENABLE_COMMENTS"] == "false"
tree_scan = next(step for step in steps if step.get("name") == "Scan release tree")
assert tree_scan["if"] == "github.event_name == 'push'"
assert tree_scan["run"] == "gitleaks dir --redact ."
assert ci["jobs"]["ci-ok"]["needs"] == ["verify", "runtime"]
PY
  [ "$status" -eq 0 ]
}

@test "synthetic secret fails scanning without printing its value" {
  fixture="$BATS_TEST_TMPDIR/fake-secret.txt"
  secret=b3e5f2a8c9d1f7a4
  printf 'password = "%s"\n' "$secret" > "$fixture"
  run gitleaks dir --redact --exit-code=2 "$BATS_TEST_TMPDIR"
  [ "$status" -eq 2 ]
  [[ "$output" != *"$secret"* ]]
}
