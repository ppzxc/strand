# Issue #486 evidence — PR and release secret scans

- Owner: build and release
- Rules: `SEC-003`, `TOOL-001`, Fast gate
- Implementation commit: `df4c374062a75cf0d873241817ec08c965438222` (2026-11-25 19:02:45 Asia/Seoul)
- Integration commit: `23b3b0aa1bd0fe86bdf3a05ff114bfa7b1f175bc`
- Scanner: Gitleaks 8.24.3 arm64; workflow action is pinned.

| Check | Command or path | Result |
|---|---|---|
| Workflow wiring | `.github/workflows/ci.yml`, `.github/workflows/release.yml`; YAML assertions | PASS; PR commit range and release tree are scanned; required result routes through `ci / ci-ok` |
| Synthetic secret redaction | `gitleaks dir --redact .` against a temporary synthetic password fixture | EXPECTED FAIL, exit 2; output omitted the synthetic value |
| Workflow tests | `.github/scripts/test/gitleaks-workflow.bats` | NOT RUN locally; Bats unavailable. CI installs Bats and runs the file. |
| Hosted CI | GitHub Actions | NOT RUN from this workspace |

Test execution timestamp was not captured. No real secret value was used. A scan finding and scanner failure are distinct gate failures; logs and reports use redaction.
