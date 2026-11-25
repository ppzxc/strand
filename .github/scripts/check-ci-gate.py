#!/usr/bin/env python3
"""Fail unless all required reusable-workflow jobs are present and successful."""

import json
import sys


REQUIRED_JOBS = {"verify", "runtime"}


def all_required_jobs_succeeded(needs):
    return (
        isinstance(needs, dict)
        and set(needs) == REQUIRED_JOBS
        and all(isinstance(job, dict) and job.get("result") == "success" for job in needs.values())
    )


def main():
    try:
        needs = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError):
        print("::error::필수 CI 결과 JSON을 읽지 못했다", file=sys.stderr)
        return 1

    print(json.dumps(needs, sort_keys=True))
    if not all_required_jobs_succeeded(needs):
        print("::error::verify 또는 runtime 매트릭스가 success가 아니다", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
