#!/usr/bin/env python3
"""Copy only required dependency manifests into an isolated OSV scan root."""

import shutil
import sys
from pathlib import Path

from osv_inputs import REQUIRED_INPUTS


def stage_inputs(source_root, target_root):
    source_root = Path(source_root).resolve()
    target_root = Path(target_root)
    source_paths = {}
    missing = []
    escaped = []
    for relative in REQUIRED_INPUTS:
        try:
            source = (source_root / relative).resolve(strict=True)
        except OSError:
            missing.append(relative)
            continue
        if not source.is_file():
            missing.append(relative)
        elif not source.is_relative_to(source_root):
            escaped.append(relative)
        else:
            source_paths[relative] = source
    if missing:
        raise ValueError(f"Missing required OSV inputs: {', '.join(missing)}")
    if escaped:
        raise ValueError(f"OSV inputs escape workspace: {', '.join(escaped)}")
    target_root.mkdir(parents=True)
    for relative in REQUIRED_INPUTS:
        target = target_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_paths[relative], target)


def main(args):
    if len(args) != 3:
        print("Usage: prepare-osv-inputs.py <workspace> <scan-root>", file=sys.stderr)
        return 2
    try:
        stage_inputs(args[1], args[2])
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
