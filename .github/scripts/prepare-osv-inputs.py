#!/usr/bin/env python3
"""Copy only required dependency manifests into an isolated OSV scan root."""

import shutil
import sys
from pathlib import Path

from osv_inputs import REQUIRED_INPUTS


def stage_inputs(source_root, target_root):
    source_root = Path(source_root)
    target_root = Path(target_root)
    missing = [relative for relative in REQUIRED_INPUTS if not (source_root / relative).is_file()]
    if missing:
        raise ValueError(f"Missing required OSV inputs: {', '.join(missing)}")
    target_root.mkdir(parents=True)
    for relative in REQUIRED_INPUTS:
        source = source_root / relative
        target = target_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)


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
