from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--version-file", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--watch", nargs="+", required=True)
    args = parser.parse_args()

    changed_files = git("diff", "--name-only", f"origin/{args.base_ref}...HEAD", "--", *args.watch).splitlines()
    changed_files = [item for item in changed_files if item]
    if not changed_files:
        print(f"[{args.label}] no watched files changed; skipping version bump validation")
        return 0

    version_path = Path(args.version_file)
    current_version = version_path.read_text(encoding="utf-8").strip()
    if not current_version:
        print(f"[{args.label}] version file is empty: {version_path}", file=sys.stderr)
        return 1

    try:
        previous_version = git("show", f"origin/{args.base_ref}:{version_path.as_posix()}").strip()
    except RuntimeError:
        print(f"[{args.label}] version file not present on base ref; skipping comparison")
        return 0

    if current_version == previous_version:
        print(
            f"[{args.label}] version did not change. previous={previous_version} current={current_version}",
            file=sys.stderr,
        )
        return 1

    print(f"[{args.label}] version bump detected: {previous_version} -> {current_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
