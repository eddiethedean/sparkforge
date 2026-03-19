from __future__ import annotations

import argparse
import re
from pathlib import Path

import pytest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    args = parser.parse_args()

    log_path = Path(
        "/Users/odosmatthews/.cursor/projects/Users-odosmatthews-Documents-coding-sparkforge/agent-tools/095bb115-385b-41e5-98a7-24736a13cdca.txt"
    )
    text = log_path.read_text(encoding="utf-8", errors="replace")

    nodeids = [
        m.group(1) for m in re.finditer(r"^FAILED\s+(tests/[^\s]+)", text, flags=re.M)
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for nodeid in nodeids:
        if nodeid not in seen:
            seen.add(nodeid)
            ordered.append(nodeid)

    subset = ordered[args.start : args.end]
    print(f"Collected {len(ordered)} failing nodeids from {log_path}")
    print(f"Running subset [{args.start}:{args.end}] -> {len(subset)} nodeids")
    return pytest.main(["-n", "10", "-ra", "--tb=short", *subset])


if __name__ == "__main__":
    raise SystemExit(main())

