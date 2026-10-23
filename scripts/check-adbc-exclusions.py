"""Require a complete GoogleTest report containing only expected failures or skips."""

import argparse
from collections import Counter
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


def check_results(report: Path, returncode: int, expected: list[str]) -> None:
    if returncode not in (0, 1):
        raise ValueError(f"harness exited with status {returncode}, not a test result")

    root = ET.parse(report).getroot()
    if root.tag != "testsuites":
        raise ValueError("expected a GoogleTest <testsuites> report")
    cases = root.findall("testsuite/testcase")
    names = [f"{case.get('classname')}.{case.get('name')}" for case in cases]
    if Counter(names) != Counter(expected):
        raise ValueError(
            f"incomplete or unexpected test results: expected {sorted(expected)}, "
            f"got {sorted(names)}"
        )

    failed = False
    for name, case in zip(names, cases):
        failure = case.find("failure") is not None
        skipped = case.find("skipped") is not None
        result = case.get("result")
        if case.get("status") != "run" or (failure and skipped):
            raise ValueError(f"{name} did not produce a valid test result")
        if result == "completed" and not skipped:
            if not failure:
                raise ValueError(f"{name} now passes; remove it from EXCLUDED")
            failed = True
        elif result != "skipped" or not skipped or failure:
            raise ValueError(f"{name} did not complete or report a skip")

    if returncode != int(failed):
        raise ValueError(f"harness exit status {returncode} disagrees with its XML report")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("returncode", type=int)
    parser.add_argument("expected", nargs="+")
    args = parser.parse_args()
    try:
        check_results(args.report, args.returncode, args.expected)
    except (OSError, ET.ParseError, ValueError) as error:
        print(f"!! expected-failure guard: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
