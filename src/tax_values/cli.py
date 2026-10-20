"""Command-line entry point for local workbook inspection."""

from __future__ import annotations

import argparse
import json
from typing import Sequence

from .inspection import inspect_file


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tax-values")
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="parse a workbook and print a safe summary")
    inspect.add_argument("path", help="path to a Verohallinto .xls workbook")
    args = parser.parse_args(argv)

    if args.command == "inspect":
        report = inspect_file(args.path)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
