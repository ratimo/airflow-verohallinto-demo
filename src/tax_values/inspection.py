"""Privacy-conscious workbook summary for local source-data verification."""

from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from .validation import normalize_row
from .xls_parser import EXPECTED_COLUMNS, ParsedRow, parse_xls_bytes


def build_report(filename: str, content: bytes, rows: Iterable[ParsedRow]) -> dict[str, Any]:
    parsed_rows = list(rows)
    normalized = [normalize_row(row) for row in parsed_rows]
    records_by_sheet = Counter(row.source_sheet for row in parsed_rows)
    issue_counts = Counter(
        f"{issue.field}:{issue.code}"
        for row in normalized
        for issue in row.issues
    )
    return {
        "source_file": Path(filename).name,
        "file_size_bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "schema_columns": list(EXPECTED_COLUMNS),
        "records_total": len(parsed_rows),
        "records_by_sheet": dict(sorted(records_by_sheet.items())),
        "rows_with_issues": sum(bool(row.issues) for row in normalized),
        "quality_issues_by_field_code": dict(sorted(issue_counts.items())),
    }


def inspect_file(path: str | Path) -> dict[str, Any]:
    source_path = Path(path)
    content = source_path.read_bytes()
    rows = parse_xls_bytes(content)
    return build_report(source_path.name, content, rows)
