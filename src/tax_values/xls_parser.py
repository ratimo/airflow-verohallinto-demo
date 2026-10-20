"""Read Verohallinto's passenger-car XLS sheets without normalizing values."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


EXPECTED_COLUMNS = (
    "Merkki",
    "Malli",
    "Cm3",
    "Käyttövoima",
    "OvienLkm",
    "Korimalli",
    "Vetotapa",
    "Vaihteisto",
    "Kw",
    "Yksilölliset ominaisuudet",
    "Päätöspäivä",
    "Ensirekisteröintipäivä",
    "Ajokm/1000",
    "Verotusarvo",
    "Veroprosentti",
    "Autovero",
)


class SheetLike(Protocol):
    name: str
    nrows: int
    ncols: int

    def cell_value(self, rowx: int, colx: int) -> Any: ...


class WorkbookLike(Protocol):
    def sheets(self) -> list[SheetLike]: ...


class WorkbookSchemaError(ValueError):
    """Raised when a data sheet is missing or changes expected columns."""


@dataclass(frozen=True)
class ParsedRow:
    source_sheet: str
    source_row: int
    values: dict[str, Any]


def _row_values(sheet: SheetLike, row_index: int) -> list[Any]:
    return [sheet.cell_value(row_index, col_index) for col_index in range(sheet.ncols)]


def _clean_header(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def parse_workbook(workbook: WorkbookLike) -> list[ParsedRow]:
    """Extract rows from monthly tables, preserving raw cell values and lineage."""
    parsed: list[ParsedRow] = []
    required = set(EXPECTED_COLUMNS)

    for sheet in workbook.sheets():
        header_index = None
        headers: list[str] = []

        for row_index in range(min(sheet.nrows, 40)):
            candidate = [_clean_header(value) for value in _row_values(sheet, row_index)]
            present = {value for value in candidate if value}
            if "Merkki" in present or "Autovero" in present:
                missing = required - present
                if missing:
                    raise WorkbookSchemaError(
                        f"Sheet {sheet.name!r} has an incomplete header; missing: "
                        + ", ".join(sorted(missing))
                    )
                headers = candidate
                header_index = row_index
                break

        if header_index is None:
            # Cover pages and non-data sheets do not contain both anchor columns.
            continue

        column_indexes = {name: headers.index(name) for name in EXPECTED_COLUMNS}
        for row_index in range(header_index + 1, sheet.nrows):
            values = _row_values(sheet, row_index)
            if not any(value not in (None, "") for value in values):
                continue
            parsed.append(
                ParsedRow(
                    source_sheet=sheet.name,
                    source_row=row_index + 1,
                    values={
                        name: values[column_indexes[name]]
                        if column_indexes[name] < len(values)
                        else ""
                        for name in EXPECTED_COLUMNS
                    },
                )
            )

    return parsed


def parse_xls_bytes(content: bytes) -> list[ParsedRow]:
    """Open an Excel 97-2003 workbook with xlrd and parse its monthly sheets."""
    try:
        import xlrd
    except ImportError as exc:
        raise RuntimeError("Parsing .xls files requires xlrd>=2.0.1") from exc

    workbook = xlrd.open_workbook(file_contents=content, on_demand=True)
    try:
        return parse_workbook(workbook)
    finally:
        workbook.release_resources()
