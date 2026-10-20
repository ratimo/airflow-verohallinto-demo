"""Type conversion and explicit, non-destructive data-quality reporting."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .xls_parser import ParsedRow


@dataclass(frozen=True)
class QualityIssue:
    field: str
    code: str
    message: str


@dataclass(frozen=True)
class NormalizedRow:
    source_sheet: str
    source_row: int
    raw_values: dict[str, Any]
    values: dict[str, Any]
    issues: tuple[QualityIssue, ...]


TEXT_FIELDS = {
    "Merkki": "brand",
    "Malli": "model",
    "Käyttövoima": "fuel_type",
    "Korimalli": "body_style",
    "Vetotapa": "drivetrain",
    "Vaihteisto": "transmission",
    "Yksilölliset ominaisuudet": "individual_features",
}
INTEGER_FIELDS = {
    "Cm3": "engine_displacement_cm3",
    "OvienLkm": "door_count",
    "Kw": "power_kw",
}
DECIMAL_FIELDS = {
    "Ajokm/1000": "odometer_1000_km",
    "Verotusarvo": "tax_value",
    "Veroprosentti": "tax_rate_percent",
    "Autovero": "vehicle_tax_amount",
}
DATE_FIELDS = {
    "Päätöspäivä": "decision_date",
    "Ensirekisteröintipäivä": "first_registration_date",
}
REQUIRED_FIELDS = {"Merkki", "Malli", "Päätöspäivä", "Verotusarvo"}
_NUMBER_PATTERN = re.compile(r"^[+-]?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?$")


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _parse_decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise InvalidOperation
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = Decimal(str(value))
    elif isinstance(value, str):
        text = value.strip()
        if not _NUMBER_PATTERN.fullmatch(text):
            raise InvalidOperation
        parsed = Decimal(text.replace(",", ""))
    else:
        raise InvalidOperation
    if not parsed.is_finite():
        raise InvalidOperation
    return parsed


def _parse_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, bool):
        raise ValueError
    if isinstance(value, (int, float, Decimal)):
        numeric = Decimal(str(value))
        if not numeric.is_finite() or numeric != numeric.to_integral_value():
            raise ValueError
        text = str(int(numeric))
    elif isinstance(value, str):
        text = value.strip()
    else:
        raise ValueError
    if not re.fullmatch(r"\d{8}", text):
        raise ValueError
    return datetime.strptime(text, "%Y%m%d").date()


def normalize_row(row: ParsedRow) -> NormalizedRow:
    """Convert a source row while retaining it and reporting every bad field."""
    raw = dict(row.values)
    values: dict[str, Any] = {}
    issues: list[QualityIssue] = []

    for source_name, target_name in TEXT_FIELDS.items():
        value = raw.get(source_name)
        values[target_name] = None if _is_blank(value) else str(value).strip()

    for source_name, target_name in INTEGER_FIELDS.items():
        value = raw.get(source_name)
        if _is_blank(value):
            values[target_name] = None
            continue
        try:
            number = _parse_decimal(value)
            if number != number.to_integral_value():
                raise InvalidOperation
            values[target_name] = int(number)
        except (InvalidOperation, ValueError, TypeError):
            values[target_name] = None
            issues.append(QualityIssue(source_name, "invalid_integer", "Expected a whole number."))

    for source_name, target_name in DECIMAL_FIELDS.items():
        value = raw.get(source_name)
        if _is_blank(value):
            values[target_name] = None
            continue
        try:
            values[target_name] = _parse_decimal(value)
        except (InvalidOperation, ValueError, TypeError):
            values[target_name] = None
            issues.append(QualityIssue(source_name, "invalid_number", "Expected a finite number."))

    odometer_thousands = values["odometer_1000_km"]
    values["odometer_km"] = odometer_thousands * Decimal(1000) if odometer_thousands is not None else None

    for source_name, target_name in DATE_FIELDS.items():
        value = raw.get(source_name)
        if _is_blank(value):
            values[target_name] = None
            continue
        try:
            values[target_name] = _parse_date(value)
        except (ValueError, TypeError, OverflowError):
            values[target_name] = None
            issues.append(QualityIssue(source_name, "invalid_date", "Expected a valid YYYYMMDD date."))

    for source_name in REQUIRED_FIELDS:
        if _is_blank(raw.get(source_name)):
            issues.append(QualityIssue(source_name, "missing_required", "Required source value is blank."))

    return NormalizedRow(
        source_sheet=row.source_sheet,
        source_row=row.source_row,
        raw_values=raw,
        values=values,
        issues=tuple(issues),
    )
