import unittest
from datetime import date
from decimal import Decimal

from tax_values.xls_parser import ParsedRow
from tax_values.validation import normalize_row


class NormalizationTests(unittest.TestCase):
    def test_converts_verified_source_formats_and_keeps_raw_values(self):
        raw = {
            "Merkki": " AUDI ",
            "Malli": "A3",
            "Cm3": 1498.0,
            "Käyttövoima": "Bensiini",
            "OvienLkm": 5.0,
            "Korimalli": "Hatchback",
            "Vetotapa": "Etuveto",
            "Vaihteisto": "AUT",
            "Kw": 110.0,
            "Yksilölliset ominaisuudet": "",
            "Päätöspäivä": "20260112",
            "Ensirekisteröintipäivä": "20240205",
            "Ajokm/1000": 20.0,
            "Verotusarvo": 28224.0,
            "Veroprosentti": 11.1,
            "Autovero": 3132.86,
        }
        row = ParsedRow("Tammikuu-Januari", 4, raw)

        result = normalize_row(row)

        self.assertEqual(result.values["brand"], "AUDI")
        self.assertEqual(result.values["engine_displacement_cm3"], 1498)
        self.assertEqual(result.values["decision_date"], date(2026, 1, 12))
        self.assertEqual(result.values["odometer_1000_km"], Decimal("20"))
        self.assertEqual(result.values["odometer_km"], Decimal("20000"))
        self.assertEqual(result.values["tax_value"], Decimal("28224"))
        self.assertEqual(result.values["vehicle_tax_amount"], Decimal("3132.86"))
        self.assertEqual(result.raw_values, raw)
        self.assertEqual(result.issues, ())

    def test_keeps_invalid_row_and_reports_field_issues(self):
        raw = {
            "Merkki": "AUDI",
            "Malli": "A3",
            "Cm3": "1,49x",
            "Käyttövoima": "Bensiini",
            "OvienLkm": "five",
            "Korimalli": "Hatchback",
            "Vetotapa": "Etuveto",
            "Vaihteisto": "",
            "Kw": 110,
            "Yksilölliset ominaisuudet": "",
            "Päätöspäivä": "20261340",
            "Ensirekisteröintipäivä": "20240205",
            "Ajokm/1000": 20,
            "Verotusarvo": 28224,
            "Veroprosentti": 11.1,
            "Autovero": 3132.86,
        }
        row = ParsedRow("Tammikuu-Januari", 5, raw)

        result = normalize_row(row)

        self.assertEqual(result.raw_values, raw)
        self.assertIsNone(result.values["engine_displacement_cm3"])
        self.assertIsNone(result.values["door_count"])
        self.assertIsNone(result.values["decision_date"])
        self.assertEqual(
            {(issue.field, issue.code) for issue in result.issues},
            {
                ("Cm3", "invalid_integer"),
                ("OvienLkm", "invalid_integer"),
                ("Päätöspäivä", "invalid_date"),
            },
        )


if __name__ == "__main__":
    unittest.main()
