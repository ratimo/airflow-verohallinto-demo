import unittest

from tax_values.xls_parser import ParsedRow
from tax_values.inspection import build_report


class InspectionReportTests(unittest.TestCase):
    def test_report_contains_lineage_and_quality_counts_without_row_values(self):
        row = ParsedRow(
            "Tammikuu-Januari",
            2,
            {
                "Merkki": "AUDI",
                "Malli": "A3",
                "Cm3": 1498,
                "Käyttövoima": "Bensiini",
                "OvienLkm": 5,
                "Korimalli": "Hatchback",
                "Vetotapa": "Etuveto",
                "Vaihteisto": "AUT",
                "Kw": 110,
                "Yksilölliset ominaisuudet": "",
                "Päätöspäivä": "20261340",
                "Ensirekisteröintipäivä": "20240205",
                "Ajokm/1000": 20,
                "Verotusarvo": 28224,
                "Veroprosentti": 11.1,
                "Autovero": 3132.86,
            },
        )

        report = build_report("sample.xls", b"content", [row])

        self.assertEqual(report["source_file"], "sample.xls")
        self.assertEqual(report["file_size_bytes"], 7)
        self.assertEqual(report["records_total"], 1)
        self.assertEqual(report["records_by_sheet"], {"Tammikuu-Januari": 1})
        self.assertEqual(report["rows_with_issues"], 1)
        self.assertEqual(report["quality_issues_by_field_code"], {"Päätöspäivä:invalid_date": 1})
        self.assertNotIn("AUDI", repr(report))


if __name__ == "__main__":
    unittest.main()
