import unittest

from tax_values.xls_parser import parse_workbook


HEADERS = [
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
]


class FakeSheet:
    def __init__(self, name, rows):
        self.name = name
        self.rows = rows
        self.nrows = len(rows)
        self.ncols = max((len(row) for row in rows), default=0)

    def cell_value(self, row_index, col_index):
        row = self.rows[row_index]
        return row[col_index] if col_index < len(row) else ""


class FakeWorkbook:
    def __init__(self, sheets):
        self._sheets = sheets

    def sheets(self):
        return self._sheets


class XlsParserTests(unittest.TestCase):
    def test_skips_cover_and_blank_month_and_preserves_source_row_and_values(self):
        cover = FakeSheet("Etusivu", [["Taulukoita sovelletuista verotusarvoista"]])
        january = FakeSheet(
            "Tammikuu-Januari",
            [
                [""],
                HEADERS,
                ["AUDI", "A3", 1498, "Bensiini", 5, "Hatchback", "Etuveto", "AUT", 110, "", 20260112, 20240205, 20, 28224.0, 11.1, 3132.86],
            ],
        )
        empty_month = FakeSheet("Helmikuu-Februari", [HEADERS])

        parsed = parse_workbook(FakeWorkbook([cover, january, empty_month]))

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].source_sheet, "Tammikuu-Januari")
        self.assertEqual(parsed[0].source_row, 3)
        self.assertEqual(parsed[0].values["Merkki"], "AUDI")
        self.assertEqual(parsed[0].values["Päätöspäivä"], 20260112)
        self.assertEqual(parsed[0].values["Ajokm/1000"], 20)
        self.assertEqual(parsed[0].values["Vaihteisto"], "AUT")

    def test_raises_when_a_data_sheet_header_changes(self):
        changed_headers = HEADERS[:-1]
        changed = FakeSheet("Tammikuu-Januari", [changed_headers])

        with self.assertRaisesRegex(ValueError, "incomplete header"):
            parse_workbook(FakeWorkbook([changed]))


if __name__ == "__main__":
    unittest.main()
