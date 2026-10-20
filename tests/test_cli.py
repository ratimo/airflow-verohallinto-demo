import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from tax_values.cli import main


class CliTests(unittest.TestCase):
    def test_inspect_command_prints_json_report(self):
        report = {"source_file": "sample.xls", "records_total": 3}
        output = StringIO()
        with patch("tax_values.cli.inspect_file", return_value=report):
            with redirect_stdout(output):
                status = main(["inspect", "sample.xls"])

        self.assertEqual(status, 0)
        self.assertEqual(json.loads(output.getvalue()), report)


if __name__ == "__main__":
    unittest.main()
