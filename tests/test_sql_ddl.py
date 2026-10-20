import re
import unittest
from pathlib import Path


SQL_PATH = Path(__file__).parents[1] / "sql" / "00_create_tables_and_gold_view.sql"


class BigQueryDdlTests(unittest.TestCase):
    def test_bootstrap_ddl_targets_existing_dataset_and_declares_all_layers(self):
        sql = SQL_PATH.read_text(encoding="utf-8")
        normalized = re.sub(r"\s+", " ", sql).casefold()

        for table in (
            "bronze_vehicle_tax_rows",
            "silver_vehicle_tax_values",
            "silver_quality_issues",
            "pipeline_runs",
        ):
            self.assertIn(
                f"create table if not exists `airflow-verohallinto-demo.autot_veroarvot.{table}`",
                normalized,
            )

        self.assertIn(
            "create or replace view `airflow-verohallinto-demo.autot_veroarvot.gold_tax_value_summary_by_model_month`",
            normalized,
        )
        self.assertIn("raw_values json not null", normalized)
        self.assertIn("decision_date date", normalized)
        self.assertIn("odometer_km numeric", normalized)
        self.assertIn("tax_value numeric", normalized)

    def test_gold_view_uses_only_confirmed_fields_and_keeps_currency_agnostic_names(self):
        sql = SQL_PATH.read_text(encoding="utf-8").casefold()

        self.assertIn("from `airflow-verohallinto-demo.autot_veroarvot.silver_vehicle_tax_values`", sql)
        self.assertIn("date_trunc(decision_date, month)", sql)
        self.assertIn("avg(odometer_km)", sql)
        self.assertNotIn("co2", sql)
        self.assertNotIn("tax_value_eur", sql)


if __name__ == "__main__":
    unittest.main()
