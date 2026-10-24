import ast
import unittest
from pathlib import Path


DAG_PATH = Path(__file__).parents[1] / "dags" / "verohallinto_ingestion.py"
EXPECTED_TASKS = ["discover_and_download", "parse_source_rows", "load_bronze"]


class IngestionDagScopeTests(unittest.TestCase):
    def test_dag_only_downloads_parses_and_loads_bronze(self):
        source = DAG_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        dag_function = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "verohallinto_vehicle_tax_ingestion"
        )
        task_functions = [
            node.name for node in dag_function.body if isinstance(node, ast.FunctionDef)
        ]
        parse_function = next(
            node
            for node in dag_function.body
            if isinstance(node, ast.FunctionDef) and node.name == "parse_source_rows"
        )
        rejects_empty_parse = any(
            isinstance(node, ast.If)
            and isinstance(node.test, ast.UnaryOp)
            and isinstance(node.test.op, ast.Not)
            and isinstance(node.test.operand, ast.Name)
            and node.test.operand.id == "rows"
            and any(isinstance(child, ast.Raise) for child in node.body)
            for node in ast.walk(parse_function)
        )

        self.assertEqual(task_functions, EXPECTED_TASKS)
        self.assertTrue(rejects_empty_parse)
        self.assertIn("row.values", source)
        self.assertNotIn("row.raw_values", source)
        self.assertNotIn("silver_vehicle_tax_values", source)
        self.assertNotIn("silver_quality_issues", DAG_PATH.read_text(encoding="utf-8"))
        self.assertNotIn("pipeline_runs", DAG_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
