-- Bootstrap tables and a first Gold reporting view in the existing dataset.
-- Run this script manually in BigQuery after selecting the project's location.
-- Re-running is safe for table creation; CREATE TABLE IF NOT EXISTS does not
-- alter an existing table schema. The Gold view is replaced on each run.

CREATE TABLE IF NOT EXISTS `airflow-verohallinto-demo.autot_veroarvot.bronze_vehicle_tax_rows` (
  source_row_id STRING NOT NULL,
  source_file_sha256 STRING NOT NULL,
  source_file_name STRING NOT NULL,
  source_url STRING NOT NULL,
  decision_year INT64 NOT NULL,
  source_sheet STRING NOT NULL,
  source_row_number INT64 NOT NULL,
  retrieved_at_utc TIMESTAMP NOT NULL,
  raw_values JSON NOT NULL,
  loaded_at_utc TIMESTAMP NOT NULL
)
OPTIONS (
  description = 'Bronze parsed source rows. Original XLS bytes are retained separately in Cloud Storage.'
);

CREATE TABLE IF NOT EXISTS `airflow-verohallinto-demo.autot_veroarvot.silver_vehicle_tax_values` (
  source_row_id STRING NOT NULL,
  source_file_sha256 STRING NOT NULL,
  source_file_name STRING NOT NULL,
  decision_year INT64 NOT NULL,
  source_sheet STRING NOT NULL,
  source_row_number INT64 NOT NULL,
  retrieved_at_utc TIMESTAMP NOT NULL,
  brand STRING,
  model STRING,
  engine_displacement_cm3 INT64,
  fuel_type STRING,
  door_count INT64,
  body_style STRING,
  drivetrain STRING,
  transmission STRING,
  power_kw INT64,
  individual_features STRING,
  decision_date DATE,
  first_registration_date DATE,
  odometer_1000_km NUMERIC,
  odometer_km NUMERIC,
  tax_value NUMERIC,
  tax_rate_percent NUMERIC,
  vehicle_tax_amount NUMERIC,
  quality_issue_count INT64 NOT NULL,
  loaded_at_utc TIMESTAMP NOT NULL
)
OPTIONS (
  description = 'Silver typed passenger-car tax-decision records. Amount fields have no currency suffix because the workbook headers do not state a currency.'
);

CREATE TABLE IF NOT EXISTS `airflow-verohallinto-demo.autot_veroarvot.silver_quality_issues` (
  source_row_id STRING NOT NULL,
  source_file_sha256 STRING NOT NULL,
  source_sheet STRING NOT NULL,
  source_row_number INT64 NOT NULL,
  field_name STRING NOT NULL,
  issue_code STRING NOT NULL,
  issue_message STRING NOT NULL,
  detected_at_utc TIMESTAMP NOT NULL
)
OPTIONS (
  description = 'One row per validation issue; invalid source rows are retained in Silver rather than silently dropped.'
);

CREATE TABLE IF NOT EXISTS `airflow-verohallinto-demo.autot_veroarvot.pipeline_runs` (
  run_id STRING NOT NULL,
  source_url STRING NOT NULL,
  source_file_name STRING,
  source_file_sha256 STRING,
  decision_year INT64,
  started_at_utc TIMESTAMP NOT NULL,
  finished_at_utc TIMESTAMP,
  status STRING NOT NULL,
  rows_read INT64,
  rows_loaded INT64,
  rows_with_issues INT64,
  quality_issues_total INT64,
  error_message STRING
)
OPTIONS (
  description = 'Airflow run audit metadata; never store credentials or tokens here.'
);

CREATE OR REPLACE VIEW `airflow-verohallinto-demo.autot_veroarvot.gold_tax_value_summary_by_model_month` AS
SELECT
  decision_year,
  DATE_TRUNC(decision_date, MONTH) AS decision_month,
  brand,
  model,
  COUNT(*) AS decision_count,
  AVG(tax_value) AS average_tax_value,
  APPROX_QUANTILES(tax_value, 100)[OFFSET(50)] AS median_tax_value,
  MIN(tax_value) AS minimum_tax_value,
  MAX(tax_value) AS maximum_tax_value,
  AVG(odometer_km) AS average_odometer_km
FROM `airflow-verohallinto-demo.autot_veroarvot.silver_vehicle_tax_values`
WHERE tax_value IS NOT NULL
GROUP BY decision_year, decision_month, brand, model;
