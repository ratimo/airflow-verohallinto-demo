# Source data: passenger-car applied tax values

## Source identity

- Publisher: Finnish Tax Administration (Verohallinto)
- Source page: <https://www.vero.fi/henkiloasiakkaat/omaisuus/autovero/autoveron-maara/ajoneuvojen-sovellettuja-verotusarvoja/>
- Current source file: `henkiloautojen-sovelletut-verotusarvot-2026.xls`
- Direct workbook URL: <https://www.vero.fi/contentassets/f3326d05d3954bed9c1686a17917754d/henkiloautojen-sovelletut-verotusarvot-2026.xls>
- Inspected: 2026-10-20
- Workbook's internal last-saved metadata: 2026-10-06
- SHA-256 of inspected copy: `cd662c1c49f70515aa370163c75ac40cea6c5263ad539a1e96448eb353a82bed`
- Observed file size: 8,132,608 bytes

The page labels the year by the year in which the tax decision was made, not by the vehicle model year. The 2026 workbook contains 13 sheets: a cover sheet and 12 bilingual monthly sheets. The inspected copy has populated data for January–September (29,114 rows total) and empty October–December sheets.

## Observed source columns

| Source column | Current interpretation | Notes |
|---|---|---|
| `Merkki` | Make/brand | Text |
| `Malli` | Model | Text; repeated make/model combinations are expected |
| `Cm3` | Engine displacement | Numeric; source examples include zero for electric vehicles |
| `Käyttövoima` | Fuel/power source | Text categories, including combined powertrains |
| `OvienLkm` | Number of doors | Numeric |
| `Korimalli` | Body style | Text |
| `Vetotapa` | Drivetrain | Text |
| `Vaihteisto` | Transmission | Text or blank; values such as `AUT` occur |
| `Kw` | Engine/motor power | Numeric, kilowatts as indicated by the field name |
| `Yksilölliset ominaisuudet` | Individual characteristics | Text or blank; do not infer completeness |
| `Päätöspäivä` | Tax decision date | Text in `YYYYMMDD` format in the inspected workbook |
| `Ensirekisteröintipäivä` | First registration date | Text in `YYYYMMDD` format in the inspected workbook |
| `Ajokm/1000` | Mileage in thousands of kilometres | Numeric; preserve this source value and derive kilometres only as a separate field |
| `Verotusarvo` | Applied tax value | Numeric; the column heading does not explicitly state a currency unit |
| `Veroprosentti` | Tax percentage | Numeric percentage points, e.g. `18.3` |
| `Autovero` | Vehicle tax amount | Numeric; the column heading does not explicitly state a currency unit |

The official `.xls` is the authoritative source artifact. The direct-to-BigQuery design preserves parsed raw cell values plus file hash and sheet/row lineage in Bronze, but not the original workbook bytes. The task-local copy is not guaranteed to persist. The current normalized field names deliberately do not assume an EUR suffix because the inspected column headings do not state the currency.

## Limitations and quality controls

The workbook's cover sheet says the table is based on tax decisions; vehicle condition and equipment affect general retail value but are not shown for technical reasons. It also states that the table includes vehicles with tax benefits and is not updated when a tax decision is changed after assessment. These caveats must accompany analytics and reports.

The 2026 source examples use numeric cells for displacement, door count, power, mileage, tax value, rate, and vehicle tax. Dates are stored as text like `20260112`. The parser detects the expected 16-column header; a missing/changed header raises a schema error rather than treating a partial table as valid. Empty monthly sheets are skipped. The normalizer retains raw values, converts observed date/numeric formats, and reports invalid values per field without dropping the row.

The current local inspection found 29,114 populated rows and no normalization issues in the inspected 2026 file. Row-level errors are not currently written to a persistent report; that belongs in the later Airflow/warehouse implementation.

## Update detection

The source page's displayed update date (11 May 2026 at inspection) predates the workbook's internal last-saved metadata (6 October 2026). Do not rely on the page date alone. For subsequent checks, discover the current passenger-car link from the page, download only the selected workbook, calculate SHA-256, and skip downstream processing if the content hash is unchanged. Record URL, retrieval time, filename, hash, and available HTTP metadata. The original workbook is not committed to the repository; durable archival requires a separately configured persistent location.

## License and distribution

The project is for the user's own use. This page's source-specific reuse license was not established during inspection; do not publish the workbook or its row-level data in a public dashboard/repository without separately confirming the applicable terms. Always attribute the source and keep the tax-value limitations visible.
