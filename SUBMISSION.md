# My dataset submission

Authored in Databricks. Implementation and evidence are in `notebook.py` (Databricks notebook `notebook`).

## Inputs and outputs

**Inputs (read-only)**

| Role | Table |
| --- | --- |
| Initial delivery (rejected) | `training.to6_challenge3_inputs.returns_initial` |
| Corrected delivery (accepted) | `training.to6_challenge3_inputs.returns_corrected` |
| Supplied lineage view | `training.to6_challenge3_inputs.source_lineage` |
| Upstream source (from lineage view) | `training.to6_challenge3_inputs.parsed_source` |

**Outputs** (schema `training.to6_c3_89287c814847`)

| Table | Contents |
| --- | --- |
| `return_features` | 12 accepted returns: `return_id`, `tax_year`, `deduction_to_income` |
| `features_train` | 2022 returns (4) |
| `features_validation` | 2023 returns (4) |
| `features_test` | 2024 returns (4) |
| `initial_rejected_records` | the 3 affected initial records with rule and reason |
| `batch_assessment` | per-rule PASS/FAIL and decision for both batches |

**Row meaning.** One row is one tax return. `return_id` is the unique identifier. `taxpayer_id` identifies the filer, who can have returns in several years, so it is not unique.

**Ratio definition.** `deduction_to_income = deduction / income`, computed as a double. When `income = 0` the return is kept and the ratio is null. `taxpayer_id`, `income` and `deduction` are excluded from all feature and split tables (need-to-know rule).

## Checks and decisions

**Source references (notebook step 1).** The Catalog descriptions say `returns_initial` has known delivery defects and `returns_corrected` has one row per return. The `source_lineage` view shows both are derived from `parsed_source`. Sample rows and a full-batch profile (rows by year, missing required values, duplicate IDs, zero income) are in cells 1a to 1c.

**Rules (step 2a), applied identically to both batches**

* Completeness: `return_id`, `tax_year`, `income`, `deduction` all present.
* Uniqueness: each `return_id` occurs exactly once.
* Validity: `income` and `deduction` are nonnegative; zero income is valid.

**Initial batch: REJECTED (step 2b).** The gate `require_usable` raised `BatchRejected` before any feature output was written.

| return_id | tax_year | Rule | Reason |
| --- | --- | --- | --- |
| R02 | 2022 | completeness | missing required value: income |
| R01 | 2022 | uniqueness | return_id appears 2 times (both rows kept) |

The batch was not repaired: R02's income was not replaced with 0 and the duplicate R01 was not dropped. Evidence: `outputs/initial_rejected_records.csv`, `outputs/initial_rejection.json` (rejection message, rule results, notebook URL and UTC timestamp) and table `initial_rejected_records`. Next action: use the replacement delivery.

**Corrected batch: ACCEPTED (step 2c).** The same rules found 0 failing rows (R01 once, R02 income 200). Evidence: `outputs/batch_assessment.csv`.

**Feature table verification (step 3)**, by reading `return_features` back after saving:

* Columns are exactly `return_id`, `tax_year`, `deduction_to_income`; 12 rows, 12 distinct IDs, same IDs as the accepted batch.
* Every ratio was recomputed in plain Python from the source amounts and matched.
* Nonzero case: R01, deduction 20 / income 100, expected 0.2, saved 0.2.
* Zero-income case: R03, deduction 46 / income 0, kept with a null ratio (R09 also has zero income and a null ratio).
* Separate SQL cross-check against `returns_corrected`: maximum difference between saved and source ratio 0.0, and both zero-income returns null.

## Two successful runs

Both were complete interactive runs of the whole notebook (no Jobs).

| Run | Notebook URL | Executed at (UTC) | Snapshot |
| --- | --- | --- | --- |
| 1 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558985 | 2026-11-20T17:18:11+00:00 | `outputs/runs/run_20261120T171811Z.csv` |
| 2 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558985 | 2026-11-20T17:20:14+00:00 | `outputs/runs/run_20261120T172014Z.csv` |

Each snapshot holds the actual `return_id`, `tax_year` and `deduction_to_income` of `return_features` and the three year tables (24 rows). The last notebook cell (4b) writes a snapshot only after every earlier cell has passed, then compares the two latest snapshots row by row. `outputs/run_comparison.json` records 24 rows compared, 0 mismatched, `identical_ids_and_values: true`.

**How the handoff was checked (step 4a and a separate SQL check)**

* Year assignment: every row of `features_train` has `tax_year` 2022, `features_validation` 2023 and `features_test` 2024 (SQL check: 0 rows in the wrong year).
* One copy of every accepted return: the three year tables together hold 12 rows and 12 distinct IDs, and each row matches `return_features` exactly (SQL `EXCEPT ALL` in both directions: 0 rows).
* No appended copies: all output tables are written with overwrite mode, and cell 4b asserts each `return_id` appears once per table. Row counts after run 2 were the same as after run 1 (12, 4, 4, 4).

## Final files and limitations

**Final exports under `outputs/`**

* `return_features.csv`, `features_train.csv`, `features_validation.csv`, `features_test.csv`
* `initial_rejected_records.csv`, `initial_rejection.json`
* `batch_assessment.csv`
* `run_comparison.json`
* `runs/run_20261120T171811Z.csv` and `.json`, `runs/run_20261120T172014Z.csv` and `.json`

**Practice file (not part of the assessment):** `outputs/connection-practice.json`.

**Not verified / limitations**

* `initial_rejected_records.csv` keeps `taxpayer_id` and raw amounts because it is evidence of the faulty delivery, not feature output.
* `initial_rejection.json` is rewritten on each run; it holds the rejection reference of the latest run.
* I did not inspect `parsed_source` directly; the upstream source is taken from the supplied lineage view.
* Both runs were made in the same notebook and the URL is the same for both; runs are distinguished by timestamp.
* These checks set the data contract only. They do not prove the absence of model leakage; target labels and prediction-time availability must be assessed in later modelling work.
* The final commit SHA is reported in Classroom, not here.
