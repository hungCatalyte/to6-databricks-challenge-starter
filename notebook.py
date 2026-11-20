# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

pwd

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/hung.q.phan.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------



# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# DBTITLE 1,1a. Catalog descriptions, lineage and sample rows
# 1a. Source reference: Catalog descriptions, supplied lineage view, sample rows
for t in (INITIAL_TABLE, CORRECTED_TABLE, LINEAGE_VIEW):
    comment = spark.sql(f"DESCRIBE TABLE EXTENDED {t}").filter("col_name = 'Comment'").select("data_type").first()
    print(t, "->", comment[0] if comment else "(no description)")

display(spark.table(LINEAGE_VIEW))

# At least three sample rows from each batch
display(spark.table(INITIAL_TABLE).orderBy("tax_year", "return_id").limit(5))
display(spark.table(CORRECTED_TABLE).orderBy("tax_year", "return_id").limit(5))

# COMMAND ----------

# DBTITLE 1,1b. Full-batch profile of both deliveries
# 1b. Inspect the full batch, not just samples: by year, missing required values, duplicate IDs, zero income
REQUIRED = ["return_id", "tax_year", "income", "deduction"]
missing_any = " OR ".join(f"{c} IS NULL" for c in REQUIRED)

for t in (INITIAL_TABLE, CORRECTED_TABLE):
    print("==", t)
    display(spark.sql(f"""
        SELECT tax_year,
               COUNT(*)                                          AS rows,
               COUNT(DISTINCT return_id)                         AS distinct_return_ids,
               SUM(CASE WHEN {missing_any} THEN 1 ELSE 0 END)    AS rows_missing_required,
               SUM(CASE WHEN income = 0 THEN 1 ELSE 0 END)       AS zero_income_rows,
               SUM(CASE WHEN income < 0 OR deduction < 0 THEN 1 ELSE 0 END) AS negative_amount_rows
        FROM {t} GROUP BY tax_year ORDER BY tax_year"""))
    # The actual affected records
    display(spark.sql(f"SELECT *, 'missing required value' AS finding FROM {t} WHERE {missing_any}"))
    display(spark.sql(f"""SELECT t.*, 'duplicate return_id' AS finding FROM {t} t
        JOIN (SELECT return_id FROM {t} GROUP BY return_id HAVING COUNT(*) > 1) d USING (return_id)"""))
    display(spark.sql(f"SELECT *, 'zero income (valid)' AS finding FROM {t} WHERE income = 0"))

# COMMAND ----------

# DBTITLE 1,1c. What the input tells us
# 1c. Summarize the findings from 1a/1b side by side and state what they mean for steps 2 and 3
from pyspark.sql import functions as F

REQUIRED = ["return_id", "tax_year", "income", "deduction"]
missing_cond = " OR ".join(f"{c} IS NULL" for c in REQUIRED)

def batch_findings(table):
    df = spark.table(table)
    dup_ids = [r.return_id for r in df.groupBy("return_id").count().filter("count > 1").collect()]
    missing = df.filter(missing_cond).collect()
    missing_desc = [f"{r.return_id} ({', '.join(c for c in REQUIRED if r[c] is None)} null)" for r in missing]
    zero_income = [f"{r.return_id} ({r.tax_year})" for r in df.filter("income = 0").orderBy("return_id").collect()]
    by_year = {r.tax_year: r["count"] for r in df.groupBy("tax_year").count().orderBy("tax_year").collect()}
    return {
        "batch": table.split(".")[-1],
        "rows": df.count(),
        "distinct_return_ids": df.select("return_id").distinct().count(),
        "rows_by_year": ", ".join(f"{y}: {n}" for y, n in by_year.items()),
        "missing_required": "; ".join(missing_desc) or "none",
        "duplicate_return_ids": ", ".join(sorted(dup_ids)) or "none",
        "zero_income_returns": ", ".join(zero_income) or "none",
        "negative_amount_rows": df.filter("income < 0 OR deduction < 0").count(),
        "passes_completeness": len(missing) == 0,
        "passes_uniqueness": len(dup_ids) == 0,
    }

findings = [batch_findings(t) for t in (INITIAL_TABLE, CORRECTED_TABLE)]
display(spark.createDataFrame(findings).select(
    "batch", "rows", "distinct_return_ids", "rows_by_year", "missing_required",
    "duplicate_return_ids", "zero_income_returns", "negative_amount_rows",
    "passes_completeness", "passes_uniqueness"))

init, corr = findings
print("Source: both deliveries are built by SQL from training.to6_challenge3_inputs.parsed_source (source_lineage view).")
print("Row meaning: one row = one return; return_id is the unique key; taxpayer_id is the filer and can repeat across years.")
print()
print("What this means for the next steps:")
print(f"* Step 2: the initial batch fails completeness ({init['missing_required']}) and uniqueness "
      f"(duplicate return_id {init['duplicate_return_ids']}). It must be rejected with these records and reasons kept; "
      "no filling nulls with 0 and no silent de-duplication.")
print(f"* Step 2: the corrected batch shows completeness={corr['passes_completeness']}, uniqueness={corr['passes_uniqueness']} "
      "in this profile, but the same checks must still be applied to it formally.")
print(f"* Step 3: zero income is valid ({corr['zero_income_returns']}). These returns must stay in return_features "
      "with a null deduction_to_income instead of a divide-by-zero.")
print(f"* Negative amounts: {init['negative_amount_rows']} (initial), {corr['negative_amount_rows']} (corrected), so no extra handling is needed now.")

# COMMAND ----------

# DBTITLE 1,1c. What the input tells us
# MAGIC %md
# MAGIC ### 1c. Findings and what they mean for the next steps
# MAGIC
# MAGIC **Source reference.** Both deliveries, `training.to6_challenge3_inputs.returns_initial` and `returns_corrected`, were created by SQL from the upstream table `training.to6_challenge3_inputs.parsed_source` (from the `source_lineage` view). The Catalog description of `returns_initial` warns of known delivery defects. The description of `returns_corrected` says one row per return.
# MAGIC
# MAGIC **Row meaning and fields.** One row = one tax return. `return_id` is the uniqueness identifier. `taxpayer_id` is the filer, who can have returns in several years (for example F0 files R01 in 2022 and R05 in 2023), so it is **not** unique. Required fields: `return_id`, `tax_year`, `income`, `deduction`. Years covered: 2022 to 2024.
# MAGIC
# MAGIC **Full-batch profile.**
# MAGIC
# MAGIC | Batch | Rows | Distinct return_id | Missing required | Duplicate IDs | Zero income | Negative amounts |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | initial | 13 (2022: 5, 2023: 4, 2024: 4) | 12 | 1 (R02, `income` null) | R01 appears twice (2022) | 2 (R03 2022, R09 2024) | 0 |
# MAGIC | corrected | 12 (4 per year) | 12 | 0 | none | 2 (R03, R09) | 0 |
# MAGIC
# MAGIC In `returns_corrected`, R01 appears once and R02 has `income` 200.
# MAGIC
# MAGIC **What this means for step 2 and step 3.**
# MAGIC * The initial batch breaks both rules: R02 has no `income`, and R01 appears twice. The completeness and uniqueness checks in step 2 must reject it, and keep these records with the reason. Do not fill R02's income with 0 or drop the duplicate quietly.
# MAGIC * The corrected batch passes on this profile, but step 2 still has to apply the same checks to it.
# MAGIC * Zero income is valid. Those returns stay in `return_features` with a null `deduction_to_income`, so the ratio must not divide by zero.
# MAGIC * No negative amounts were found, but the nonnegative rule stays a check.
# MAGIC * `taxpayer_id`, `income` and `deduction` must not appear in the output tables (need-to-know rule).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How the two batches work and how to decide which is usable
# MAGIC
# MAGIC **Why are there two tables?**  
# MAGIC The challenge simulates a real-world data-delivery scenario:
# MAGIC
# MAGIC 1. **`returns_initial`** — the *first* delivery from the upstream source. Think of it as "the data team sent us their first extract." Your job is to run **quality checks** (completeness, uniqueness, non-negative amounts) on it. As we found in step 1, this batch **fails**:
# MAGIC    - `R02` has a NULL `income` → completeness violation  
# MAGIC    - `R01` appears twice → uniqueness violation  
# MAGIC
# MAGIC 2. **`returns_corrected`** — a *replacement* delivery sent after the first one was rejected. The upstream team fixed the problems and re-sent the data. Your job is to run the **same checks** on this batch to confirm it actually passes.
# MAGIC
# MAGIC **The decision process (step 2):**
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐     Completeness + Uniqueness     ┌──────────┐
# MAGIC │  returns_initial     │ ──────── checks ────────────────▶ │  REJECT  │
# MAGIC └─────────────────────┘     (fails: null income, dup ID)   └──────────┘
# MAGIC
# MAGIC ┌─────────────────────┐     Same checks                    ┌──────────┐
# MAGIC │  returns_corrected   │ ──────── checks ────────────────▶ │  ACCEPT  │
# MAGIC └─────────────────────┘     (passes all)                   └──────────┘
# MAGIC ```
# MAGIC
# MAGIC 1. **Check the initial batch** → it fails → save the rejection evidence (which rows failed and why)  
# MAGIC 2. **Check the corrected batch** → it passes → use it as the input for step 3 (feature table)  
# MAGIC
# MAGIC **Key rules:**
# MAGIC - Do NOT fix the initial batch (don't fill nulls, don't silently de-dup) — reject it and record *why*  
# MAGIC - Apply the exact same checks to the corrected batch — don't assume it's clean just because of its name  
# MAGIC - Keep the rejection evidence (the bad rows + reasons) as an output artifact
# MAGIC
# MAGIC So you're not "parsing one input into two tables." The two tables represent **two separate deliveries** of the same dataset — the first was defective, the second is the fix. Your code must programmatically prove which one passes the quality gate.

# COMMAND ----------

# DBTITLE 1,2a. Quality rules (same rules for both batches)
# 2a. Data-contract rules applied identically to every batch.
#   completeness: return_id, tax_year, income, deduction must all be present
#   uniqueness:   each return_id occurs exactly once
#   validity:     income and deduction are nonnegative (zero income is valid)
import json, datetime
from pyspark.sql import functions as F, Window

REQUIRED = ["return_id", "tax_year", "income", "deduction"]
RULES = ["completeness", "uniqueness", "validity"]

def quality_failures(table):
    """Return every row that breaks a rule, with the rule and a human-readable reason. Nothing is filled or dropped."""
    df = spark.table(table)
    missing_cols = F.concat_ws(", ", *[F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED])
    completeness = (df.filter(" OR ".join(f"{c} IS NULL" for c in REQUIRED))
                      .withColumn("rule", F.lit("completeness"))
                      .withColumn("reason", F.concat(F.lit("missing required value: "), missing_cols)))
    n = F.count("*").over(Window.partitionBy("return_id"))
    uniqueness = (df.filter("return_id IS NOT NULL").withColumn("n", n).filter("n > 1")
                    .withColumn("rule", F.lit("uniqueness"))
                    .withColumn("reason", F.concat(F.lit("return_id appears "), F.col("n").cast("string"), F.lit(" times")))
                    .drop("n"))
    validity = (df.filter("income < 0 OR deduction < 0")
                  .withColumn("rule", F.lit("validity"))
                  .withColumn("reason", F.lit("negative amount")))
    return (completeness.unionByName(uniqueness).unionByName(validity)
            .withColumn("source_table", F.lit(table))
            .orderBy("rule", "return_id"))

def assess(table):
    """Run all rules; return (failures_df, per-rule summary rows, accepted flag)."""
    failures = quality_failures(table)
    counts = {r.rule: r["count"] for r in failures.groupBy("rule").count().collect()}
    summary = [{"batch": table, "rule": r, "failed_rows": int(counts.get(r, 0)),
                "result": "FAIL" if counts.get(r, 0) else "PASS"} for r in RULES]
    return failures, summary, not counts

def run_reference():
    """Notebook URL + UTC timestamp for this execution (evidence for SUBMISSION.md)."""
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    try:
        host = spark.conf.get("spark.databricks.workspaceUrl")
        nb_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookId().get()
        url = f"https://{host}/editor/notebooks/{nb_id}"
    except Exception:
        url = "unavailable"
    return {"executed_at_utc": ts, "notebook_url": url}

class BatchRejected(Exception):
    pass

def require_usable(table):
    """Gate: raise BatchRejected if any rule fails, so no feature output can be written from this batch."""
    failures, summary, ok = assess(table)
    if not ok:
        detail = "; ".join(f"{r.return_id} [{r.rule}] {r.reason}" for r in failures.collect())
        raise BatchRejected(f"{table} rejected. Failed rules: "
                            f"{[s['rule'] for s in summary if s['result'] == 'FAIL']}. Records: {detail}")
    return summary

# COMMAND ----------

# DBTITLE 1,2b. Initial batch: gate rejects it, keep affected records
# 2b. Assess the initial batch. The gate must reject it before any feature output exists.
init_failures, init_summary, init_ok = assess(INITIAL_TABLE)
rejection_ref = run_reference()

try:
    require_usable(INITIAL_TABLE)
    raise AssertionError("Expected returns_initial to be rejected, but it passed the checks")
except BatchRejected as e:
    rejection_message = str(e)
    print("REJECTED:", rejection_message)
    print("Run reference:", rejection_ref)

display(spark.createDataFrame(init_summary))

# Preserve the affected records exactly as delivered, with rule + reason (overwrite: reruns never append copies)
(init_failures.withColumn("checked_at_utc", F.lit(rejection_ref["executed_at_utc"]))
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{OUTPUT_SCHEMA}.initial_rejected_records"))
display(spark.table(f"{OUTPUT_SCHEMA}.initial_rejected_records"))

# Small evidence exports for the repo
out_dir = repo_root / "outputs"
init_failures.toPandas().astype({"tax_year": "Int64", "income": "Int64", "deduction": "Int64"}).to_csv(out_dir / "initial_rejected_records.csv", index=False)
(out_dir / "initial_rejection.json").write_text(json.dumps({
    **rejection_ref, "batch": INITIAL_TABLE, "decision": "REJECTED",
    "message": rejection_message, "rule_results": init_summary}, indent=2))
print("Wrote", out_dir / "initial_rejected_records.csv", "and", out_dir / "initial_rejection.json")

# COMMAND ----------

# DBTITLE 1,2c. Corrected batch: same rules, accept for the final run
# 2c. Apply exactly the same rules to the replacement. require_usable raises (and stops the run
#     before step 3) if anything fails, so feature output can only come from an accepted batch.
corr_failures, corr_summary, corr_ok = assess(CORRECTED_TABLE)
print("Failing rows in returns_corrected:", corr_failures.count())
require_usable(CORRECTED_TABLE)
ACCEPTED_TABLE = CORRECTED_TABLE
print("ACCEPTED:", ACCEPTED_TABLE)

# Side-by-side decision record for both batches
assessment = (spark.createDataFrame(init_summary + corr_summary)
    .withColumn("decision", F.when(F.col("batch") == INITIAL_TABLE, F.lit("ACCEPTED" if init_ok else "REJECTED"))
                             .otherwise(F.lit("ACCEPTED" if corr_ok else "REJECTED")))
    .select("batch", "rule", "failed_rows", "result", "decision"))
assessment.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{OUTPUT_SCHEMA}.batch_assessment")
assessment.toPandas().to_csv(out_dir / "batch_assessment.csv", index=False)
display(spark.table(f"{OUTPUT_SCHEMA}.batch_assessment"))

# COMMAND ----------

# DBTITLE 1,2d. Decision and next action
# MAGIC %md
# MAGIC ### 2d. Decision and next action
# MAGIC
# MAGIC **Rules (same for both batches):** completeness (`return_id`, `tax_year`, `income`, `deduction` present), uniqueness (one row per `return_id`), validity (nonnegative amounts; zero income is valid).
# MAGIC
# MAGIC **`returns_initial`: REJECTED.** The gate `require_usable` raised `BatchRejected` before any feature output was written:
# MAGIC * **R02** (2022, filer F1): `income` is null, which breaks completeness. Its true income is unknown, so filling it with 0 would invent a value.
# MAGIC * **R01** (2022, filer F0): delivered twice with identical values, which breaks uniqueness. Silently dropping one copy would hide the delivery defect.
# MAGIC
# MAGIC These affected records and their reasons are stored in `initial_rejected_records` in the output schema and exported to `outputs/initial_rejected_records.csv`. The rejection message, rule results and run reference (notebook URL and UTC timestamp) are in `outputs/initial_rejection.json`.
# MAGIC
# MAGIC **Next action:** do not repair the initial batch. Use the replacement delivery from the data owner, `returns_corrected`, and run the same rules on it.
# MAGIC
# MAGIC **`returns_corrected`: ACCEPTED.** All three rules passed with 0 failing rows (R01 once, R02 income present). `ACCEPTED_TABLE` is set to this table and is the only input for step 3. If it ever failed, `require_usable` would raise and stop the run before step 3. The per-rule decision for both batches is in `batch_assessment` and `outputs/batch_assessment.csv`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,3. Build return_features and verify by reading it back
# 3. Build return_features from the accepted batch only, then verify the saved table by reading it back.
#    Contract: exactly return_id, tax_year, deduction_to_income (= deduction / income; null when income = 0).
#    taxpayer_id, income and deduction are excluded (need-to-know rule).
assert "ACCEPTED_TABLE" in globals() and ACCEPTED_TABLE == CORRECTED_TABLE, "Run step 2 first: no accepted batch"
FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
FEATURE_COLS = ["return_id", "tax_year", "deduction_to_income"]

source = spark.table(ACCEPTED_TABLE)
features = source.select(
    "return_id", "tax_year",
    F.when(F.col("income") == 0, F.lit(None).cast("double"))
     .otherwise(F.col("deduction").cast("double") / F.col("income")).alias("deduction_to_income"))

# Overwrite (not append): a rerun replaces the table, so no copies accumulate
features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

# ---- Verification: read the saved table back ----
saved = spark.table(FEATURES_TABLE)
saved_rows = {r.return_id: r for r in saved.collect()}
source_rows = {r.return_id: r for r in source.collect()}

assert saved.columns == FEATURE_COLS, f"Unexpected columns: {saved.columns}"
assert saved.count() == len(saved_rows) == source.count(), "Row count or return_id uniqueness mismatch"
assert set(saved_rows) == set(source_rows), "Saved return_ids differ from the accepted batch"

# Independent check: recompute every ratio in plain Python from the source amounts
for rid, s in source_rows.items():
    got = saved_rows[rid]
    assert got.tax_year == s.tax_year, f"{rid}: tax_year changed"
    if s.income == 0:
        assert got.deduction_to_income is None, f"{rid}: zero income must give a null ratio"
    else:
        assert abs(got.deduction_to_income - s.deduction / s.income) < 1e-12, f"{rid}: ratio mismatch"

# Named evidence: one nonzero case and one zero-income case
nz = next(s for rid, s in sorted(source_rows.items()) if s.income > 0 and s.deduction > 0)
zi = next(s for rid, s in sorted(source_rows.items()) if s.income == 0)
verification = [
    {"check": "nonzero ratio", "return_id": nz.return_id,
     "source": f"deduction {nz.deduction} / income {nz.income}",
     "expected": str(nz.deduction / nz.income), "saved": str(saved_rows[nz.return_id].deduction_to_income)},
    {"check": "zero income kept, null ratio", "return_id": zi.return_id,
     "source": f"deduction {zi.deduction} / income {zi.income}",
     "expected": "None", "saved": str(saved_rows[zi.return_id].deduction_to_income)},
]
display(spark.createDataFrame(verification))
print(f"PASS: {FEATURES_TABLE} has {saved.count()} rows, columns {saved.columns}, "
      f"one row per accepted return, all ratios match the source.")

display(saved.orderBy("tax_year", "return_id"))
saved.orderBy("return_id").toPandas().to_csv(out_dir / "return_features.csv", index=False)
print("Wrote", out_dir / "return_features.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,4a. Year split tables from the saved features
# 4a. Build the year tables from the SAVED return_features table (not from the source), then verify them.
SPLITS = {"features_train": 2022, "features_validation": 2023, "features_test": 2024}
saved_features = spark.table(FEATURES_TABLE)

for name, year in SPLITS.items():
    (saved_features.filter(F.col("tax_year") == year)
        .write.mode("overwrite").option("overwriteSchema", "true")   # overwrite: reruns never append copies
        .saveAsTable(f"{OUTPUT_SCHEMA}.{name}"))

# ---- Verification by reading every split back ----
feature_rows = {r.return_id: r.asDict() for r in saved_features.collect()}
seen, split_summary = {}, []
for name, year in SPLITS.items():
    t = spark.table(f"{OUTPUT_SCHEMA}.{name}")
    rows = t.collect()
    assert t.columns == FEATURE_COLS, f"{name}: unexpected columns {t.columns}"
    assert all(r.tax_year == year for r in rows), f"{name}: contains a return outside {year}"
    for r in rows:
        assert r.return_id not in seen, f"{r.return_id} appears in {seen.get(r.return_id)} and {name}"
        assert r.asDict() == feature_rows[r.return_id], f"{r.return_id}: values differ from return_features"
        seen[r.return_id] = name
    split_summary.append({"table": f"{OUTPUT_SCHEMA}.{name}", "tax_year": year, "rows": len(rows),
                          "distinct_return_ids": len({r.return_id for r in rows})})
    t.orderBy("return_id").toPandas().to_csv(out_dir / f"{name}.csv", index=False)

assert set(seen) == set(feature_rows), f"Accepted returns missing from splits: {set(feature_rows) - set(seen)}"
assert sum(s["rows"] for s in split_summary) == saved_features.count()
display(spark.createDataFrame(split_summary))
print(f"PASS: all {len(feature_rows)} accepted returns appear exactly once, in the table for their tax_year, "
      "with values identical to return_features.")

# COMMAND ----------

# DBTITLE 1,4b. Run snapshot and comparison of the last two complete runs
# 4b. Rerun evidence. This is the last cell, so a snapshot exists only for a run where every earlier cell passed.
#     Each complete run saves the actual return IDs and values of all four output tables; the two latest
#     snapshots are then compared value by value (row counts alone would not prove this).
import pandas as pd

run_ref = run_reference()
run_id = datetime.datetime.fromisoformat(run_ref["executed_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
runs_dir = out_dir / "runs"
runs_dir.mkdir(exist_ok=True)

OUTPUT_TABLES = ["return_features"] + list(SPLITS)
snapshot = pd.concat([
    spark.table(f"{OUTPUT_SCHEMA}.{t}").toPandas().assign(table=t) for t in OUTPUT_TABLES
])[["table"] + FEATURE_COLS].sort_values(["table", "return_id"]).reset_index(drop=True)

# No appended copies: every table holds each return_id once
dup = snapshot.groupby(["table", "return_id"]).size()
assert (dup == 1).all(), f"Duplicate rows found: {dup[dup > 1]}"

snapshot.to_csv(runs_dir / f"run_{run_id}.csv", index=False)
(runs_dir / f"run_{run_id}.json").write_text(json.dumps({**run_ref, "run_id": run_id,
    "row_counts": snapshot.groupby("table").size().to_dict()}, indent=2))
print("Saved snapshot", runs_dir / f"run_{run_id}.csv", "|", run_ref)

snapshots = sorted(runs_dir.glob("run_*.csv"))
if len(snapshots) < 2:
    print("Only one complete run so far. Run the whole notebook again to produce the comparison.")
else:
    prev_path, curr_path = snapshots[-2], snapshots[-1]
    prev, curr = pd.read_csv(prev_path), pd.read_csv(curr_path)
    diff = prev.merge(curr, on=["table", "return_id"], how="outer", suffixes=("_run1", "_run2"), indicator=True)
    mismatched = diff[(diff["_merge"] != "both")
                      | (diff["tax_year_run1"] != diff["tax_year_run2"])
                      | ~((diff["deduction_to_income_run1"] == diff["deduction_to_income_run2"])
                          | (diff["deduction_to_income_run1"].isna() & diff["deduction_to_income_run2"].isna()))]
    identical = mismatched.empty and prev.equals(curr)
    comparison = {
        "run_1": json.loads(prev_path.with_suffix(".json").read_text()),
        "run_2": json.loads(curr_path.with_suffix(".json").read_text()),
        "snapshot_files": [f"outputs/runs/{prev_path.name}", f"outputs/runs/{curr_path.name}"],
        "rows_compared": int(len(diff)),
        "mismatched_rows": int(len(mismatched)),
        "identical_ids_and_values": bool(identical),
    }
    (out_dir / "run_comparison.json").write_text(json.dumps(comparison, indent=2))
    display(spark.createDataFrame(snapshot.groupby("table").size().reset_index(name="rows")))
    print(json.dumps(comparison, indent=2))
    assert identical, f"Runs differ: {mismatched}"