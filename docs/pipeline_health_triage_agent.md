# Pipeline Health + Data Quality Triage Agent

## What It Does

This MVP adds a deterministic run-triage layer for the Airflow pipeline in
`dags/nba_analytics_dag.py`.

For each run it:

- collects task-level states from Airflow
- reuses existing task return payloads for row counts, DQ metrics, and merge reconciliation
- classifies one primary status:
  - `healthy`
  - `ingestion_failure`
  - `dq_failure`
  - `merge_reconciliation_failure`
  - `dbt_failure`
  - `downstream_publish_failure`
  - `unknown_failure`
- produces operator next steps from deterministic rules
- writes a machine-readable JSON artifact

No LLM call is required. The human summary is rendered from the structured
artifact only.

## How It Decides Status

The triage logic lives in [dags/nba_pipeline_triage.py](/Users/mikeh/Documents/Coding/NBA_GCP/dags/nba_pipeline_triage.py).

The classifier works in this order:

1. If the DAG run finished successfully with no failed tasks, classify as `healthy`.
2. If DQ metrics show a hard-gate issue such as zero rows, null business keys, or duplicate business keys, classify as `dq_failure`.
3. Otherwise, find the earliest failed stage and map it to a primary failure type:
   - extract/load stages -> `ingestion_failure`
   - DQ stages -> `dq_failure`
   - merge stages -> `merge_reconciliation_failure`
   - dbt stage -> `dbt_failure`
   - similarity, analysis snapshot, Redshift sync, or metadata publish -> `downstream_publish_failure`
4. If no deterministic mapping applies, fall back to `unknown_failure`.

Evidence is attached directly from Airflow task states plus existing task payloads:

- failing task ids
- task timestamps and duration
- domain row counts
- `dq_results`
- merge `reconciliation`
- dbt failure summary when the dbt subprocess exits non-zero

Subprocess failure summaries redact obvious credential-shaped values such as
passwords, API keys, bearer tokens, and `sk-...` keys before the JSON artifact is
written. Treat raw Airflow logs and local ignored environment files as sensitive
regardless.

## Where Artifacts Are Written

Runtime triage artifacts are written to:

`reports/pipeline_triage/nba_analytics_pipeline/<run_id>.json`

That directory is already ignored from git, which makes it appropriate for local
or Airflow runtime artifacts.

Checked-in example outputs live in:

- [docs/examples/pipeline_triage_dq_failure.json](/Users/mikeh/Documents/Coding/NBA_GCP/docs/examples/pipeline_triage_dq_failure.json)
- [docs/examples/pipeline_triage_dq_failure_summary.txt](/Users/mikeh/Documents/Coding/NBA_GCP/docs/examples/pipeline_triage_dq_failure_summary.txt)

## Airflow Integration

The DAG now uses the triage module in two places:

- DAG success callback: writes a final healthy artifact for clean runs
- task failure callback: writes a failed artifact using the current task exception plus all available task/XCom evidence

This keeps the MVP narrow while allowing failed runs to generate artifacts even
when `publish_run_metrics` never executes.

## How To Extend It Later

- Persist the same JSON payload into a BigQuery metadata table if the repo wants searchable run history in `nba_metadata`.
- Add more evidence extractors for dbt node names, failing SQL models, or warehouse freshness metrics.
- Add alert hooks that route only the structured artifact, not raw logs.
- Keep any future LLM summarizer downstream and optional by summarizing the JSON artifact rather than inferring root cause from logs alone.
