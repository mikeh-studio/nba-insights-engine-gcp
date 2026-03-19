# AGENTS.md

This file provides repository guidance for coding agents working in this project.

## Repository Purpose

The repo is an NBA data pipeline evolving into a production-style data engineering project on GCP.

The intended end state is:

- Airflow-orchestrated ingestion and scheduling
- incremental, idempotent loads from `nba_api`
- BigQuery as the warehouse system of record
- dbt-managed bronze/silver/gold transformations
- testing, observability, CI/CD, and Terraform-backed core infrastructure
- optional downstream AI/reporting artifacts after curated warehouse outputs exist

## Current Implementation Snapshot

- Notebook workflow exists in `nba_api.ipynb`.
- Airflow DAG logic lives under `dags/`.
- Shared pipeline functions live in `dags/nba_pipeline.py`.
- Optional Redshift sync logic lives in `dags/nba_redshift_sync.py`.
- The current pipeline works, but it is still Python-task heavy and not yet fully aligned to the target DE architecture.

When making changes, improve the repo toward the target state rather than reinforcing notebook-style patterns.

## Preferred Change Order

If multiple improvements are possible, prioritize them in this order:

1. Incremental + idempotent ingestion
2. Curated warehouse modeling with dbt
3. Data quality, tests, and observability
4. CI/CD and Terraform-managed infra
5. Optional reporting, documents, spreadsheets, or AI summaries

## Architecture Direction

Target flow:

```text
NBA API
  -> incremental extract using watermark + replay buffer
  -> GCS landing
  -> BigQuery bronze staging
  -> idempotent raw merge
  -> dbt silver/gold models
  -> curated facts, dimensions, and analytics outputs
  -> optional downstream reporting
  -> optional Redshift secondary warehouse sync (BQ -> GCS Parquet -> S3 -> Redshift COPY)
```

Preferred Airflow graph:

```text
extract_incremental
  >> load_bronze_staging
  >> dq_gate
  >> merge_bronze_raw
  >> dbt_silver_gold
  >> publish_run_metrics
  >> optional_reporting
```

## Modeling Rules

- Keep source ingestion and orchestration in Airflow.
- Keep warehouse transformations in BigQuery/dbt, not in large pandas tasks.
- Treat bronze as landed/raw source data.
- Use silver for cleaned and standardized models.
- Use gold for curated facts, dimensions, and analytics outputs.
- Preserve the current business key `(player_id, game_date, matchup)` unless the source data model changes.

Planned curated models:

- `gold.fct_player_game_stats`
- `gold.dim_player`
- `gold.dim_team`
- `gold.player_trends`
- `gold.daily_leaderboard`

## Ingestion Rules

- Favor incremental loads over full refreshes.
- Persist watermarks in warehouse metadata tables rather than hidden local state.
- Use a replay buffer so reruns can safely capture late-arriving corrections.
- Make reruns idempotent through deterministic merge keys and replay-safe load behavior.
- Preserve NBA API retry and rate-limit protections when touching extraction logic.

## Data Quality and Observability

Minimum quality bar:

- zero-row detection
- null business key detection
- duplicate business key detection

Desired operational visibility:

- run metadata table per DAG run
- row-count and inserted/updated metrics
- freshness checks
- warehouse model tests
- alerting hooks for failures or stale data

Agents should prefer adding measurable operational signals over adding cosmetic reporting features.

## Preferred Tools

Primary tools:

- Airflow
- BigQuery
- GCS
- dbt
- `pytest`
- Terraform (GCP core infra; AWS Redshift infra in `infra/terraform-aws/`)

Optional supporting tools:

- Google Workspace CLI for Docs/Sheets-based handoffs, report exports, analyst collaboration artifacts, or operational runbooks
- Claude/Anthropic tooling for optional narrative outputs after warehouse results exist

Google Workspace CLI is optional. Do not make core pipeline execution depend on it.

## Validation Expectations

Before handing back changes, validate the parts you touched with the strongest available checks. Prefer:

- DAG import/parse validation for Airflow changes
- `pytest` for Python logic
- dbt parse/test for warehouse changes
- SQL or warehouse-level checks for modeled tables
- Terraform validation or planning for infra changes

If a full validation step cannot run, state exactly what was not verified.

## Working Style

- Bias toward production data engineering patterns over exploratory notebook habits.
- Separate current-state documentation from target-state intent when editing docs.
- Keep optional AI/report generation downstream and non-blocking.
- Favor explicit configuration, reproducibility, and operational clarity.
- Do not introduce tools or workflows that complicate the core pipeline without a concrete DE benefit.
