# AGENTS.md

Repository guidance for coding agents.

## Repository Purpose

NBA data pipeline evolving into a production-style data engineering project on GCP (BigQuery, GCS, Airflow, dbt) with optional AWS Redshift secondary warehouse.

## Current Implementation Snapshot

- Exploratory notebook workflow is archived under `notebooks/nba_api.ipynb`.
- Airflow DAG logic lives under `dags/`.
- Shared pipeline functions live in `dags/nba_pipeline.py`.
- Optional Redshift sync logic lives in `dags/nba_redshift_sync.py`.

When making changes, improve the repo toward the target state rather than reinforcing notebook-style patterns.

## Preferred Change Order

1. Incremental + idempotent ingestion
2. Curated warehouse modeling with dbt
3. Data quality, tests, and observability
4. CI/CD and Terraform-managed infra
5. Optional reporting, documents, spreadsheets, or AI summaries

## Modeling Rules

- Keep source ingestion and orchestration in Airflow.
- Keep warehouse transformations in BigQuery/dbt, not in large pandas tasks.
- Bronze = landed/raw source data. Silver = cleaned and standardized. Gold = curated facts, dimensions, analytics.
- Preserve the business key `(player_id, game_date, matchup)` unless the source data model changes.

## Data Quality Minimum Bar

- Zero-row detection
- Null business key detection
- Duplicate business key detection

Prefer adding measurable operational signals over cosmetic reporting features.

## Validation Expectations

Before handing back changes, validate what you touched:

- DAG import/parse for Airflow changes
- `pytest` for Python logic
- `dbt parse` / `dbt test` for warehouse changes
- Terraform plan for infra changes

If a full validation step cannot run, state exactly what was not verified.

## Working Style

- Bias toward production data engineering patterns over exploratory notebook habits.
- Separate current-state documentation from target-state intent when editing docs.
- Keep optional AI/report generation downstream and non-blocking.
- Favor explicit configuration, reproducibility, and operational clarity.
- Do not introduce tools or workflows that complicate the core pipeline without a concrete DE benefit.
