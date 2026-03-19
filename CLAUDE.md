# CLAUDE.md

This file provides guidance to Claude Code (`claude.ai/code`) when working in this repository.

## Project Overview

This repository is moving from a notebook-derived NBA analytics project into an Airflow-orchestrated data engineering pipeline on GCP.

The target direction is:

- incremental, idempotent ingestion from `nba_api`
- warehouse modeling in BigQuery with curated dbt layers
- production-oriented testing, observability, CI/CD, and Terraform-managed core infrastructure
- optional AI/reporting outputs downstream of the warehouse, not in the core data-platform SLA path

## Current State vs Target State

**Current state**

- The repository contains a notebook entrypoint in `nba_api.ipynb`.
- Airflow orchestration lives in `dags/nba_analytics_dag.py`.
- Shared business logic lives in `dags/nba_pipeline.py`.
- The current flow is mostly Python-task driven and still reflects its notebook origin.

**Target state**

- Airflow is the primary orchestration layer.
- BigQuery is the warehouse system of record.
- dbt owns bronze/silver/gold warehouse transformations.
- Ingestion is incremental and replay-safe.
- Tests, run metadata, alerts, CI validation, and Terraform-backed infra are first-class parts of the project.

## Target Architecture

```text
NBA API
  -> incremental extract with watermark + replay buffer
  -> GCS landing
  -> BigQuery bronze staging
  -> idempotent MERGE into bronze raw
  -> dbt silver/gold models in BigQuery
  -> curated analytics tables and views
  -> optional AI/reporting outputs
  -> optional Redshift secondary warehouse sync (BQ -> GCS Parquet -> S3 -> Redshift COPY)
```

Preferred orchestration shape:

```text
extract_incremental
  >> load_bronze_staging
  >> dq_gate
  >> merge_bronze_raw
  >> dbt_silver_gold
  >> publish_run_metrics
  >> optional_reporting
```

## Engineering Priorities

When modifying this repo, prefer work that strengthens the DE story in this order:

1. Incremental and idempotent ingestion
2. Warehouse modeling with curated tables
3. Data quality, tests, and observability
4. CI/CD and Terraform-managed infra
5. Optional downstream reporting or AI generation

Avoid moving warehouse logic back into pandas-heavy Airflow tasks if the same behavior belongs in BigQuery or dbt.

## Warehouse Design Direction

Target model layers:

- **Bronze**: landed source data and raw merged tables
- **Silver**: cleaned, standardized, deduplicated models
- **Gold**: curated fact and dimension tables for analytics consumption

Planned curated tables include:

- `bronze.raw_game_logs`
- `silver.stg_game_logs_clean`
- `silver.int_player_game_enriched`
- `gold.fct_player_game_stats`
- `gold.dim_player`
- `gold.dim_team`
- `gold.player_trends`
- `gold.daily_leaderboard`

Keep the bronze/raw merge keyed on `(player_id, game_date, matchup)` unless there is a source-level reason to redefine the business key.

## Ingestion Direction

The intended ingestion pattern is:

- track a persisted watermark in BigQuery metadata tables
- extract only new or recently replayed game dates
- allow a small replay buffer for late corrections
- write landed files to GCS
- load to staging and MERGE idempotently into bronze/raw tables
- record row counts, watermark movement, and run status for each pipeline run

If you change ingestion code, preserve NBA API backoff and rate-limiting behavior.

## Data Quality and Observability

Current hard gates already include:

- zero-row protection
- null business key checks
- duplicate business key checks

The target observability layer should add:

- pipeline run metadata tables
- row-count and inserted/updated metrics
- freshness tracking
- alert hooks for failures or stale data
- warehouse-level tests for keys, relationships, and accepted values

## Tooling

Primary tools for this repository:

- Airflow for orchestration
- BigQuery for warehouse storage and SQL processing
- GCS for landing and intermediate files
- dbt for warehouse modeling and tests
- `pytest` for Python validation
- Terraform for core GCP infrastructure
- Terraform for optional AWS Redshift infrastructure (`infra/terraform-aws/`)

Optional supporting tools:

- Google Workspace CLI for creating or updating Docs/Sheets-based handoff artifacts, reports, or collaboration outputs
- Anthropic/Claude tooling for optional downstream article generation

Google Workspace CLI is auxiliary only. It should not become a required runtime dependency for the core pipeline.

## Configuration Direction

The repo currently uses environment variables and Airflow Variables. As the project evolves, favor explicit config for:

- `GCP_PROJECT_ID`
- `GCS_BUCKET_NAME`
- `BQ_PROJECT`
- `BQ_DATASET_BRONZE`
- `BQ_DATASET_SILVER`
- `BQ_DATASET_GOLD`
- `BQ_METADATA_DATASET`
- `DBT_TARGET`
- `NBA_SEASON`
- replay buffer and watermark settings
- alerting or notification configuration
- `ENABLE_REDSHIFT` and Redshift connection settings (see `.env.example`)

Prefer config that works both locally and in Composer/Airflow without hidden defaults.

## Commands

Current local commands:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

jupyter notebook nba_api.ipynb

source .venv-airflow/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow standalone
```

Target-state validation commands to prefer once the corresponding pieces exist:

```bash
pytest
dbt parse
dbt test
terraform plan
```

## Working Guidance

- Treat Airflow as the orchestrator, not the transformation engine.
- Prefer BigQuery SQL and dbt for warehouse logic over pandas when the work belongs in the warehouse.
- Keep optional reporting and AI generation downstream of curated models.
- Make changes in a way that improves idempotency, testability, and operational visibility.
- When documenting the system, distinguish clearly between current implementation and target architecture.
