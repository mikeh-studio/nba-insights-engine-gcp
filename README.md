# NBA Data Platform

Production-style NBA data pipeline and public fantasy-facing service for the `2025-26` season only. Built on **GCP** (BigQuery, GCS, Cloud Run) with an optional **AWS** secondary warehouse (Redshift Serverless, S3).

The v1 target architecture is:

`NBA API -> GCS landing -> BigQuery bronze -> dbt silver/gold -> fantasy rankings + insights -> deterministic analysis snapshots -> Cloud Run site/API`

With optional Redshift sync:

`BigQuery bronze -> GCS Parquet -> S3 -> Redshift COPY -> dbt Redshift models`

Core decisions for this version:

- BigQuery (GCP) is the warehouse system of record.
- Redshift Serverless (AWS) is an optional secondary warehouse for cross-cloud portfolio/learning.
- dbt remains the bronze/silver/gold transformation layer, with cross-database macros for BigQuery/Redshift compatibility.
- Self-hosted Airflow is the supported orchestration path.
- Cloud Run is the public read-only website/API target.
- Infrastructure is managed by Terraform — GCP core infra and AWS Redshift infra (`infra/terraform-aws/`).
- The operational scope is fixed to season `2025-26`.
- Claude/Anthropic is not part of the v1 runtime path.
- Analysis output is deterministic and template-based, not LLM-generated.
- Supporting context is schedule-based only; injury-report ingestion is not part of the current runtime path.

## Pipeline Flow

The Airflow DAG in `dags/nba_analytics_dag.py` runs this path:

1. Extract active-player game logs from `nba_api` for season `2025-26`.
2. Apply incremental filtering using a persisted watermark plus replay buffer.
3. Fetch the upcoming schedule window from `nba_api`.
4. Land both domains in GCS and load them into bronze staging tables.
5. Run DQ checks for game logs and schedule context.
6. Merge into `bronze.raw_game_logs` and `bronze.raw_schedule`.
7. Run dbt bronze/silver/gold models and tests for the fantasy serving layer.
8. Build a deterministic `gold.analysis_snapshots` record from leaderboard, trend, ranking, and recommendation outputs.
9. Publish watermark and run metadata to `nba_metadata`.

## Optional Redshift Secondary Warehouse

The pipeline optionally syncs bronze tables to an AWS Redshift Serverless cluster as a secondary warehouse. This is a cross-cloud learning/portfolio feature and is not required for the primary BigQuery pipeline.

**How to enable:** Set `ENABLE_REDSHIFT=true` in your environment. When enabled, the Airflow DAG appends a Redshift sync task after the BigQuery bronze merge.

**Data flow:** BigQuery bronze tables are exported as Parquet to GCS, copied to S3, and loaded into Redshift via `COPY`. dbt models then run against the Redshift target using cross-database compatibility macros.

**Infrastructure:** AWS resources (Redshift Serverless, S3 bucket, IAM roles, networking) are managed by Terraform in `infra/terraform-aws/`.

**Configuration:** See `.env.example` for the full set of Redshift-related environment variables (`REDSHIFT_HOST`, `REDSHIFT_DB`, `AWS_S3_BUCKET_NAME`, etc.).

## Warehouse Layout

- `bronze.raw_game_logs`: replay-safe raw source table
- `bronze.raw_schedule`: upcoming schedule context by team
- `silver.stg_game_logs_clean`: season-scoped cleaned source model
- `silver.int_player_game_enriched`: matchup and team enrichment
- `silver.stg_schedule_clean`: cleaned schedule context
- `gold.fct_player_game_stats`: fact table for player game stats
- `gold.dim_player`: player dimension
- `gold.dim_team`: team dimension
- `gold.player_trends`: recent-vs-prior player trend model
- `gold.player_recent_form`: rolling recent form and fantasy proxy output
- `gold.player_category_profile`: category-score profile for fantasy ranking
- `gold.player_opportunity_outlook`: schedule-only opportunity context
- `gold.player_fantasy_rankings`: deterministic fantasy ranking surface
- `gold.fantasy_insights`: structured recommendation cards
- `gold.fantasy_recommendation_backtest`: forward outcome evaluation for recommendation rows
- `gold.daily_leaderboard`: daily leaderboard output
- `gold.analysis_snapshots`: deterministic narrative snapshot output written by the DAG

dbt is intentionally centered on `2025-26` only. The silver layer filters to that season and the accepted in-season date window.

When Redshift sync is enabled, bronze tables are also available in Redshift under the configured schema (default `nba_bronze`).

## Public Service

The FastAPI service is intended for Cloud Run and serves both HTML and JSON from the same process.

Public HTML routes:

- `/`
- `/analysis`
- `/recommendations`
- `/players/{player_id}`

Public JSON routes:

- `/api/leaderboard`
- `/api/trends`
- `/api/analysis/latest`
- `/api/recommendations`
- `/api/rankings`
- `/api/players/search`
- `/api/players/{player_id}`
- `/api/health`

The service reads only from gold tables and metadata tables. It is public read-only for v1 and does not include auth.

Freshness is reported from the latest successful pipeline run in `nba_metadata.pipeline_run_log`, evaluated against a daily freshness threshold.

## Local Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy the local env template and fill in your project-specific values:

```bash
cp .env.example .env
```

4. Configure environment variables. Minimum useful local values:

```env
GCP_PROJECT_ID=your_gcp_project
BQ_PROJECT=your_gcp_project
GCS_BUCKET_NAME=your_gcs_bucket
BQ_DATASET_BRONZE=nba_bronze
BQ_DATASET_SILVER=nba_silver
BQ_DATASET_GOLD=nba_gold
BQ_METADATA_DATASET=nba_metadata
BQ_LOCATION=US
NBA_MAX_PLAYERS=0
NBA_REPLAY_DAYS=3
NBA_SCHEDULE_LOOKAHEAD_DAYS=7
DBT_TARGET=dev
API_FRESHNESS_THRESHOLD_HOURS=36
API_MAX_SEARCH_RESULTS=12
PORT=8080
AIRFLOW_HOME=./airflow_home
ENABLE_REDSHIFT=false
```

Notes:

- `gcloud` authentication is assumed during rollout or local warehouse validation.
- The production season is fixed to `2025-26`; do not treat the notebook as the production contract.
- Airflow is intended to run locally or on a self-hosted machine for orchestration.
- The DAG reads Airflow Variables first and falls back to environment variables for local runs.
- Keep local secrets in ignored env files only; do not commit `.env*` files or ad hoc variants such as `.env `.
- The notebook may remain useful for exploration, but Anthropic/Claude is not part of the production contract.

## Running Airflow Locally

This repo supports a host-based Airflow workflow without Docker. The included `Makefile`
standardizes the repo-local `AIRFLOW_HOME` path and points Airflow at `dags/`.
If a `.env` file exists in the repo root, `make` exports those variables into the Airflow
commands automatically.

Initialize the local Airflow metadata database:

```bash
make airflow-init
```

Create an admin user for the local web UI:

```bash
make airflow-create-user
```

Start the scheduler and webserver in separate terminals:

```bash
make airflow-scheduler
make airflow-webserver
```

Trigger the pipeline manually:

```bash
make airflow-trigger
```

Useful URLs and commands:

- Airflow UI: `http://localhost:8080`
- List DAGs: `make airflow-list`
- Run a DAG parse check: `make airflow-parse`

## Running the App Locally

Run the FastAPI service with:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

Then open:

- `http://localhost:8080/`
- `http://localhost:8080/analysis`
- `http://localhost:8080/recommendations`
- `http://localhost:8080/api/health`

## QA Expectations

The repo now expects QA coverage across data logic, orchestration, and the public service.

Primary validation commands:

```bash
python -m compileall dags app tests
pytest
dbt parse --project-dir . --profiles-dir dbt/profiles
dbt test --project-dir . --profiles-dir dbt/profiles --target dev \
  --exclude analysis_snapshot_latest source:gold_runtime.analysis_snapshots path:dbt/tests/no_duplicate_analysis_snapshots.sql
```

Additional checks when validating Airflow changes:

- DAG import/parse in the local Airflow environment

When validating Redshift cross-db compatibility:

```bash
dbt parse --project-dir . --profiles-dir dbt/profiles --target redshift
```

Current local validation caveats:

- `dbt parse` runs locally without warehouse access.
- `dbt test` requires a real BigQuery-enabled project and valid GCP auth; it will fail against placeholder projects such as `local-project`.
- Airflow parse checks require the Airflow CLI/module to be installed in the active environment.

Areas covered by tests should include:

- watermark + replay logic
- schedule-context normalization and DQ
- season-only filtering for `2025-26`
- run metadata record generation
- deterministic analysis snapshot generation with fantasy recommendations
- DAG import
- API smoke coverage for all public routes

## Runtime Notes

- Local or self-hosted Airflow runs the DAG and needs access to GCS, BigQuery, and the dbt profile configuration.
- Cloud Run serves the FastAPI app and needs read access to `nba_gold` and `nba_metadata`.
- Terraform is intentionally not required for this v1 implementation pass.

## Security Hygiene

- Runtime services read from GCP auth and environment configuration; credentials must not be committed.
- The public service is read-only and only queries curated gold and metadata tables.
- Any previously used Anthropic credentials should be treated as local-only and rotated if they were ever stored outside ignored files.
