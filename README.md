# From NBA Stats to Story: Trending Player Insights with GCP and Claude API

A data pipeline that pulls NBA player game logs, stores them in BigQuery with partitioning and clustering, runs trending-player analytics with window functions, and uses the Claude API to generate written insights. Built as a GCP + AI integration demo.

> **Note:** This project is a practice build for learning GCP data workflows and Claude API integration.  

> It is intended for educational/demo use, not production.

Production-style NBA data pipeline and public serving layer for the `2025-26` season only.

The v1 target architecture is:

`NBA API -> GCS landing -> BigQuery bronze -> dbt silver/gold -> deterministic analysis snapshots -> Cloud Run site/API`

Core decisions for this version:

- BigQuery remains the warehouse system of record.
- dbt remains the bronze/silver/gold transformation layer.
- Cloud Composer is the production pipeline path.
- Cloud Run is the public read-only website/API target.
- The operational scope is fixed to season `2025-26`.
- Claude/Anthropic is not part of the v1 runtime path.
- Analysis output is deterministic and template-based, not LLM-generated.

## Production Flow

The Airflow DAG in `dags/nba_analytics_dag.py` runs this path:

1. Extract active-player game logs from `nba_api` for season `2025-26`.
2. Apply incremental filtering using a persisted watermark plus replay buffer.
3. Land the incremental extract in GCS.
4. Load the landed file into `bronze.stg_game_logs`.
5. Run DQ checks for zero rows, null business keys, duplicate business keys, invalid `WL`, invalid season, and out-of-window dates.
6. Merge into `bronze.raw_game_logs` using `(player_id, game_date, matchup)` as the business key.
7. Run dbt bronze/silver/gold models and tests for the warehouse serving layer.
8. Build a deterministic `gold.analysis_snapshots` record from gold tables.
9. Publish watermark and run metadata to `nba_metadata`.

## Warehouse Layout

- `bronze.raw_game_logs`: replay-safe raw source table
- `silver.stg_game_logs_clean`: season-scoped cleaned source model
- `silver.int_player_game_enriched`: matchup and team enrichment
- `gold.fct_player_game_stats`: fact table for player game stats
- `gold.dim_player`: player dimension
- `gold.dim_team`: team dimension
- `gold.player_trends`: recent-vs-prior player trend model
- `gold.daily_leaderboard`: daily leaderboard output
- `gold.analysis_snapshots`: deterministic narrative snapshot output written by the DAG

dbt is intentionally centered on `2025-26` only. The silver layer filters to that season and the accepted in-season date window.

## Public Service

The FastAPI service is intended for Cloud Run and serves both HTML and JSON from the same process.

Public HTML routes:

- `/`
- `/analysis`

Public JSON routes:

- `/api/leaderboard`
- `/api/trends`
- `/api/analysis/latest`
- `/api/health`

The service reads only from gold tables and metadata tables. It is public read-only for v1 and does not include auth.

Freshness is reported from the latest successful pipeline run in `nba_metadata.pipeline_run_log`, evaluated against a daily freshness threshold.

## Local Setup

1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables in `.env`:
   ```env
   ANTHROPIC_API_KEY=your_key
   GCP_PROJECT_ID=your_gcp_project
   GCS_BUCKET_NAME=your_gcs_bucket
   BQ_DATASET=nba_data
   BQ_LOCATION=US
   NBA_SEASON=2024-25
   NBA_RUN_FULL_EXTRACT=false
   NBA_MAX_PLAYERS=25
   TREND_WINDOW_DAYS=7
   TREND_MIN_GAMES=2
   ANTHROPIC_MODEL=claude-sonnet-4-5-20250929
   ```

## Notebook Flow
1. Pull active players + game logs from `nba_api`.
2. Build leaderboards and season aggregates in pandas.
3. Upload raw game logs CSV to GCS.
4. BigQuery load pattern:
   - Load incoming file into staging table: `stg_game_logs`.
   - Run data quality checks (row count, null business keys, duplicate business keys).
   - `MERGE` staging into partitioned/clustered raw table: `raw_game_logs`.
5. Run BigQuery analytics (window functions, seasonal averages, trends, reusable view).
6. Generate AI analysis to write a simple article from query outputs

## Data Quality Gates
The notebook stops execution if staging data has:
- zero rows
- null business keys (`player_id`, `game_date`, `matchup`)
- duplicate business keys (`player_id`, `game_date`, `matchup`)

## BigQuery Design
- Dataset: `${GCP_PROJECT_ID}.${BQ_DATASET}`
- Staging table: `stg_game_logs` (reloaded each run)
- Raw table: `raw_game_logs` (append via `MERGE`)
- Partitioning: `game_date`
- Clustering: `player_id`, `player_name`

```bash
pip install -r requirements.txt
```

3. Configure environment variables. Minimum useful local values:

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
DBT_TARGET=dev
API_FRESHNESS_THRESHOLD_HOURS=36
PORT=8080
```

Notes:

- `gcloud` authentication is assumed during rollout or local warehouse validation.
- The production season is fixed to `2025-26`; do not treat the notebook as the production contract.
- Airflow itself is for local validation only here; Cloud Composer is the deployment target.
- Keep local secrets in ignored env files only; do not commit `.env*` files or ad hoc variants such as `.env `.
- The notebook may remain useful for exploration, but Anthropic/Claude is not part of the production contract.

## Running the App Locally

Run the FastAPI service with:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

Then open:

- `http://localhost:8080/`
- `http://localhost:8080/analysis`
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

- DAG import/parse in a local Airflow environment

Areas covered by tests should include:

- watermark + replay logic
- season-only filtering for `2025-26`
- run metadata record generation
- deterministic analysis snapshot generation
- DAG import
- API smoke coverage for all public routes

## Deployment Notes

- Cloud Composer runs the Airflow DAG and needs access to GCS, BigQuery, and the dbt profile configuration.
- Cloud Run serves the FastAPI app and needs read access to `nba_gold` and `nba_metadata`.
- Terraform is intentionally not required for this v1 implementation pass.

## Security Hygiene

- Runtime services read from GCP auth and environment configuration; credentials must not be committed.
- The public service is read-only and only queries curated gold and metadata tables.
- Any previously used Anthropic credentials should be treated as local-only and rotated if they were ever stored outside ignored files.
