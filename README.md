# NBA Data Platform

Production-style NBA data pipeline and public NBA stats workbench for the `2025-26` season only. Built on **GCP** (BigQuery, GCS, Cloud Run) with an optional **AWS** secondary warehouse (Redshift Serverless, S3).

The v1 target architecture is:

`NBA API -> GCS landing -> BigQuery bronze -> dbt silver/gold -> player similarity + archetypes -> deterministic analysis snapshots -> Cloud Run site/API`

With optional Redshift sync:

`BigQuery bronze -> GCS Parquet -> S3 -> Redshift COPY -> dbt Redshift models`

Core decisions for this version:

- BigQuery (GCP) is the warehouse system of record.
- Redshift Serverless (AWS) is an optional secondary warehouse for cross-cloud portfolio/learning.
- dbt remains the bronze/silver/gold transformation layer, with cross-database macros for BigQuery/Redshift compatibility.
- Self-hosted Airflow is the supported orchestration path.
- Cloud Run is the public read-only website/API target.
- Infrastructure is managed by Terraform — GCP core infra and AWS Redshift infra (`infra/terraform-aws/`).
- GitHub Actions CI validates `pytest`, `dbt parse`, and Terraform on pull requests and pushes to `main`.
- The operational scope is fixed to season `2025-26`.
- Claude/Anthropic is not part of the v1 runtime path.
- Analysis output is deterministic and template-based, not LLM-generated.
- Supporting context now includes upcoming schedule, team line scores, and player reference attributes; injury-report ingestion is still not part of the current runtime path.

## Pipeline Flow

The Airflow DAG in `dags/nba_analytics_dag.py` runs this path:

1. Extract active-player game logs from `nba_api` for season `2025-26`.
2. Apply incremental filtering using a persisted watermark plus replay buffer.
3. Derive the replay-window `game_id` set and fetch team line scores for those games.
4. Fetch active-player reference attributes and roster context.
5. Fetch the upcoming schedule window from `nba_api`.
6. Land all four domains in GCS and load them into bronze staging tables.
7. Run DQ checks for game logs, line scores, player reference, and schedule context.
8. Merge into `bronze.raw_game_logs`, `bronze.raw_game_line_scores`, `bronze.raw_player_reference`, and `bronze.raw_schedule`, then validate merge reconciliation against loaded and inserted/updated counts.
9. Run dbt bronze/silver/gold models and tests for the public stats-serving layer.
10. Build player similarity vectors and archetype clusters from `gold.player_similarity_feature_input`, then publish `gold.player_similarity_features` and `gold.player_archetypes`.
11. Build a deterministic `analysis_snapshots` record from leaderboard, trend, ranking, recommendation, scoring-contribution, and player-context outputs.
12. Publish watermark and run metadata to `nba_metadata`.

## Optional Redshift Secondary Warehouse

The pipeline optionally syncs bronze tables to AWS Redshift Serverless as a secondary warehouse (cross-cloud learning/portfolio feature). Set `ENABLE_REDSHIFT=true` to enable — the DAG appends a Redshift sync task after the BigQuery bronze merge.

Data flows from BigQuery bronze as Parquet through GCS to S3, then loads into Redshift via `COPY` with automatic schema alignment. dbt models run against Redshift using cross-database compatibility macros. AWS infrastructure is managed by Terraform in `infra/terraform-aws/`. See `.env.example` for Redshift-related variables. The `dbt-redshift` adapter is required for local Redshift validation.

## Warehouse Layout

- `bronze.raw_game_logs`: replay-safe raw source table
- `bronze.raw_game_line_scores`: team final score and quarter/OT line score by game
- `bronze.raw_player_reference`: stable player profile and roster attributes
- `bronze.raw_schedule`: upcoming schedule context by team
- `silver.stg_game_logs_clean`: season-scoped cleaned source model
- `silver.stg_game_line_scores_clean`: cleaned team line scores
- `silver.stg_player_reference_clean`: cleaned player profile and roster context
- `silver.int_player_game_enriched`: matchup and team enrichment
- `silver.stg_schedule_clean`: cleaned schedule context
- `gold.fct_player_game_stats`: fact table for player game stats
- `gold.fct_team_game_scores`: team score, quarter totals, margin, and opponent context
- `gold.fct_player_scoring_contribution`: player points as a share of team and game scoring
- `gold.dim_player`: player dimension
- `gold.player_trends`: recent-vs-prior player trend model
- `gold.player_recent_form`: rolling recent form and box-score-derived proxy output
- `gold.player_category_profile`: category-score profile for the ranking surface
- `gold.player_opportunity_outlook`: schedule-only opportunity context
- `gold.player_fantasy_rankings`: deterministic ranking surface
- `gold.player_similarity_feature_input`: clustering feature table built from season-to-date and recent-form stat shape
- `gold.player_similarity_features`: normalized similarity vectors plus per-player summary traits
- `gold.player_archetypes`: batch-assigned archetype labels and confidence by player
- `gold.workbench_compare`: fixed-window compare input model for bounded compare windows
- `gold.workbench_dashboard`: dashboard-oriented player read model with bounded reason fields
- `gold.workbench_home_dashboard`: seven-day dashboard snapshot model keyed by `as_of_date`
- `gold.workbench_player_detail`: player-detail read model built from dashboard + compare windows
- `gold.fantasy_insights`: structured recommendation cards
- `gold.daily_leaderboard`: daily leaderboard output
- `gold.analysis_snapshots`: deterministic narrative snapshot output written by the DAG

dbt is intentionally centered on `2025-26` only. The silver layer filters to that season and the accepted in-season date window.

The FastAPI service now reads from the similarity outputs in addition to the existing gold and metadata tables. Player detail uses `gold.player_similarity_features` and `gold.player_archetypes`, and compare adds a stat-profile similarity summary when both players have stable feature vectors.

When Redshift sync is enabled, bronze tables are also available in Redshift under the configured schema (default `nba_bronze`).

## Public Service

The FastAPI service is intended for Cloud Run and serves both HTML and JSON from the same process.

Public HTML routes:

- `/`
- `/players/{player_id}`
- `/compare`
- `/visualize`

Public JSON routes:

- `/api/leaderboard`
- `/api/trends`
- `/api/analysis/latest`
- `/api/recommendations`
- `/api/rankings`
- `/api/players/search`
- `/api/players/{player_id}`
- `/api/compare`
- `/api/health`

The service reads only from gold tables and metadata tables. It is public read-only for v1 and does not include auth.

Freshness is reported from the latest successful pipeline run in `nba_metadata.pipeline_run_log`, evaluated against a daily freshness threshold.

UI freshness states now render as relative time labels (for example, "2 days ago") with the exact ISO timestamp preserved in the hover title.

The compare page supports two entry modes: direct `player_a_id` deep links and first-player search when no initial player is provided.

`/api/analysis/latest` now returns the existing narrative fields plus nested `score_contribution` and `player_context` sections sourced from the expanded snapshot record.

`/api/players/{player_id}` now includes `archetype`, `similar_players`, and `similarity_reason` fields, and the player page renders an archetype card plus a similar-player panel when the sample is stable.

`/api/compare` now includes a `similarity` block with pair score, shared traits, and contrasting traits, and the compare page renders that stat-profile summary inline.

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
NBA_ARCHETYPE_CLUSTERS=6
NBA_SCHEDULE_LOOKAHEAD_DAYS=7
DBT_TARGET=dev
API_FRESHNESS_THRESHOLD_HOURS=36
API_MAX_SEARCH_RESULTS=12
PORT=8080
AIRFLOW_HOME=./airflow_home
ENABLE_REDSHIFT=false
```

## Running Airflow Locally

This repo supports a host-based Airflow workflow without Docker. The included `Makefile`
standardizes the repo-local `AIRFLOW_HOME` path and points Airflow at `dags/`.
If a `.env` file exists in the repo root, `make` exports those variables into the Airflow
commands automatically. If `airflow` is not on your global `PATH`, the `Makefile` falls back
to the repo-local `.venv-airflow` Python and runs `python -m airflow`.

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

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

The service is available at `http://localhost:8080`. See **Public Service** above for available routes.

## QA Expectations

The repo now expects QA coverage across data logic, orchestration, and the public service.

Primary validation commands:

```bash
python -m compileall dags app tests
PYTHONPATH=. pytest
dbt parse --project-dir . --profiles-dir dbt/profiles
dbt test --project-dir . --profiles-dir dbt/profiles --target dev \
  --exclude source:gold_runtime.analysis_snapshots path:dbt/tests/no_duplicate_analysis_snapshots.sql
dbt build --project-dir . --profiles-dir dbt/profiles --target dev \
  --select player_similarity_feature_input
dbt test --project-dir . --profiles-dir dbt/profiles --target dev \
  --select workbench_compare workbench_dashboard workbench_home_dashboard workbench_player_detail
```

Additional checks when validating Airflow changes:

- DAG import/parse in the local Airflow environment

When validating Redshift cross-db compatibility:

```bash
dbt parse --project-dir . --profiles-dir dbt/profiles --target redshift
dbt build --project-dir . --profiles-dir dbt/profiles --target redshift --select path:dbt/models/silver
```

Current local validation caveats:

- `dbt parse` runs locally without warehouse access.
- `dbt build --target redshift --select path:dbt/models/silver` is the recommended compatibility check for the Redshift secondary warehouse and requires working Redshift credentials plus the `dbt-redshift` adapter.
- `dbt test` requires a real BigQuery-enabled project and valid GCP auth; it will fail against placeholder projects such as `local-project`.
- The targeted workbench-model `dbt test --select ...` command has the same BigQuery auth requirement.
- In the latest local validation run for this branch, `python -m compileall dags app tests`, `PYTHONPATH=. pytest`, `dbt parse`, and `make airflow-parse` all pass.
- In the latest live validation run for this branch, the configured BigQuery project `nba-data-485505` is reachable and the minimum core chain builds successfully:
  `dim_player dim_team dim_game fct_player_game_stats fct_team_game_scores fct_player_scoring_contribution player_recent_form player_similarity_feature_input`.
- Live Airflow orchestration still needs follow-up hardening: the local `make airflow-trigger` path did not find the DAG in `DagModel`, and direct DAG testing hit NBA API timeouts. The warehouse was repaired directly in BigQuery for this validation pass.

## Security Hygiene

- Runtime services read from GCP auth and environment configuration; credentials must not be committed.
- The public service is read-only and only queries curated gold and metadata tables.
- Pipeline triage artifacts are operational reports. They redact obvious credential-looking tokens before writing subprocess failure summaries, but runtime logs and ignored local `.env` files should still be treated as sensitive.
- Any previously used Anthropic credentials should be treated as local-only and rotated if they were ever stored outside ignored files.
