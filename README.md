# From NBA Stats to Story: Trending Player Insights with GCP and Claude API

A data pipeline that pulls NBA player game logs, stores them in BigQuery with partitioning and clustering, runs trending-player analytics with window functions, and uses the Claude API to generate written insights. Built as a GCP + AI integration demo.

> **Note:** This project is a practice build for learning GCP data workflows and Claude API integration.  

> It is intended for educational/demo use, not production.


## Setup
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


