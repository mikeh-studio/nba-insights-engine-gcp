# TODOS

## Review

### Player Similarity DS Layer Follow-Up

The player similarity and archetype branch is code-complete locally, but live BigQuery is not healthy enough to call the feature warehouse-green yet.

Tomorrow's agent should fix the live warehouse in this order:

1. Verify Airflow Variables and runtime env match the intended BigQuery project and datasets.
   Confirm `BQ_PROJECT`, `GCP_PROJECT_ID`, `BQ_DATASET_BRONZE`, `BQ_DATASET_SILVER`, `BQ_DATASET_GOLD`, and `BQ_METADATA_DATASET` all point at the same warehouse.
2. Inspect `nba_metadata.pipeline_run_log` and `nba_metadata.ingestion_state` for season `2025-26`.
   We need to know whether the DAG has been writing into the wrong project, or whether the live project is simply only half-bootstrapped.
3. Rebuild bronze if the live project is incomplete.
   `dbt test` showed missing `nba_bronze.raw_schedule`, `nba_bronze.raw_game_line_scores`, and `nba_bronze.raw_player_reference` in `nba-data-485505`.
4. Repair dirty `raw_game_logs` rows.
   The live warehouse still has null `game_id` values in `raw_game_logs`, and source tests reported 16,043 failing rows.
5. Rerun the full DAG or a full `dbt build`, not just the similarity model.
   `player_similarity_feature_input` depends on upstream gold tables including `fct_player_scoring_contribution`.
6. Add a code guardrail after the warehouse is fixed.
   The game-log staging DQ currently enforces `player_id`, `game_date`, and `matchup`, but not `game_id`. That should be tightened so bronze does not admit rows that later fail dbt source tests.

## Completed

### Add Custom Compare Controls

Implemented bounded compare presets beyond the base flow, adding `last_3` and `last_7` windows plus a stat-focus control for `balanced`, `scoring`, `playmaking`, and `defense`.

### Instrument Degraded Panel States

Implemented app-level structured telemetry for degraded freshness, player-detail panel states, dashboard opportunity degradation, and compare-side limitations.
