"""NBA Analytics Pipeline DAG for self-hosted Airflow.

Target shape:
    extract_incremental -> load_staging -> dq_gate -> merge_raw
        -> dbt_build -> build_analysis_snapshot -> publish_run_metrics
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger("nba_pipeline")
SUPPORTED_SEASON = "2025-26"


def get_config(key: str, default: str | None = None) -> str | None:
    """Read from Airflow Variables first, fall back to env var, then default."""
    try:
        return Variable.get(key)
    except Exception:
        return os.getenv(key, default)


def get_project_id() -> str:
    pid = get_config("BQ_PROJECT", get_config("GCP_PROJECT_ID"))
    if not pid:
        raise ValueError("BQ_PROJECT or GCP_PROJECT_ID must be configured")
    return pid


def get_dataset(dataset_key: str, default_name: str) -> str:
    return get_config(dataset_key, get_config("BQ_DATASET", default_name))


def get_dbt_repo_root() -> Path:
    """Resolve the dbt project root in local Airflow-friendly layouts."""
    dag_file = Path(__file__).resolve()
    candidates = [dag_file.parents[1], dag_file.parent]

    for candidate in candidates:
        if (candidate / "dbt_project.yml").exists() and (
            candidate / "dbt" / "profiles"
        ).exists():
            return candidate

    raise FileNotFoundError(
        "Could not find dbt_project.yml and dbt/profiles alongside the DAG. "
        "Checked: " + ", ".join(str(path) for path in candidates)
    )


default_args = {
    "owner": "nba-analytics",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
}


@dag(
    dag_id="nba_analytics_pipeline",
    description="Incremental NBA 2025-26 player stats pipeline with BigQuery + dbt",
    schedule="0 11 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["nba", "airflow", "bigquery", "dbt", "self-hosted"],
)
def nba_analytics_pipeline():
    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=45),
    )
    def extract_incremental() -> dict:
        """Fetch player game logs, apply replay-window filtering, and land a CSV in GCS."""
        import pandas as pd
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        season = SUPPORTED_SEASON
        replay_days = int(get_config("NBA_REPLAY_DAYS", "3"))
        max_players = int(get_config("NBA_MAX_PLAYERS", "0"))
        project_id = get_project_id()
        bucket_name = get_config("GCS_BUCKET_NAME")
        location = get_config("BQ_LOCATION", "US")
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")
        metadata_dataset = get_dataset("BQ_METADATA_DATASET", "nba_metadata")

        client = bq.Client(project=project_id)
        pipeline.ensure_dataset(client, f"{project_id}.{bronze_dataset}", location)
        pipeline.ensure_dataset(client, f"{project_id}.{metadata_dataset}", location)

        state_table = f"{project_id}.{metadata_dataset}.ingestion_state"
        run_table = f"{project_id}.{metadata_dataset}.pipeline_run_log"
        pipeline.create_metadata_tables(client, state_table, run_table)
        state = pipeline.get_ingestion_state(client, state_table, season=season)

        active = pipeline.get_active_players()
        selected = active if max_players <= 0 else active[:max_players]
        logger.info("Processing %s players for season %s", len(selected), season)

        df = pipeline.get_all_player_game_logs(selected, season=season)
        incremental_df = pipeline.filter_incremental_game_logs(
            df,
            watermark_date=state["watermark_date"],
            replay_days=replay_days,
            season=season,
        )

        if incremental_df.empty:
            logger.info("No rows remain after replay-window filtering")
            return {
                "domain": "game_logs",
                "gcs_uri": "",
                "row_count": 0,
                "season": season,
                "watermark_before": state["watermark_date"].isoformat()
                if state["watermark_date"]
                else None,
                "watermark_after": state["watermark_date"].isoformat()
                if state["watermark_date"]
                else None,
            }

        watermark_after = pipeline.coerce_to_date(incremental_df["GAME_DATE"].max())
        run_stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
        min_date = incremental_df["GAME_DATE"].min().strftime("%Y%m%d")
        max_date = incremental_df["GAME_DATE"].max().strftime("%Y%m%d")
        blob_path = (
            f"nba_data/{season}/landing/{run_stamp}_{min_date}_{max_date}_game_logs.csv"
        )
        gcs_uri = pipeline.upload_df_to_gcs(
            incremental_df, project_id, bucket_name, blob_path
        )

        return {
            "domain": "game_logs",
            "gcs_uri": gcs_uri,
            "row_count": len(incremental_df),
            "season": season,
            "watermark_before": state["watermark_date"].isoformat()
            if state["watermark_date"]
            else None,
            "watermark_after": watermark_after.isoformat() if watermark_after else None,
        }

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_schedule_context() -> dict:
        """Fetch the upcoming schedule window and land a CSV in GCS."""
        import pandas as pd
        import nba_pipeline as pipeline

        season = SUPPORTED_SEASON
        horizon_days = int(get_config("NBA_SCHEDULE_LOOKAHEAD_DAYS", "7"))
        project_id = get_project_id()
        bucket_name = get_config("GCS_BUCKET_NAME")
        schedule_df = pipeline.get_upcoming_schedule(
            season=season,
            horizon_days=horizon_days,
        )
        if schedule_df.empty:
            logger.info(
                "No schedule rows available for the configured lookahead window"
            )
            return {
                "domain": "schedule",
                "gcs_uri": "",
                "row_count": 0,
                "season": season,
            }

        run_stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
        min_date = schedule_df["SCHEDULE_DATE"].min().strftime("%Y%m%d")
        max_date = schedule_df["SCHEDULE_DATE"].max().strftime("%Y%m%d")
        blob_path = (
            f"nba_data/{season}/landing/{run_stamp}_{min_date}_{max_date}_schedule.csv"
        )
        gcs_uri = pipeline.upload_df_to_gcs(
            schedule_df, project_id, bucket_name, blob_path
        )
        return {
            "domain": "schedule",
            "gcs_uri": gcs_uri,
            "row_count": len(schedule_df),
            "season": season,
        }

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def load_game_log_staging(extract_result: dict) -> dict:
        """Load landed game log rows to staging."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        project_id = get_project_id()
        location = get_config("BQ_LOCATION", "US")
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")

        client = bq.Client(project=project_id)
        pipeline.ensure_dataset(client, f"{project_id}.{bronze_dataset}", location)
        staging_table = f"{project_id}.{bronze_dataset}.stg_game_logs"

        if extract_result["row_count"] == 0:
            logger.info(
                "Skipping game log staging load because extract produced no rows"
            )
            return {
                "domain": "game_logs",
                "staging_table": staging_table,
                "row_count": 0,
                "season": extract_result["season"],
                "watermark_before": extract_result["watermark_before"],
                "watermark_after": extract_result["watermark_after"],
                "gcs_uri": extract_result["gcs_uri"],
            }

        pipeline.load_gcs_to_bigquery(
            client,
            extract_result["gcs_uri"],
            staging_table,
            pipeline.get_game_logs_schema(),
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
        )
        return {
            "domain": "game_logs",
            "staging_table": staging_table,
            "row_count": extract_result["row_count"],
            "season": extract_result["season"],
            "watermark_before": extract_result["watermark_before"],
            "watermark_after": extract_result["watermark_after"],
            "gcs_uri": extract_result["gcs_uri"],
        }

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def load_schedule_staging(extract_result: dict) -> dict:
        """Load landed schedule rows to staging."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        project_id = get_project_id()
        location = get_config("BQ_LOCATION", "US")
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")
        client = bq.Client(project=project_id)
        pipeline.ensure_dataset(client, f"{project_id}.{bronze_dataset}", location)
        staging_table = f"{project_id}.{bronze_dataset}.stg_schedule_context"

        if extract_result["row_count"] == 0:
            return {
                "domain": "schedule",
                "staging_table": staging_table,
                "row_count": 0,
                "season": extract_result["season"],
                "gcs_uri": extract_result["gcs_uri"],
            }

        pipeline.load_gcs_to_bigquery(
            client,
            extract_result["gcs_uri"],
            staging_table,
            pipeline.get_schedule_schema(),
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
        )
        return {
            "domain": "schedule",
            "staging_table": staging_table,
            "row_count": extract_result["row_count"],
            "season": extract_result["season"],
            "gcs_uri": extract_result["gcs_uri"],
        }

    @task(retries=0)
    def dq_game_log_staging(load_result: dict) -> dict:
        """Run hard DQ checks for game logs unless the run is a no-op."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        if load_result["row_count"] == 0:
            return load_result

        client = bq.Client(project=get_project_id())
        load_result["dq_results"] = pipeline.run_data_quality_checks(
            client,
            load_result["staging_table"],
            season=SUPPORTED_SEASON,
        )
        return load_result

    @task(retries=0)
    def dq_schedule_staging(load_result: dict) -> dict:
        """Run DQ checks for upcoming schedule rows."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        if load_result["row_count"] == 0:
            load_result["dq_results"] = {}
            return load_result

        client = bq.Client(project=get_project_id())
        load_result["dq_results"] = pipeline.run_schedule_quality_checks(
            client,
            load_result["staging_table"],
            season=SUPPORTED_SEASON,
        )
        return load_result

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def merge_game_logs(load_result: dict) -> dict:
        """Merge staged game log rows into the bronze raw table."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        project_id = get_project_id()
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")
        raw_table = f"{project_id}.{bronze_dataset}.raw_game_logs"

        if load_result["row_count"] == 0:
            return {
                "domain": "game_logs",
                "raw_table": raw_table,
                "rows_loaded": 0,
                "rows_inserted": 0,
                "rows_updated": 0,
                "season": load_result["season"],
                "gcs_uri": load_result["gcs_uri"],
                "watermark_before": load_result["watermark_before"],
                "watermark_after": load_result["watermark_after"],
                "dq_results": load_result.get("dq_results", {}),
            }

        client = bq.Client(project=project_id)
        result = pipeline.create_and_merge_raw_table(
            client, load_result["staging_table"], raw_table
        )
        return {
            "domain": "game_logs",
            "raw_table": raw_table,
            "rows_loaded": load_result["row_count"],
            "rows_inserted": result["inserted"],
            "rows_updated": result["updated"],
            "season": load_result["season"],
            "gcs_uri": load_result["gcs_uri"],
            "watermark_before": load_result["watermark_before"],
            "watermark_after": load_result["watermark_after"],
            "dq_results": load_result.get("dq_results", {}),
        }

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def merge_schedule_context(load_result: dict) -> dict:
        """Merge staged schedule rows into the bronze raw table."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        project_id = get_project_id()
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")
        raw_table = f"{project_id}.{bronze_dataset}.raw_schedule"

        if load_result["row_count"] == 0:
            return {
                "domain": "schedule",
                "raw_table": raw_table,
                "rows_loaded": 0,
                "rows_inserted": 0,
                "rows_updated": 0,
                "season": load_result["season"],
                "gcs_uri": load_result["gcs_uri"],
                "dq_results": load_result.get("dq_results", {}),
            }

        client = bq.Client(project=project_id)
        result = pipeline.create_and_merge_schedule_table(
            client, load_result["staging_table"], raw_table
        )
        return {
            "domain": "schedule",
            "raw_table": raw_table,
            "rows_loaded": load_result["row_count"],
            "rows_inserted": result["inserted"],
            "rows_updated": result["updated"],
            "season": load_result["season"],
            "gcs_uri": load_result["gcs_uri"],
            "dq_results": load_result.get("dq_results", {}),
        }

    @task(retries=0)
    def combine_pipeline_results(game_result: dict, schedule_result: dict) -> dict:
        """Combine per-domain results into a single warehouse build context."""
        all_gcs = [
            value
            for value in [
                game_result.get("gcs_uri", ""),
                schedule_result.get("gcs_uri", ""),
            ]
            if value
        ]
        return {
            "season": game_result["season"],
            "watermark_before": game_result.get("watermark_before"),
            "watermark_after": game_result.get("watermark_after"),
            "gcs_uri": ",".join(all_gcs),
            "rows_loaded": game_result["rows_loaded"],
            "rows_inserted": game_result["rows_inserted"],
            "rows_updated": game_result["rows_updated"],
            "schedule_rows_loaded": schedule_result["rows_loaded"],
            "schedule_rows_inserted": schedule_result["rows_inserted"],
            "schedule_rows_updated": schedule_result["rows_updated"],
            "dq_results": {
                "game_logs": game_result.get("dq_results", {}),
                "schedule": schedule_result.get("dq_results", {}),
            },
            "should_build": any(
                [
                    game_result["rows_loaded"] > 0,
                    schedule_result["rows_loaded"] > 0,
                ]
            ),
        }

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def dbt_build(merge_result: dict) -> dict:
        """Run dbt models and tests after the bronze merges."""
        if not merge_result["should_build"]:
            logger.info("Skipping dbt build because no source domain produced rows")
            merge_result["dbt_status"] = "skipped"
            return merge_result

        repo_root = get_dbt_repo_root()
        profiles_dir = repo_root / "dbt" / "profiles"
        target = get_config("DBT_TARGET", "dev")
        command = [
            "dbt",
            "build",
            "--project-dir",
            str(repo_root),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            target,
            "--exclude",
            "analysis_snapshot_latest",
            "source:gold_runtime.analysis_snapshots",
            "path:dbt/tests/no_duplicate_analysis_snapshots.sql",
        ]

        env = os.environ.copy()
        env.setdefault("BQ_PROJECT", get_project_id())
        env.setdefault(
            "BQ_DATASET_BRONZE", get_dataset("BQ_DATASET_BRONZE", "nba_bronze")
        )
        env.setdefault(
            "BQ_DATASET_SILVER", get_dataset("BQ_DATASET_SILVER", "nba_silver")
        )
        env.setdefault("BQ_DATASET_GOLD", get_dataset("BQ_DATASET_GOLD", "nba_gold"))
        env.setdefault("NBA_SEASON", SUPPORTED_SEASON)

        subprocess.run(command, cwd=repo_root, env=env, check=True)
        merge_result["dbt_status"] = "success"
        return merge_result

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def build_analysis_snapshot(merge_result: dict) -> dict:
        """Create or update the deterministic gold analysis snapshot."""
        import pandas as pd
        from airflow.operators.python import get_current_context
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        merge_result["analysis_snapshot_status"] = "skipped"
        merge_result["analysis_snapshot_id"] = ""

        if not merge_result["should_build"]:
            logger.info("Skipping analysis snapshot because no source domain changed")
            return merge_result

        context = get_current_context()
        project_id = get_project_id()
        gold_dataset = get_dataset("BQ_DATASET_GOLD", "nba_gold")
        location = get_config("BQ_LOCATION", "US")
        client = bq.Client(project=project_id)
        pipeline.ensure_dataset(client, f"{project_id}.{gold_dataset}", location)
        snapshot_table = f"{project_id}.{gold_dataset}.analysis_snapshots"
        pipeline.create_analysis_snapshot_table(client, snapshot_table)

        daily_leaders = client.query(
            f"""
            SELECT *
            FROM `{project_id}.{gold_dataset}.daily_leaderboard`
            WHERE game_date IS NOT NULL
            ORDER BY game_date DESC, pts DESC, pts_leader
            LIMIT 30
            """
        ).to_dataframe()
        trends = client.query(
            f"""
            SELECT *
            FROM `{project_id}.{gold_dataset}.player_trends`
            ORDER BY ABS(delta) DESC, player_name, stat
            LIMIT 30
            """
        ).to_dataframe()
        recommendations = client.query(
            f"""
            SELECT *
            FROM `{project_id}.{gold_dataset}.fantasy_insights`
            ORDER BY as_of_date DESC, priority_score DESC, confidence_score DESC, player_name
            LIMIT 30
            """
        ).to_dataframe()
        rankings = client.query(
            f"""
            SELECT *
            FROM `{project_id}.{gold_dataset}.player_fantasy_rankings`
            ORDER BY fantasy_rank_9cat_proxy ASC, recommendation_score DESC, player_name
            LIMIT 30
            """
        ).to_dataframe()
        freshness_row = client.query(
            f"""
            SELECT MAX(ingested_at_utc) AS freshness_ts
            FROM `{project_id}.{gold_dataset}.fct_player_game_stats`
            WHERE season = @season
            """,
            job_config=bq.QueryJobConfig(
                query_parameters=[
                    bq.ScalarQueryParameter("season", "STRING", merge_result["season"]),
                ]
            ),
        ).to_dataframe()
        freshness_ts = None
        if not freshness_row.empty:
            freshness_ts = freshness_row.iloc[0]["freshness_ts"]

        snapshot = pipeline.build_analysis_snapshot_record(
            season=merge_result["season"],
            daily_leaders=daily_leaders,
            trends=trends,
            recommendations=recommendations,
            rankings=rankings,
            source_run_id=context["run_id"],
            created_at_utc=pd.Timestamp.now(tz="UTC"),
            snapshot_date=context["data_interval_end"],
            freshness_ts=freshness_ts,
        )
        pipeline.upsert_analysis_snapshot(client, snapshot_table, snapshot)
        merge_result["analysis_snapshot_status"] = "success"
        merge_result["analysis_snapshot_id"] = snapshot["snapshot_id"]
        return merge_result

    @task(retries=0)
    def publish_run_metrics(run_result: dict) -> dict:
        """Persist watermark state and run-level metadata."""
        from google.cloud import bigquery as bq
        from airflow.operators.python import get_current_context
        import nba_pipeline as pipeline

        project_id = get_project_id()
        metadata_dataset = get_dataset("BQ_METADATA_DATASET", "nba_metadata")
        location = get_config("BQ_LOCATION", "US")
        client = bq.Client(project=project_id)
        pipeline.ensure_dataset(client, f"{project_id}.{metadata_dataset}", location)

        state_table = f"{project_id}.{metadata_dataset}.ingestion_state"
        run_table = f"{project_id}.{metadata_dataset}.pipeline_run_log"
        pipeline.create_metadata_tables(client, state_table, run_table)

        context = get_current_context()
        if run_result["rows_loaded"] > 0 and run_result["watermark_after"]:
            pipeline.upsert_ingestion_state(
                client,
                state_table,
                season=run_result["season"],
                watermark_date=run_result["watermark_after"],
            )

        record = pipeline.build_run_metadata_record(
            dag_run_id=context["run_id"],
            season=run_result["season"],
            status="success",
            gcs_uri=run_result["gcs_uri"],
            rows_extracted=(
                run_result["rows_loaded"] + run_result.get("schedule_rows_loaded", 0)
            ),
            rows_loaded=run_result["rows_loaded"],
            rows_inserted=run_result["rows_inserted"],
            rows_updated=run_result["rows_updated"],
            watermark_before=run_result["watermark_before"],
            watermark_after=run_result["watermark_after"],
            started_at_utc=context["data_interval_start"],
            finished_at_utc=datetime.now(tz=context["data_interval_start"].tzinfo),
            details=(
                f"dbt_status={run_result.get('dbt_status', 'unknown')};"
                f"analysis_snapshot_status={run_result.get('analysis_snapshot_status', 'unknown')};"
                f"analysis_snapshot_id={run_result.get('analysis_snapshot_id', '')};"
                f"schedule_rows_loaded={run_result.get('schedule_rows_loaded', 0)};"
                f"redshift_status={run_result.get('redshift_status', get_config('ENABLE_REDSHIFT', 'false'))};"
                f"dq={run_result.get('dq_results', {})}"
            ),
        )
        pipeline.record_pipeline_run(client, run_table, record)
        return run_result

    extracted = extract_incremental()
    extracted_schedule = extract_schedule_context()

    staged = load_game_log_staging(extracted)
    staged_schedule = load_schedule_staging(extracted_schedule)

    checked = dq_game_log_staging(staged)
    checked_schedule = dq_schedule_staging(staged_schedule)

    merged = merge_game_logs(checked)
    merged_schedule = merge_schedule_context(checked_schedule)

    @task.branch(retries=0)
    def check_redshift_enabled(combined_result: dict) -> str:
        """Branch: run Redshift sync only when ENABLE_REDSHIFT=true."""
        enabled = get_config("ENABLE_REDSHIFT", "false").lower() == "true"
        if enabled and combined_result["should_build"]:
            return "export_bigquery_bronze"
        return "skip_redshift_sync"

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def export_bigquery_bronze(combined_result: dict) -> dict:
        """Export bronze tables from BigQuery to GCS as Parquet."""
        import nba_redshift_sync as sync

        project_id = get_project_id()
        gcs_bucket = get_config("GCS_BUCKET_NAME")
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")
        import pandas as pd

        run_stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
        gcs_prefix = f"redshift_sync/{run_stamp}"

        for table in ["raw_game_logs", "raw_schedule"]:
            sync.export_bq_to_gcs_parquet(
                project_id, bronze_dataset, table, gcs_bucket, gcs_prefix,
            )

        combined_result["redshift_gcs_prefix"] = gcs_prefix
        return combined_result

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def sync_to_s3(combined_result: dict) -> dict:
        """Copy Parquet files from GCS to S3."""
        import nba_redshift_sync as sync

        gcs_bucket = get_config("GCS_BUCKET_NAME")
        s3_bucket = get_config("AWS_S3_BUCKET_NAME")
        gcs_prefix = combined_result["redshift_gcs_prefix"]

        for table in ["raw_game_logs", "raw_schedule"]:
            sync.copy_gcs_to_s3(
                gcs_bucket, f"{gcs_prefix}/{table}/",
                s3_bucket, f"{gcs_prefix}/{table}",
            )

        combined_result["redshift_s3_prefix"] = gcs_prefix
        return combined_result

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def load_redshift_bronze(combined_result: dict) -> dict:
        """Load S3 Parquet into Redshift and merge."""
        import nba_redshift_sync as sync

        s3_bucket = get_config("AWS_S3_BUCKET_NAME")
        iam_role = get_config("REDSHIFT_IAM_ROLE_ARN")
        schema = get_config("REDSHIFT_SCHEMA_BRONZE", "nba_bronze")
        s3_prefix = combined_result["redshift_s3_prefix"]

        sync.create_redshift_schemas_and_tables()

        tables = [
            {"name": "raw_game_logs", "keys": ["player_id", "game_date", "matchup"]},
            {"name": "raw_schedule", "keys": ["schedule_date", "team_abbr", "opponent_abbr"]},
        ]
        for tbl in tables:
            sync.load_s3_to_redshift(
                s3_bucket, f"{s3_prefix}/{tbl['name']}/",
                schema, tbl["name"], iam_role,
            )
            sync.merge_redshift_staging(schema, tbl["name"], tbl["keys"])
            sync.run_redshift_dq_checks(schema, tbl["name"], tbl["keys"])

        combined_result["redshift_load_status"] = "success"
        return combined_result

    @task(retries=1, retry_delay=timedelta(minutes=5))
    def dbt_build_redshift(combined_result: dict) -> dict:
        """Run dbt build targeting Redshift."""
        repo_root = get_dbt_repo_root()
        profiles_dir = repo_root / "dbt" / "profiles"
        command = [
            "dbt",
            "build",
            "--project-dir",
            str(repo_root),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            "redshift",
            "--exclude",
            "analysis_snapshot_latest",
            "source:gold_runtime.analysis_snapshots",
            "path:dbt/tests/no_duplicate_analysis_snapshots.sql",
        ]

        env = os.environ.copy()
        env.setdefault("BQ_PROJECT", get_project_id())
        env.setdefault("NBA_SEASON", SUPPORTED_SEASON)
        subprocess.run(command, cwd=repo_root, env=env, check=True)
        combined_result["redshift_dbt_status"] = "success"
        return combined_result

    @task(retries=0)
    def skip_redshift_sync(combined_result: dict) -> dict:
        """No-op when Redshift sync is disabled."""
        logger.info("Redshift sync is disabled, skipping")
        combined_result["redshift_status"] = "skipped"
        return combined_result

    combined = combine_pipeline_results(merged, merged_schedule)

    # Redshift branch (optional, non-blocking)
    redshift_check = check_redshift_enabled(combined)
    redshift_exported = export_bigquery_bronze(combined)
    redshift_s3 = sync_to_s3(redshift_exported)
    redshift_loaded = load_redshift_bronze(redshift_s3)
    redshift_modeled = dbt_build_redshift(redshift_loaded)
    redshift_skipped = skip_redshift_sync(combined)

    redshift_check >> [redshift_exported, redshift_skipped]

    # Main pipeline continues regardless of Redshift outcome
    modeled = dbt_build(combined)
    snapshotted = build_analysis_snapshot(modeled)
    publish_run_metrics(snapshotted)


nba_analytics_pipeline()
