"""NBA Analytics Pipeline DAG for Google Cloud Composer.

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
    return get_config("BQ_PROJECT", get_config("GCP_PROJECT_ID"))


def get_dataset(dataset_key: str, default_name: str) -> str:
    return get_config(dataset_key, get_config("BQ_DATASET", default_name))


def get_dbt_repo_root() -> Path:
    """Resolve the dbt project root in both local and Composer layouts."""
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
    tags=["nba", "airflow", "bigquery", "dbt", "cloud-composer"],
)
def nba_analytics_pipeline():
    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=45),
    )
    def extract_incremental() -> dict:
        """Fetch game logs, apply replay-window filtering, and land a CSV in GCS."""
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
                "gcs_uri": "",
                "row_count": 0,
                "player_count": 0,
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
            "gcs_uri": gcs_uri,
            "row_count": len(incremental_df),
            "player_count": int(incremental_df["PLAYER_ID"].nunique()),
            "season": season,
            "watermark_before": state["watermark_date"].isoformat()
            if state["watermark_date"]
            else None,
            "watermark_after": watermark_after.isoformat() if watermark_after else None,
        }

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def load_staging(extract_result: dict) -> dict:
        """Load landed incremental rows to staging."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        project_id = get_project_id()
        location = get_config("BQ_LOCATION", "US")
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")

        client = bq.Client(project=project_id)
        pipeline.ensure_dataset(client, f"{project_id}.{bronze_dataset}", location)
        staging_table = f"{project_id}.{bronze_dataset}.stg_game_logs"

        if extract_result["row_count"] == 0:
            logger.info("Skipping staging load because extract produced no rows")
            return {
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
            "staging_table": staging_table,
            "row_count": extract_result["row_count"],
            "season": extract_result["season"],
            "watermark_before": extract_result["watermark_before"],
            "watermark_after": extract_result["watermark_after"],
            "gcs_uri": extract_result["gcs_uri"],
        }

    @task(retries=0)
    def dq_gate(load_result: dict) -> dict:
        """Run hard DQ checks unless the run is a no-op."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        if load_result["row_count"] == 0:
            logger.info("Skipping DQ because no rows were staged")
            return load_result

        client = bq.Client(project=get_project_id())
        dq_results = pipeline.run_data_quality_checks(
            client,
            load_result["staging_table"],
            season=SUPPORTED_SEASON,
        )
        logger.info("DQ passed: %s", dq_results)
        load_result["dq_results"] = dq_results
        return load_result

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def merge_raw(load_result: dict) -> dict:
        """Merge staging rows into the bronze raw table."""
        from google.cloud import bigquery as bq
        import nba_pipeline as pipeline

        project_id = get_project_id()
        bronze_dataset = get_dataset("BQ_DATASET_BRONZE", "nba_bronze")
        raw_table = f"{project_id}.{bronze_dataset}.raw_game_logs"

        if load_result["row_count"] == 0:
            logger.info("Skipping raw merge because no rows were staged")
            return {
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
    def dbt_build(merge_result: dict) -> dict:
        """Run dbt models and tests after the raw merge."""
        if merge_result["rows_loaded"] == 0:
            logger.info("Skipping dbt build because the run had no staged rows")
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

        if merge_result["rows_loaded"] == 0:
            logger.info("Skipping analysis snapshot because the run had no staged rows")
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
            rows_extracted=run_result["rows_loaded"],
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
                f"dq={run_result.get('dq_results', {})}"
            ),
        )
        pipeline.record_pipeline_run(client, run_table, record)
        return run_result

    extracted = extract_incremental()
    staged = load_staging(extracted)
    checked = dq_gate(staged)
    merged = merge_raw(checked)
    modeled = dbt_build(merged)
    snapshotted = build_analysis_snapshot(modeled)
    publish_run_metrics(snapshotted)


nba_analytics_pipeline()
