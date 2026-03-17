"""NBA pipeline business logic.

This module stays free of Airflow imports so its helpers remain unit-testable.
It supports incremental extraction, metadata persistence, deterministic
analysis snapshot generation, and idempotent warehouse loads.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery, storage
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.endpoints import scheduleleaguev2
from nba_api.stats.static import players

logger = logging.getLogger("nba_pipeline")

SOURCE_SYSTEM = "nba_api"
SUPPORTED_SEASON = "2025-26"
SUPPORTED_SEASON_START = date(2025, 7, 1)
SUPPORTED_SEASON_END = date(2026, 6, 30)


def get_season_date_bounds(season: str = SUPPORTED_SEASON) -> Tuple[date, date]:
    """Return the inclusive date bounds for the supported production season."""
    if season != SUPPORTED_SEASON:
        raise ValueError(f"Unsupported production season: {season}")
    return SUPPORTED_SEASON_START, SUPPORTED_SEASON_END


def coerce_to_date(value: Any) -> Optional[date]:
    """Convert supported date-like values to a date."""
    if value in (None, "", pd.NaT):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def compute_replay_start(watermark_date: Any, replay_days: int = 3) -> Optional[date]:
    """Return the inclusive replay start date for an existing watermark."""
    watermark = coerce_to_date(watermark_date)
    if watermark is None:
        return None
    replay_window = max(replay_days - 1, 0)
    return watermark - timedelta(days=replay_window)


def filter_incremental_game_logs(
    df: pd.DataFrame,
    watermark_date: Any = None,
    replay_days: int = 3,
    season: str = SUPPORTED_SEASON,
) -> pd.DataFrame:
    """Keep rows inside the replay window and normalize key fields."""
    if df.empty:
        return df.copy()

    working = df.copy()
    working["GAME_DATE"] = pd.to_datetime(
        working["GAME_DATE"], errors="coerce"
    ).dt.normalize()
    working = working.dropna(subset=["GAME_DATE"]).copy()
    if "SEASON" not in working.columns:
        working["SEASON"] = season
    if "WL" in working.columns:
        working["WL"] = working["WL"].astype("string").str.upper()
    if "SEASON" in working.columns:
        working["SEASON"] = working["SEASON"].astype("string")
        working = working[working["SEASON"] == season].copy()

    season_start, season_end = get_season_date_bounds(season)
    working = working[
        (working["GAME_DATE"].dt.date >= season_start)
        & (working["GAME_DATE"].dt.date <= season_end)
    ].copy()

    replay_start = compute_replay_start(watermark_date, replay_days=replay_days)
    if replay_start is not None:
        working = working[working["GAME_DATE"].dt.date >= replay_start].copy()

    working = working.drop_duplicates(
        subset=["PLAYER_ID", "GAME_DATE", "MATCHUP"]
    ).copy()
    working = working.sort_values(["GAME_DATE", "PLAYER_ID"], ascending=[False, True])
    return working.reset_index(drop=True)


def build_run_metadata_record(
    *,
    dag_run_id: str,
    season: str,
    status: str,
    source_system: str = SOURCE_SYSTEM,
    gcs_uri: str = "",
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    watermark_before: Any = None,
    watermark_after: Any = None,
    started_at_utc: Any = None,
    finished_at_utc: Any = None,
    details: str = "",
) -> Dict[str, Any]:
    """Build a JSON-serializable metadata record for a pipeline run."""
    started = pd.to_datetime(started_at_utc or pd.Timestamp.now(tz="UTC"), utc=True)
    finished = pd.to_datetime(finished_at_utc or pd.Timestamp.now(tz="UTC"), utc=True)
    return {
        "dag_run_id": dag_run_id,
        "source_system": source_system,
        "season": season,
        "status": status,
        "gcs_uri": gcs_uri,
        "rows_extracted": int(rows_extracted),
        "rows_loaded": int(rows_loaded),
        "rows_inserted": int(rows_inserted),
        "rows_updated": int(rows_updated),
        "watermark_before": coerce_to_date(watermark_before).isoformat()
        if coerce_to_date(watermark_before)
        else None,
        "watermark_after": coerce_to_date(watermark_after).isoformat()
        if coerce_to_date(watermark_after)
        else None,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": finished.isoformat(),
        "details": details,
    }


def ensure_dataset(bq_client: bigquery.Client, dataset_id: str, location: str) -> None:
    """Create the dataset if it does not exist."""
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    bq_client.create_dataset(dataset, exists_ok=True)


def create_metadata_tables(
    bq_client: bigquery.Client,
    state_table: str,
    run_metadata_table: str,
) -> None:
    """Create metadata tables used for watermarking and run tracking."""
    state_sql = f"""
    CREATE TABLE IF NOT EXISTS `{state_table}` (
      source_system STRING,
      season STRING,
      watermark_date DATE,
      updated_at_utc TIMESTAMP
    )
    """
    run_sql = f"""
    CREATE TABLE IF NOT EXISTS `{run_metadata_table}` (
      dag_run_id STRING,
      source_system STRING,
      season STRING,
      status STRING,
      gcs_uri STRING,
      rows_extracted INT64,
      rows_loaded INT64,
      rows_inserted INT64,
      rows_updated INT64,
      watermark_before DATE,
      watermark_after DATE,
      started_at_utc TIMESTAMP,
      finished_at_utc TIMESTAMP,
      details STRING
    )
    PARTITION BY DATE(started_at_utc)
    """
    bq_client.query(state_sql).result()
    bq_client.query(run_sql).result()


def get_ingestion_state(
    bq_client: bigquery.Client,
    state_table: str,
    *,
    source_system: str = SOURCE_SYSTEM,
    season: str,
) -> Dict[str, Optional[date]]:
    """Fetch the current watermark for a source/season pair."""
    query = f"""
    SELECT watermark_date, updated_at_utc
    FROM `{state_table}`
    WHERE source_system = @source_system
      AND season = @season
    ORDER BY updated_at_utc DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
            bigquery.ScalarQueryParameter("season", "STRING", season),
        ]
    )
    rows = list(bq_client.query(query, job_config=job_config).result())
    if not rows:
        return {"watermark_date": None, "updated_at_utc": None}
    row = rows[0]
    return {
        "watermark_date": coerce_to_date(row["watermark_date"]),
        "updated_at_utc": row["updated_at_utc"],
    }


def upsert_ingestion_state(
    bq_client: bigquery.Client,
    state_table: str,
    *,
    season: str,
    watermark_date: Any,
    source_system: str = SOURCE_SYSTEM,
) -> None:
    """Persist the latest successful watermark."""
    watermark = coerce_to_date(watermark_date)
    if watermark is None:
        return

    merge_sql = f"""
    MERGE `{state_table}` T
    USING (
      SELECT
        @source_system AS source_system,
        @season AS season,
        @watermark_date AS watermark_date,
        CURRENT_TIMESTAMP() AS updated_at_utc
    ) S
    ON T.source_system = S.source_system
    AND T.season = S.season
    WHEN MATCHED THEN
      UPDATE SET watermark_date = S.watermark_date, updated_at_utc = S.updated_at_utc
    WHEN NOT MATCHED THEN
      INSERT (source_system, season, watermark_date, updated_at_utc)
      VALUES (S.source_system, S.season, S.watermark_date, S.updated_at_utc)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
            bigquery.ScalarQueryParameter("season", "STRING", season),
            bigquery.ScalarQueryParameter(
                "watermark_date", "DATE", watermark.isoformat()
            ),
        ]
    )
    bq_client.query(merge_sql, job_config=job_config).result()


def record_pipeline_run(
    bq_client: bigquery.Client,
    run_metadata_table: str,
    record: Dict[str, Any],
) -> None:
    """Insert a single metadata record into BigQuery."""
    errors = bq_client.insert_rows_json(run_metadata_table, [record])
    if errors:
        raise RuntimeError(f"Failed to record pipeline run: {errors}")


def get_active_players() -> list:
    """Return all active NBA players from the NBA API."""
    active = players.get_active_players()
    logger.info("Found %s active players", len(active))
    return active


def get_player_game_log(
    player_id: int, season: str = SUPPORTED_SEASON, retries: int = 3, delay: float = 0.8
) -> pd.DataFrame:
    """Get normalized game logs for a single player with retry logic."""
    cols = [
        "GAME_DATE",
        "MATCHUP",
        "WL",
        "MIN",
        "FGM",
        "FGA",
        "FG_PCT",
        "FG3M",
        "FG3A",
        "FG3_PCT",
        "FTM",
        "FTA",
        "FT_PCT",
        "OREB",
        "DREB",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TOV",
        "PF",
        "PLUS_MINUS",
    ]

    for attempt in range(1, retries + 1):
        try:
            gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=season)
            df = gamelog.get_data_frames()[0]

            missing_cols = [c for c in cols if c not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing expected columns: {missing_cols}")

            out = df[cols].copy()
            out["GAME_DATE"] = pd.to_datetime(out["GAME_DATE"], errors="coerce")
            out = out.dropna(subset=["GAME_DATE"])

            numeric_cols = [
                "MIN",
                "FGM",
                "FGA",
                "FG_PCT",
                "FG3M",
                "FG3A",
                "FG3_PCT",
                "FTM",
                "FTA",
                "FT_PCT",
                "OREB",
                "DREB",
                "PTS",
                "REB",
                "AST",
                "STL",
                "BLK",
                "TOV",
                "PF",
                "PLUS_MINUS",
            ]
            for col in numeric_cols:
                out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

            out["SEASON"] = season
            out["INGESTED_AT_UTC"] = pd.Timestamp.now(tz="UTC")
            return out
        except Exception:
            if attempt == retries:
                logger.exception(
                    "Failed player_id=%s after %s attempts", player_id, retries
                )
                return pd.DataFrame()
            sleep_seconds = delay * attempt
            logger.warning(
                "Retrying player_id=%s attempt=%s/%s in %.1fs",
                player_id,
                attempt,
                retries,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    return pd.DataFrame()


def get_all_player_game_logs(
    player_list: Iterable[dict], season: str = SUPPORTED_SEASON, delay: float = 0.6
) -> pd.DataFrame:
    """Fetch game logs for multiple players with rate limiting."""
    all_logs = []
    player_list = list(player_list)

    for i, player in enumerate(player_list, start=1):
        player_id = player["id"]
        player_name = player["full_name"]
        logger.info("Fetching %s/%s: %s", i, len(player_list), player_name)

        games = get_player_game_log(player_id, season=season)
        if not games.empty:
            games["PLAYER_ID"] = player_id
            games["PLAYER_NAME"] = player_name
            all_logs.append(games)

        time.sleep(delay)

    if not all_logs:
        raise RuntimeError(
            "No game logs were fetched. Check API availability and season value."
        )

    all_game_logs = pd.concat(all_logs, ignore_index=True)
    all_game_logs = all_game_logs.drop_duplicates(
        subset=["PLAYER_ID", "GAME_DATE", "MATCHUP"]
    ).copy()
    all_game_logs = all_game_logs.sort_values(
        ["GAME_DATE", "PLAYER_ID"], ascending=[False, True]
    )

    required = {
        "GAME_DATE",
        "PLAYER_ID",
        "PLAYER_NAME",
        "FGM",
        "FGA",
        "FG_PCT",
        "FG3M",
        "FG3A",
        "FG3_PCT",
        "FTM",
        "FTA",
        "FT_PCT",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
    }
    missing = required - set(all_game_logs.columns)
    if missing:
        raise ValueError(f"Missing required fields in merged logs: {sorted(missing)}")

    logger.info(
        "Fetched %s rows across %s players",
        len(all_game_logs),
        all_game_logs["PLAYER_ID"].nunique(),
    )
    return all_game_logs.reset_index(drop=True)


def upload_df_to_gcs(
    df: pd.DataFrame, project_id: str, bucket_name: str, destination_blob_name: str
) -> str:
    """Upload a DataFrame as CSV to Google Cloud Storage and return gs:// URI."""
    if df.empty:
        raise ValueError(
            f"Refusing to upload empty DataFrame to {destination_blob_name}"
        )

    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    csv_data = df.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type="text/csv")
    uri = f"gs://{bucket_name}/{destination_blob_name}"
    logger.info("Uploaded %s rows to %s", len(df), uri)
    return uri


def get_game_logs_schema() -> List[bigquery.SchemaField]:
    """Return the BigQuery schema for game logs."""
    return [
        bigquery.SchemaField("GAME_DATE", "DATE"),
        bigquery.SchemaField("MATCHUP", "STRING"),
        bigquery.SchemaField("WL", "STRING"),
        bigquery.SchemaField("MIN", "FLOAT"),
        bigquery.SchemaField("FGM", "FLOAT"),
        bigquery.SchemaField("FGA", "FLOAT"),
        bigquery.SchemaField("FG_PCT", "FLOAT"),
        bigquery.SchemaField("FG3M", "FLOAT"),
        bigquery.SchemaField("FG3A", "FLOAT"),
        bigquery.SchemaField("FG3_PCT", "FLOAT"),
        bigquery.SchemaField("FTM", "FLOAT"),
        bigquery.SchemaField("FTA", "FLOAT"),
        bigquery.SchemaField("FT_PCT", "FLOAT"),
        bigquery.SchemaField("OREB", "FLOAT"),
        bigquery.SchemaField("DREB", "FLOAT"),
        bigquery.SchemaField("PTS", "INTEGER"),
        bigquery.SchemaField("REB", "INTEGER"),
        bigquery.SchemaField("AST", "INTEGER"),
        bigquery.SchemaField("STL", "INTEGER"),
        bigquery.SchemaField("BLK", "INTEGER"),
        bigquery.SchemaField("TOV", "INTEGER"),
        bigquery.SchemaField("PF", "INTEGER"),
        bigquery.SchemaField("PLUS_MINUS", "FLOAT"),
        bigquery.SchemaField("SEASON", "STRING"),
        bigquery.SchemaField("INGESTED_AT_UTC", "TIMESTAMP"),
        bigquery.SchemaField("PLAYER_ID", "INTEGER"),
        bigquery.SchemaField("PLAYER_NAME", "STRING"),
    ]


def load_gcs_to_bigquery(
    bq_client: bigquery.Client,
    gcs_uri: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    partition_field: Optional[str] = None,
    clustering_fields: Optional[List[str]] = None,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    """Load a CSV from GCS into BigQuery with optional partitioning and clustering."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=write_disposition,
    )

    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
    if clustering_fields:
        job_config.clustering_fields = clustering_fields

    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()

    table = bq_client.get_table(table_id)
    logger.info(
        "Loaded %s rows to %s (write=%s, partitioned=%s, clustered=%s)",
        table.num_rows,
        table_id,
        write_disposition,
        partition_field or "none",
        clustering_fields or "none",
    )


def run_data_quality_checks(
    bq_client: bigquery.Client,
    staging_table: str,
    *,
    season: str = SUPPORTED_SEASON,
) -> dict:
    """Run data quality checks on staging table. Raises ValueError on failure."""
    season_start, season_end = get_season_date_bounds(season)
    dq_query = f"""
    WITH base AS (
      SELECT *
      FROM `{staging_table}`
    ),
    dups AS (
      SELECT COUNT(*) AS duplicate_keys
      FROM (
        SELECT player_id, game_date, matchup, COUNT(*) AS cnt
        FROM base
        GROUP BY player_id, game_date, matchup
        HAVING COUNT(*) > 1
      )
    )
    SELECT
      (SELECT COUNT(*) FROM base) AS total_rows,
      (SELECT COUNT(*) FROM base WHERE player_id IS NULL OR game_date IS NULL OR matchup IS NULL) AS null_key_rows,
      (SELECT duplicate_keys FROM dups) AS duplicate_key_rows,
      (SELECT COUNT(*) FROM base WHERE season != @season OR season IS NULL) AS invalid_season_rows,
      (SELECT COUNT(*) FROM base WHERE game_date < @season_start OR game_date > @season_end) AS out_of_window_rows,
      (SELECT COUNT(*) FROM base WHERE wl IS NOT NULL AND upper(wl) NOT IN ('W', 'L')) AS invalid_wl_rows,
      (
        SELECT COUNT(*)
        FROM base
        WHERE (fg_pct IS NOT NULL AND (fg_pct < 0 OR fg_pct > 1))
           OR (ft_pct IS NOT NULL AND (ft_pct < 0 OR ft_pct > 1))
           OR (fg3_pct IS NOT NULL AND (fg3_pct < 0 OR fg3_pct > 1))
      ) AS invalid_pct_rows
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("season", "STRING", season),
            bigquery.ScalarQueryParameter(
                "season_start", "DATE", season_start.isoformat()
            ),
            bigquery.ScalarQueryParameter("season_end", "DATE", season_end.isoformat()),
        ]
    )
    dq = (
        bq_client.query(dq_query, job_config=job_config)
        .to_dataframe()
        .iloc[0]
        .to_dict()
    )
    logger.info("DQ results: %s", dq)

    if dq["total_rows"] == 0:
        raise ValueError("DQ failed: staging table has zero rows")
    if dq["null_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['null_key_rows']} rows with null business keys"
        )
    if dq["duplicate_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['duplicate_key_rows']} duplicate business keys"
        )
    if dq["invalid_season_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['invalid_season_rows']} rows outside season {season}"
        )
    if dq["out_of_window_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['out_of_window_rows']} rows outside date window "
            f"{season_start.isoformat()} to {season_end.isoformat()}"
        )
    if dq["invalid_wl_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['invalid_wl_rows']} rows with invalid WL values"
        )
    if dq["invalid_pct_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['invalid_pct_rows']} rows with invalid shooting percentages"
        )

    return dq


def create_and_merge_raw_table(
    bq_client: bigquery.Client, staging_table: str, raw_table: str
) -> Dict[str, int]:
    """Create raw table if needed and MERGE staging data into it."""
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{raw_table}` (
      game_date DATE,
      matchup STRING,
      wl STRING,
      min FLOAT64,
      fgm FLOAT64,
      fga FLOAT64,
      fg_pct FLOAT64,
      fg3m FLOAT64,
      fg3a FLOAT64,
      fg3_pct FLOAT64,
      ftm FLOAT64,
      fta FLOAT64,
      ft_pct FLOAT64,
      oreb FLOAT64,
      dreb FLOAT64,
      pts INT64,
      reb INT64,
      ast INT64,
      stl INT64,
      blk INT64,
      tov INT64,
      pf INT64,
      plus_minus FLOAT64,
      season STRING,
      ingested_at_utc TIMESTAMP,
      player_id INT64,
      player_name STRING
    )
    PARTITION BY game_date
    CLUSTER BY player_id, player_name
    """

    stats_sql = f"""
    SELECT
      COUNTIF(t.player_id IS NULL) AS inserted,
      COUNTIF(
        t.player_id IS NOT NULL
        AND (
          COALESCE(t.wl, '') != COALESCE(s.wl, '')
          OR COALESCE(t.min, 0) != COALESCE(s.min, 0)
          OR COALESCE(t.fgm, 0) != COALESCE(s.fgm, 0)
          OR COALESCE(t.fga, 0) != COALESCE(s.fga, 0)
          OR COALESCE(t.fg_pct, 0) != COALESCE(s.fg_pct, 0)
          OR COALESCE(t.fg3m, 0) != COALESCE(s.fg3m, 0)
          OR COALESCE(t.fg3a, 0) != COALESCE(s.fg3a, 0)
          OR COALESCE(t.fg3_pct, 0) != COALESCE(s.fg3_pct, 0)
          OR COALESCE(t.ftm, 0) != COALESCE(s.ftm, 0)
          OR COALESCE(t.fta, 0) != COALESCE(s.fta, 0)
          OR COALESCE(t.ft_pct, 0) != COALESCE(s.ft_pct, 0)
          OR COALESCE(t.oreb, 0) != COALESCE(s.oreb, 0)
          OR COALESCE(t.dreb, 0) != COALESCE(s.dreb, 0)
          OR COALESCE(t.pts, 0) != COALESCE(s.pts, 0)
          OR COALESCE(t.reb, 0) != COALESCE(s.reb, 0)
          OR COALESCE(t.ast, 0) != COALESCE(s.ast, 0)
          OR COALESCE(t.stl, 0) != COALESCE(s.stl, 0)
          OR COALESCE(t.blk, 0) != COALESCE(s.blk, 0)
          OR COALESCE(t.tov, 0) != COALESCE(s.tov, 0)
          OR COALESCE(t.pf, 0) != COALESCE(s.pf, 0)
          OR COALESCE(t.plus_minus, 0) != COALESCE(s.plus_minus, 0)
          OR COALESCE(t.season, '') != COALESCE(s.season, '')
          OR COALESCE(t.player_name, '') != COALESCE(s.player_name, '')
        )
      ) AS updated
    FROM `{staging_table}` s
    LEFT JOIN `{raw_table}` t
      ON t.player_id = s.player_id
     AND t.game_date = s.game_date
     AND t.matchup = s.matchup
    """

    merge_sql = f"""
    MERGE `{raw_table}` T
    USING `{staging_table}` S
    ON T.player_id = S.player_id
    AND T.game_date = S.game_date
    AND T.matchup = S.matchup
    WHEN MATCHED AND (
      COALESCE(T.wl, '') != COALESCE(S.wl, '')
      OR COALESCE(T.min, 0) != COALESCE(S.min, 0)
      OR COALESCE(T.fgm, 0) != COALESCE(S.fgm, 0)
      OR COALESCE(T.fga, 0) != COALESCE(S.fga, 0)
      OR COALESCE(T.fg_pct, 0) != COALESCE(S.fg_pct, 0)
      OR COALESCE(T.fg3m, 0) != COALESCE(S.fg3m, 0)
      OR COALESCE(T.fg3a, 0) != COALESCE(S.fg3a, 0)
      OR COALESCE(T.fg3_pct, 0) != COALESCE(S.fg3_pct, 0)
      OR COALESCE(T.ftm, 0) != COALESCE(S.ftm, 0)
      OR COALESCE(T.fta, 0) != COALESCE(S.fta, 0)
      OR COALESCE(T.ft_pct, 0) != COALESCE(S.ft_pct, 0)
      OR COALESCE(T.oreb, 0) != COALESCE(S.oreb, 0)
      OR COALESCE(T.dreb, 0) != COALESCE(S.dreb, 0)
      OR COALESCE(T.pts, 0) != COALESCE(S.pts, 0)
      OR COALESCE(T.reb, 0) != COALESCE(S.reb, 0)
      OR COALESCE(T.ast, 0) != COALESCE(S.ast, 0)
      OR COALESCE(T.stl, 0) != COALESCE(S.stl, 0)
      OR COALESCE(T.blk, 0) != COALESCE(S.blk, 0)
      OR COALESCE(T.tov, 0) != COALESCE(S.tov, 0)
      OR COALESCE(T.pf, 0) != COALESCE(S.pf, 0)
      OR COALESCE(T.plus_minus, 0) != COALESCE(S.plus_minus, 0)
      OR COALESCE(T.season, '') != COALESCE(S.season, '')
      OR COALESCE(T.player_name, '') != COALESCE(S.player_name, '')
    ) THEN
      UPDATE SET
        wl = S.wl,
        min = S.min,
        fgm = S.fgm,
        fga = S.fga,
        fg_pct = S.fg_pct,
        fg3m = S.fg3m,
        fg3a = S.fg3a,
        fg3_pct = S.fg3_pct,
        ftm = S.ftm,
        fta = S.fta,
        ft_pct = S.ft_pct,
        oreb = S.oreb,
        dreb = S.dreb,
        pts = S.pts,
        reb = S.reb,
        ast = S.ast,
        stl = S.stl,
        blk = S.blk,
        tov = S.tov,
        pf = S.pf,
        plus_minus = S.plus_minus,
        season = S.season,
        ingested_at_utc = S.ingested_at_utc,
        player_name = S.player_name
    WHEN NOT MATCHED THEN
      INSERT (game_date, matchup, wl, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
              ftm, fta, ft_pct, oreb, dreb, pts, reb, ast, stl, blk, tov, pf, plus_minus,
              season, ingested_at_utc, player_id, player_name)
      VALUES (S.game_date, S.matchup, S.wl, S.min, S.fgm, S.fga, S.fg_pct, S.fg3m, S.fg3a,
              S.fg3_pct, S.ftm, S.fta, S.ft_pct, S.oreb, S.dreb, S.pts, S.reb, S.ast, S.stl,
              S.blk, S.tov, S.pf, S.plus_minus, S.season, S.ingested_at_utc, S.player_id,
              S.player_name)
    """

    bq_client.query(create_ddl).result()
    pre_count = (
        bq_client.query(f"SELECT COUNT(*) AS c FROM `{raw_table}`")
        .to_dataframe()
        .iloc[0]["c"]
    )
    stats = bq_client.query(stats_sql).to_dataframe().iloc[0].to_dict()
    bq_client.query(merge_sql).result()
    post_count = (
        bq_client.query(f"SELECT COUNT(*) AS c FROM `{raw_table}`")
        .to_dataframe()
        .iloc[0]["c"]
    )

    result = {
        "pre_count": int(pre_count),
        "post_count": int(post_count),
        "inserted": int(stats["inserted"]),
        "updated": int(stats["updated"]),
    }
    logger.info("MERGE completed: %s", result)
    return result


def get_schedule_schema() -> List[bigquery.SchemaField]:
    """Return the BigQuery schema for upcoming team schedule rows."""
    return [
        bigquery.SchemaField("SCHEDULE_DATE", "DATE"),
        bigquery.SchemaField("GAME_ID", "STRING"),
        bigquery.SchemaField("SEASON", "STRING"),
        bigquery.SchemaField("TEAM_ABBR", "STRING"),
        bigquery.SchemaField("OPPONENT_ABBR", "STRING"),
        bigquery.SchemaField("HOME_AWAY", "STRING"),
        bigquery.SchemaField("IS_BACK_TO_BACK", "BOOLEAN"),
        bigquery.SchemaField("GAME_STATUS", "STRING"),
        bigquery.SchemaField("SOURCE_UPDATED_AT_UTC", "TIMESTAMP"),
        bigquery.SchemaField("INGESTED_AT_UTC", "TIMESTAMP"),
    ]


def get_upcoming_schedule(
    *,
    season: str = SUPPORTED_SEASON,
    horizon_days: int = 7,
    today: Any = None,
) -> pd.DataFrame:
    """Fetch upcoming schedule rows from nba_api scheduleleaguev2."""
    if season != SUPPORTED_SEASON:
        raise ValueError(f"Unsupported production season: {season}")

    base_day = coerce_to_date(today) or pd.Timestamp.now(tz="UTC").date()
    end_day = base_day + timedelta(days=max(horizon_days, 1) - 1)
    schedule = scheduleleaguev2.ScheduleLeagueV2(season=season)
    frames = schedule.get_data_frames()
    if not frames:
        return pd.DataFrame(columns=[field.name for field in get_schedule_schema()])

    raw = frames[0].copy()
    if raw.empty:
        return pd.DataFrame(columns=[field.name for field in get_schedule_schema()])

    raw["gameDate"] = pd.to_datetime(raw["gameDate"], errors="coerce").dt.date
    raw["gameDateTimeUTC"] = pd.to_datetime(
        raw["gameDateTimeUTC"], errors="coerce", utc=True
    )
    raw = raw[(raw["gameDate"] >= base_day) & (raw["gameDate"] <= end_day)].copy()

    if raw.empty:
        return pd.DataFrame(columns=[field.name for field in get_schedule_schema()])

    ingested_at = pd.Timestamp.now(tz="UTC")
    team_rows: list[dict[str, Any]] = []
    for row in raw.to_dict("records"):
        game_date = row.get("gameDate")
        game_id = str(row.get("gameId", "") or "")
        source_updated_at = row.get("gameDateTimeUTC")
        game_status = str(row.get("gameStatusText", "") or "")
        season_value = season
        home_team = str(row.get("homeTeam_teamTricode", "") or "").upper()
        away_team = str(row.get("awayTeam_teamTricode", "") or "").upper()
        if not game_id or not game_date or not home_team or not away_team:
            continue
        team_rows.extend(
            [
                {
                    "SCHEDULE_DATE": game_date,
                    "GAME_ID": game_id,
                    "SEASON": season_value,
                    "TEAM_ABBR": home_team,
                    "OPPONENT_ABBR": away_team,
                    "HOME_AWAY": "HOME",
                    "IS_BACK_TO_BACK": False,
                    "GAME_STATUS": game_status,
                    "SOURCE_UPDATED_AT_UTC": source_updated_at,
                    "INGESTED_AT_UTC": ingested_at,
                },
                {
                    "SCHEDULE_DATE": game_date,
                    "GAME_ID": game_id,
                    "SEASON": season_value,
                    "TEAM_ABBR": away_team,
                    "OPPONENT_ABBR": home_team,
                    "HOME_AWAY": "AWAY",
                    "IS_BACK_TO_BACK": False,
                    "GAME_STATUS": game_status,
                    "SOURCE_UPDATED_AT_UTC": source_updated_at,
                    "INGESTED_AT_UTC": ingested_at,
                },
            ]
        )

    df = pd.DataFrame(team_rows)
    if df.empty:
        return pd.DataFrame(columns=[field.name for field in get_schedule_schema()])
    df = df.drop_duplicates(subset=["GAME_ID", "TEAM_ABBR"]).copy()
    df["SCHEDULE_DATE"] = pd.to_datetime(df["SCHEDULE_DATE"], errors="coerce")
    df = df.sort_values(["TEAM_ABBR", "SCHEDULE_DATE", "GAME_ID"]).reset_index(
        drop=True
    )
    prev_dates = df.groupby("TEAM_ABBR")["SCHEDULE_DATE"].shift(1)
    df["IS_BACK_TO_BACK"] = prev_dates.notna() & (
        (df["SCHEDULE_DATE"] - prev_dates).dt.days == 1
    )
    df["SCHEDULE_DATE"] = df["SCHEDULE_DATE"].dt.date
    return df.reset_index(drop=True)


def run_schedule_quality_checks(
    bq_client: bigquery.Client,
    staging_table: str,
    *,
    season: str = SUPPORTED_SEASON,
) -> dict:
    """Run data quality checks on schedule staging table."""
    dq_query = f"""
    WITH base AS (
      SELECT *
      FROM `{staging_table}`
    ),
    dups AS (
      SELECT COUNT(*) AS duplicate_keys
      FROM (
        SELECT game_id, team_abbr, COUNT(*) AS cnt
        FROM base
        GROUP BY game_id, team_abbr
        HAVING COUNT(*) > 1
      )
    )
    SELECT
      (SELECT COUNT(*) FROM base) AS total_rows,
      (SELECT COUNT(*) FROM base WHERE schedule_date IS NULL OR game_id IS NULL OR team_abbr IS NULL) AS null_key_rows,
      (SELECT duplicate_keys FROM dups) AS duplicate_key_rows,
      (SELECT COUNT(*) FROM base WHERE season != @season OR season IS NULL) AS invalid_season_rows,
      (SELECT COUNT(*) FROM base WHERE home_away IS NULL OR upper(home_away) NOT IN ('HOME', 'AWAY')) AS invalid_home_away_rows
    """
    dq = (
        bq_client.query(
            dq_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("season", "STRING", season),
                ]
            ),
        )
        .to_dataframe()
        .iloc[0]
        .to_dict()
    )
    if dq["null_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['null_key_rows']} schedule rows with null keys"
        )
    if dq["duplicate_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['duplicate_key_rows']} duplicate schedule rows"
        )
    if dq["invalid_season_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['invalid_season_rows']} schedule rows outside season {season}"
        )
    if dq["invalid_home_away_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['invalid_home_away_rows']} schedule rows with invalid home/away values"
        )
    return dq


def create_and_merge_schedule_table(
    bq_client: bigquery.Client, staging_table: str, raw_table: str
) -> Dict[str, int]:
    """Create schedule raw table if needed and merge staging data into it."""
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{raw_table}` (
      schedule_date DATE,
      game_id STRING,
      season STRING,
      team_abbr STRING,
      opponent_abbr STRING,
      home_away STRING,
      is_back_to_back BOOL,
      game_status STRING,
      source_updated_at_utc TIMESTAMP,
      ingested_at_utc TIMESTAMP
    )
    PARTITION BY schedule_date
    CLUSTER BY team_abbr, game_id
    """
    stats_sql = f"""
    SELECT
      COUNTIF(t.game_id IS NULL) AS inserted,
      COUNTIF(
        t.game_id IS NOT NULL
        AND (
          COALESCE(t.opponent_abbr, '') != COALESCE(s.opponent_abbr, '')
          OR COALESCE(t.home_away, '') != COALESCE(s.home_away, '')
          OR COALESCE(t.is_back_to_back, FALSE) != COALESCE(s.is_back_to_back, FALSE)
          OR COALESCE(t.game_status, '') != COALESCE(s.game_status, '')
          OR COALESCE(t.source_updated_at_utc, TIMESTAMP('1970-01-01')) != COALESCE(s.source_updated_at_utc, TIMESTAMP('1970-01-01'))
        )
      ) AS updated
    FROM `{staging_table}` s
    LEFT JOIN `{raw_table}` t
      ON t.game_id = s.game_id
     AND t.team_abbr = s.team_abbr
    """
    merge_sql = f"""
    MERGE `{raw_table}` T
    USING `{staging_table}` S
    ON T.game_id = S.game_id
    AND T.team_abbr = S.team_abbr
    WHEN MATCHED AND (
      COALESCE(T.opponent_abbr, '') != COALESCE(S.opponent_abbr, '')
      OR COALESCE(T.home_away, '') != COALESCE(S.home_away, '')
      OR COALESCE(T.is_back_to_back, FALSE) != COALESCE(S.is_back_to_back, FALSE)
      OR COALESCE(T.game_status, '') != COALESCE(S.game_status, '')
      OR COALESCE(T.source_updated_at_utc, TIMESTAMP('1970-01-01')) != COALESCE(S.source_updated_at_utc, TIMESTAMP('1970-01-01'))
    ) THEN UPDATE SET
      schedule_date = S.schedule_date,
      season = S.season,
      opponent_abbr = S.opponent_abbr,
      home_away = S.home_away,
      is_back_to_back = S.is_back_to_back,
      game_status = S.game_status,
      source_updated_at_utc = S.source_updated_at_utc,
      ingested_at_utc = S.ingested_at_utc
    WHEN NOT MATCHED THEN
      INSERT (schedule_date, game_id, season, team_abbr, opponent_abbr, home_away, is_back_to_back, game_status, source_updated_at_utc, ingested_at_utc)
      VALUES (S.schedule_date, S.game_id, S.season, S.team_abbr, S.opponent_abbr, S.home_away, S.is_back_to_back, S.game_status, S.source_updated_at_utc, S.ingested_at_utc)
    """
    bq_client.query(create_ddl).result()
    pre_count = (
        bq_client.query(f"SELECT COUNT(*) AS c FROM `{raw_table}`")
        .to_dataframe()
        .iloc[0]["c"]
    )
    stats = bq_client.query(stats_sql).to_dataframe().iloc[0].to_dict()
    bq_client.query(merge_sql).result()
    post_count = (
        bq_client.query(f"SELECT COUNT(*) AS c FROM `{raw_table}`")
        .to_dataframe()
        .iloc[0]["c"]
    )
    return {
        "pre_count": int(pre_count),
        "post_count": int(post_count),
        "inserted": int(stats["inserted"]),
        "updated": int(stats["updated"]),
    }


def create_analysis_snapshot_table(
    bq_client: bigquery.Client,
    table_id: str,
) -> None:
    """Create the deterministic analysis snapshot table if it does not exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
      snapshot_id STRING,
      snapshot_date DATE,
      created_at_utc TIMESTAMP,
      season STRING,
      headline STRING,
      dek STRING,
      body STRING,
      trend_player STRING,
      trend_stat STRING,
      trend_delta FLOAT64,
      freshness_ts TIMESTAMP,
      source_run_id STRING
    )
    PARTITION BY snapshot_date
    CLUSTER BY season
    """
    bq_client.query(ddl).result()


def build_analysis_snapshot_record(
    *,
    season: str,
    daily_leaders: pd.DataFrame,
    trends: pd.DataFrame,
    recommendations: Optional[pd.DataFrame] = None,
    rankings: Optional[pd.DataFrame] = None,
    source_run_id: str,
    created_at_utc: Any = None,
    snapshot_date: Any = None,
    freshness_ts: Any = None,
) -> Dict[str, Any]:
    """Build a deterministic analysis snapshot from gold outputs."""
    if season != SUPPORTED_SEASON:
        raise ValueError(f"Analysis snapshots only support season {SUPPORTED_SEASON}")
    created_at = pd.to_datetime(created_at_utc or pd.Timestamp.now(tz="UTC"), utc=True)

    leaders = daily_leaders.copy()
    if leaders.empty:
        raise ValueError("Cannot build analysis snapshot without leaderboard data")
    leaders["game_date"] = pd.to_datetime(leaders["game_date"], errors="coerce")
    leaders = leaders.dropna(subset=["game_date"]).sort_values(
        ["game_date", "pts"], ascending=[False, False]
    )
    if leaders.empty:
        raise ValueError("Cannot build analysis snapshot from invalid leaderboard rows")

    latest_row = leaders.iloc[0]
    snapshot_day = coerce_to_date(snapshot_date) or created_at.date()
    latest_game_date = latest_row["game_date"].date()
    recommendations = (
        recommendations.copy() if recommendations is not None else pd.DataFrame()
    )
    rankings = rankings.copy() if rankings is not None else pd.DataFrame()

    trend_player = ""
    trend_stat = ""
    trend_delta = 0.0
    trend_sentence = "No player trend qualified for the latest snapshot window."
    recommendation_sentence = (
        "No fantasy recommendation qualified for the latest snapshot window."
    )
    ranking_sentence = "No fantasy ranking summary is available yet."

    if not trends.empty:
        trend_working = trends.copy()
        trend_working["delta"] = pd.to_numeric(trend_working["delta"], errors="coerce")
        trend_working = trend_working.dropna(subset=["delta"]).copy()
        if not trend_working.empty:
            trend_working["abs_delta"] = trend_working["delta"].abs()
            trend_working = trend_working.sort_values(
                ["abs_delta", "player_name", "stat"],
                ascending=[False, True, True],
            )
            top_trend = trend_working.iloc[0]
            trend_player = str(top_trend["player_name"])
            trend_stat = str(top_trend["stat"])
            trend_delta = round(float(top_trend["delta"]), 1)
            recent_avg = round(float(top_trend["recent_avg"]), 1)
            prior_avg = round(float(top_trend["prior_avg"]), 1)
            direction = "up" if trend_delta >= 0 else "down"
            trend_sentence = (
                f"{trend_player} is trending {direction} in {trend_stat}, moving from "
                f"{prior_avg:.1f} to {recent_avg:.1f} per game ({trend_delta:+.1f})."
            )

    top_recommendation = None
    if not recommendations.empty:
        recommendation_working = recommendations.copy()
        recommendation_working["priority_score"] = pd.to_numeric(
            recommendation_working["priority_score"], errors="coerce"
        )
        recommendation_working["confidence_score"] = pd.to_numeric(
            recommendation_working["confidence_score"], errors="coerce"
        )
        recommendation_working = recommendation_working.dropna(
            subset=["priority_score", "confidence_score"]
        ).copy()
        if not recommendation_working.empty:
            recommendation_working = recommendation_working.sort_values(
                ["priority_score", "confidence_score", "player_name"],
                ascending=[False, False, True],
            )
            top_recommendation = recommendation_working.iloc[0]
            recommendation_sentence = (
                f"Top fantasy signal: {top_recommendation['player_name']} profiles as "
                f"{top_recommendation['insight_type']} with recommendation "
                f"{top_recommendation['recommendation']} and priority "
                f"{float(top_recommendation['priority_score']):.1f}."
            )

    if not rankings.empty:
        ranking_working = rankings.copy()
        rank_col = (
            "fantasy_rank_9cat_proxy"
            if "fantasy_rank_9cat_proxy" in ranking_working.columns
            else "overall_rank"
        )
        if rank_col in ranking_working.columns:
            ranking_working[rank_col] = pd.to_numeric(
                ranking_working[rank_col], errors="coerce"
            )
            ranking_working = ranking_working.dropna(subset=[rank_col]).copy()
            if not ranking_working.empty:
                ranking_working = ranking_working.sort_values(
                    [rank_col, "player_name"], ascending=[True, True]
                )
                top_ranked = ranking_working.iloc[0]
                ranking_sentence = (
                    f"Current fantasy leader: {top_ranked['player_name']} sits at rank "
                    f"{int(top_ranked[rank_col])} with tier "
                    f"{top_ranked.get('recommendation_tier', 'n/a')}."
                )

    headline = (
        f"{top_recommendation['player_name']} headlines the {season} fantasy board"
        if top_recommendation is not None
        else (
            f"{latest_row['pts_leader']} sets the pace for the {season} nightly board"
            if not trend_player
            else f"{trend_player} headlines the {season} trend watch"
        )
    )
    dek = (
        f"Latest leaders from {latest_game_date.isoformat()} are anchored by "
        f"{latest_row['pts_leader']} in scoring, {latest_row['reb_leader']} on the glass, "
        f"and {latest_row['ast_leader']} as the top playmaker. {recommendation_sentence}"
    )
    body = "\n\n".join(
        [
            (
                f"The latest completed game day in the {season} warehouse is {latest_game_date.isoformat()}. "
                f"{latest_row['pts_leader']} led scoring with {int(latest_row['pts'])} points in "
                f"{latest_row['pts_matchup']}, while {latest_row['reb_leader']} posted "
                f"{int(latest_row['reb'])} rebounds and {latest_row['ast_leader']} handed out "
                f"{int(latest_row['ast'])} assists."
            ),
            trend_sentence,
            ranking_sentence,
            recommendation_sentence,
            (
                f"This snapshot was generated deterministically from gold tables and linked to "
                f"pipeline run {source_run_id}. Freshness is measured from "
                f"{pd.to_datetime(freshness_ts or created_at, utc=True).isoformat()}."
            ),
        ]
    )

    return {
        "snapshot_id": f"{season.replace('-', '')}_{snapshot_day.strftime('%Y%m%d')}",
        "snapshot_date": snapshot_day.isoformat(),
        "created_at_utc": created_at.isoformat(),
        "season": season,
        "headline": headline,
        "dek": dek,
        "body": body,
        "trend_player": trend_player,
        "trend_stat": trend_stat,
        "trend_delta": trend_delta,
        "freshness_ts": pd.to_datetime(
            freshness_ts or created_at, utc=True
        ).isoformat(),
        "source_run_id": source_run_id,
    }


def upsert_analysis_snapshot(
    bq_client: bigquery.Client,
    table_id: str,
    record: Dict[str, Any],
) -> None:
    """Upsert a deterministic analysis snapshot keyed by snapshot_id."""
    create_analysis_snapshot_table(bq_client, table_id)
    merge_sql = f"""
    MERGE `{table_id}` T
    USING (
      SELECT
        @snapshot_id AS snapshot_id,
        @snapshot_date AS snapshot_date,
        @created_at_utc AS created_at_utc,
        @season AS season,
        @headline AS headline,
        @dek AS dek,
        @body AS body,
        @trend_player AS trend_player,
        @trend_stat AS trend_stat,
        @trend_delta AS trend_delta,
        @freshness_ts AS freshness_ts,
        @source_run_id AS source_run_id
    ) S
    ON T.snapshot_id = S.snapshot_id
    WHEN MATCHED THEN
      UPDATE SET
        snapshot_date = S.snapshot_date,
        created_at_utc = S.created_at_utc,
        season = S.season,
        headline = S.headline,
        dek = S.dek,
        body = S.body,
        trend_player = S.trend_player,
        trend_stat = S.trend_stat,
        trend_delta = S.trend_delta,
        freshness_ts = S.freshness_ts,
        source_run_id = S.source_run_id
    WHEN NOT MATCHED THEN
      INSERT (
        snapshot_id, snapshot_date, created_at_utc, season, headline, dek, body,
        trend_player, trend_stat, trend_delta, freshness_ts, source_run_id
      )
      VALUES (
        S.snapshot_id, S.snapshot_date, S.created_at_utc, S.season, S.headline, S.dek, S.body,
        S.trend_player, S.trend_stat, S.trend_delta, S.freshness_ts, S.source_run_id
      )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "snapshot_id", "STRING", record["snapshot_id"]
            ),
            bigquery.ScalarQueryParameter(
                "snapshot_date", "DATE", record["snapshot_date"]
            ),
            bigquery.ScalarQueryParameter(
                "created_at_utc", "TIMESTAMP", record["created_at_utc"]
            ),
            bigquery.ScalarQueryParameter("season", "STRING", record["season"]),
            bigquery.ScalarQueryParameter("headline", "STRING", record["headline"]),
            bigquery.ScalarQueryParameter("dek", "STRING", record["dek"]),
            bigquery.ScalarQueryParameter("body", "STRING", record["body"]),
            bigquery.ScalarQueryParameter(
                "trend_player", "STRING", record["trend_player"]
            ),
            bigquery.ScalarQueryParameter("trend_stat", "STRING", record["trend_stat"]),
            bigquery.ScalarQueryParameter(
                "trend_delta", "FLOAT64", record["trend_delta"]
            ),
            bigquery.ScalarQueryParameter(
                "freshness_ts", "TIMESTAMP", record["freshness_ts"]
            ),
            bigquery.ScalarQueryParameter(
                "source_run_id", "STRING", record["source_run_id"]
            ),
        ]
    )
    bq_client.query(merge_sql, job_config=job_config).result()
