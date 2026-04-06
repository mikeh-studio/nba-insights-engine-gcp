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
from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import commonplayerinfo
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
        "Game_ID",
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
            out = out.rename(columns={"Game_ID": "GAME_ID"})
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


def get_game_line_scores(
    game_id: str,
    *,
    season: str = SUPPORTED_SEASON,
    retries: int = 3,
    delay: float = 0.8,
) -> pd.DataFrame:
    """Get normalized team line scores for a single game."""
    line_score_fields = [
        "GAME_DATE_EST",
        "GAME_ID",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "TEAM_CITY_NAME",
        "TEAM_NICKNAME",
        "TEAM_WINS_LOSSES",
        "PTS_QTR1",
        "PTS_QTR2",
        "PTS_QTR3",
        "PTS_QTR4",
        "PTS_OT1",
        "PTS_OT2",
        "PTS_OT3",
        "PTS_OT4",
        "PTS_OT5",
        "PTS_OT6",
        "PTS_OT7",
        "PTS_OT8",
        "PTS_OT9",
        "PTS_OT10",
        "PTS",
    ]

    empty = pd.DataFrame(columns=[field.name for field in get_game_line_scores_schema()])

    for attempt in range(1, retries + 1):
        try:
            summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
            available = summary.get_available_data()
            if "LineScore" not in available:
                return empty.copy()
            line_score = summary.get_data_frames()[available.index("LineScore")].copy()
            missing_cols = [c for c in line_score_fields if c not in line_score.columns]
            if missing_cols:
                raise ValueError(f"Missing expected line score columns: {missing_cols}")

            out = line_score[line_score_fields].copy()
            out = out.rename(
                columns={
                    "GAME_DATE_EST": "GAME_DATE",
                    "TEAM_ABBREVIATION": "TEAM_ABBR",
                }
            )
            out["GAME_DATE"] = pd.to_datetime(out["GAME_DATE"], errors="coerce").dt.date
            out = out.dropna(subset=["GAME_DATE", "GAME_ID", "TEAM_ID"]).copy()
            out["TEAM_ABBR"] = out["TEAM_ABBR"].astype("string").str.upper()
            out["SEASON"] = season
            numeric_cols = ["TEAM_ID", "PTS_QTR1", "PTS_QTR2", "PTS_QTR3", "PTS_QTR4"]
            numeric_cols.extend([f"PTS_OT{idx}" for idx in range(1, 11)])
            numeric_cols.append("PTS")
            for col in numeric_cols:
                out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
            out["INGESTED_AT_UTC"] = pd.Timestamp.now(tz="UTC")
            return out[[field.name for field in get_game_line_scores_schema()]]
        except Exception:
            if attempt == retries:
                logger.exception("Failed game_id=%s after %s attempts", game_id, retries)
                return empty.copy()
            sleep_seconds = delay * attempt
            logger.warning(
                "Retrying game_id=%s attempt=%s/%s in %.1fs",
                game_id,
                attempt,
                retries,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    return empty.copy()


def get_all_game_line_scores(
    game_ids: Iterable[Any],
    *,
    season: str = SUPPORTED_SEASON,
    delay: float = 0.4,
) -> pd.DataFrame:
    """Fetch team line scores for many games with rate limiting."""
    seen: set[str] = set()
    normalized_ids: list[str] = []
    for value in game_ids:
        if value in (None, ""):
            continue
        normalized = str(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_ids.append(normalized)

    all_scores = []
    for idx, game_id in enumerate(normalized_ids, start=1):
        logger.info("Fetching line score %s/%s: %s", idx, len(normalized_ids), game_id)
        line_scores = get_game_line_scores(game_id, season=season)
        if not line_scores.empty:
            all_scores.append(line_scores)
        time.sleep(delay)

    if not all_scores:
        return pd.DataFrame(columns=[field.name for field in get_game_line_scores_schema()])

    combined = pd.concat(all_scores, ignore_index=True)
    combined = combined.drop_duplicates(subset=["GAME_ID", "TEAM_ID"]).copy()
    combined = combined.sort_values(["GAME_DATE", "GAME_ID", "TEAM_ID"]).reset_index(
        drop=True
    )
    return combined[[field.name for field in get_game_line_scores_schema()]]


def get_player_reference(
    player_id: int,
    *,
    retries: int = 3,
    delay: float = 0.8,
) -> pd.DataFrame:
    """Get normalized player reference attributes for a single player."""
    expected_cols = [
        "PERSON_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "DISPLAY_FIRST_LAST",
        "PLAYER_SLUG",
        "BIRTHDATE",
        "SCHOOL",
        "COUNTRY",
        "LAST_AFFILIATION",
        "HEIGHT",
        "WEIGHT",
        "SEASON_EXP",
        "JERSEY",
        "POSITION",
        "ROSTERSTATUS",
        "TEAM_ID",
        "TEAM_NAME",
        "TEAM_ABBREVIATION",
        "TEAM_CODE",
        "TEAM_CITY",
        "FROM_YEAR",
        "TO_YEAR",
        "DRAFT_YEAR",
        "DRAFT_ROUND",
        "DRAFT_NUMBER",
    ]
    empty = pd.DataFrame(columns=[field.name for field in get_player_reference_schema()])

    for attempt in range(1, retries + 1):
        try:
            info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            available = info.get_available_data()
            if "CommonPlayerInfo" not in available:
                return empty.copy()
            profile = info.get_data_frames()[available.index("CommonPlayerInfo")].copy()
            if profile.empty:
                return empty.copy()
            missing_cols = [c for c in expected_cols if c not in profile.columns]
            if missing_cols:
                raise ValueError(
                    f"Missing expected player reference columns: {missing_cols}"
                )

            out = profile[expected_cols].head(1).copy()
            out = out.rename(
                columns={
                    "PERSON_ID": "PLAYER_ID",
                    "DISPLAY_FIRST_LAST": "PLAYER_NAME",
                    "TEAM_ABBREVIATION": "TEAM_ABBR",
                    "ROSTERSTATUS": "ROSTER_STATUS",
                }
            )
            out["BIRTHDATE"] = pd.to_datetime(out["BIRTHDATE"], errors="coerce").dt.date
            for col in (
                "PLAYER_ID",
                "WEIGHT",
                "SEASON_EXP",
                "TEAM_ID",
                "FROM_YEAR",
                "TO_YEAR",
            ):
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            out["PLAYER_ID"] = out["PLAYER_ID"].fillna(player_id).astype("Int64")
            out["TEAM_ABBR"] = out["TEAM_ABBR"].astype("string").str.upper()
            out["ROSTER_STATUS"] = pd.to_numeric(
                out["ROSTER_STATUS"], errors="coerce"
            ).fillna(0).astype(int).astype(bool)
            out["INGESTED_AT_UTC"] = pd.Timestamp.now(tz="UTC")
            return out[[field.name for field in get_player_reference_schema()]]
        except Exception:
            if attempt == retries:
                logger.exception(
                    "Failed player reference player_id=%s after %s attempts",
                    player_id,
                    retries,
                )
                return empty.copy()
            sleep_seconds = delay * attempt
            logger.warning(
                "Retrying player reference player_id=%s attempt=%s/%s in %.1fs",
                player_id,
                attempt,
                retries,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    return empty.copy()


def get_all_player_references(
    player_list: Iterable[dict],
    *,
    delay: float = 0.4,
) -> pd.DataFrame:
    """Fetch player reference data for many players with rate limiting."""
    all_profiles = []
    player_list = list(player_list)

    for idx, player in enumerate(player_list, start=1):
        player_id = player["id"]
        player_name = player["full_name"]
        logger.info(
            "Fetching player reference %s/%s: %s", idx, len(player_list), player_name
        )
        profile = get_player_reference(player_id)
        if not profile.empty:
            all_profiles.append(profile)
        time.sleep(delay)

    if not all_profiles:
        return pd.DataFrame(columns=[field.name for field in get_player_reference_schema()])

    combined = pd.concat(all_profiles, ignore_index=True)
    combined = combined.drop_duplicates(subset=["PLAYER_ID"]).copy()
    combined = combined.sort_values(["PLAYER_ID"]).reset_index(drop=True)
    return combined[[field.name for field in get_player_reference_schema()]]


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
        bigquery.SchemaField("GAME_ID", "STRING"),
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


def get_game_line_scores_schema() -> List[bigquery.SchemaField]:
    """Return the BigQuery schema for team line scores."""
    return [
        bigquery.SchemaField("GAME_DATE", "DATE"),
        bigquery.SchemaField("GAME_ID", "STRING"),
        bigquery.SchemaField("SEASON", "STRING"),
        bigquery.SchemaField("TEAM_ID", "INTEGER"),
        bigquery.SchemaField("TEAM_ABBR", "STRING"),
        bigquery.SchemaField("TEAM_CITY_NAME", "STRING"),
        bigquery.SchemaField("TEAM_NICKNAME", "STRING"),
        bigquery.SchemaField("TEAM_WINS_LOSSES", "STRING"),
        bigquery.SchemaField("PTS_QTR1", "INTEGER"),
        bigquery.SchemaField("PTS_QTR2", "INTEGER"),
        bigquery.SchemaField("PTS_QTR3", "INTEGER"),
        bigquery.SchemaField("PTS_QTR4", "INTEGER"),
        bigquery.SchemaField("PTS_OT1", "INTEGER"),
        bigquery.SchemaField("PTS_OT2", "INTEGER"),
        bigquery.SchemaField("PTS_OT3", "INTEGER"),
        bigquery.SchemaField("PTS_OT4", "INTEGER"),
        bigquery.SchemaField("PTS_OT5", "INTEGER"),
        bigquery.SchemaField("PTS_OT6", "INTEGER"),
        bigquery.SchemaField("PTS_OT7", "INTEGER"),
        bigquery.SchemaField("PTS_OT8", "INTEGER"),
        bigquery.SchemaField("PTS_OT9", "INTEGER"),
        bigquery.SchemaField("PTS_OT10", "INTEGER"),
        bigquery.SchemaField("PTS", "INTEGER"),
        bigquery.SchemaField("INGESTED_AT_UTC", "TIMESTAMP"),
    ]


def get_player_reference_schema() -> List[bigquery.SchemaField]:
    """Return the BigQuery schema for player reference data."""
    return [
        bigquery.SchemaField("PLAYER_ID", "INTEGER"),
        bigquery.SchemaField("FIRST_NAME", "STRING"),
        bigquery.SchemaField("LAST_NAME", "STRING"),
        bigquery.SchemaField("PLAYER_NAME", "STRING"),
        bigquery.SchemaField("PLAYER_SLUG", "STRING"),
        bigquery.SchemaField("BIRTHDATE", "DATE"),
        bigquery.SchemaField("SCHOOL", "STRING"),
        bigquery.SchemaField("COUNTRY", "STRING"),
        bigquery.SchemaField("LAST_AFFILIATION", "STRING"),
        bigquery.SchemaField("HEIGHT", "STRING"),
        bigquery.SchemaField("WEIGHT", "INTEGER"),
        bigquery.SchemaField("SEASON_EXP", "INTEGER"),
        bigquery.SchemaField("JERSEY", "STRING"),
        bigquery.SchemaField("POSITION", "STRING"),
        bigquery.SchemaField("ROSTER_STATUS", "BOOLEAN"),
        bigquery.SchemaField("TEAM_ID", "INTEGER"),
        bigquery.SchemaField("TEAM_NAME", "STRING"),
        bigquery.SchemaField("TEAM_ABBR", "STRING"),
        bigquery.SchemaField("TEAM_CODE", "STRING"),
        bigquery.SchemaField("TEAM_CITY", "STRING"),
        bigquery.SchemaField("FROM_YEAR", "INTEGER"),
        bigquery.SchemaField("TO_YEAR", "INTEGER"),
        bigquery.SchemaField("DRAFT_YEAR", "STRING"),
        bigquery.SchemaField("DRAFT_ROUND", "STRING"),
        bigquery.SchemaField("DRAFT_NUMBER", "STRING"),
        bigquery.SchemaField("INGESTED_AT_UTC", "TIMESTAMP"),
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


def run_game_line_score_quality_checks(
    bq_client: bigquery.Client,
    staging_table: str,
    *,
    season: str = SUPPORTED_SEASON,
) -> dict:
    """Run data quality checks on game line score staging rows."""
    dq_query = f"""
    WITH base AS (
      SELECT *
      FROM `{staging_table}`
    ),
    dups AS (
      SELECT COUNT(*) AS duplicate_keys
      FROM (
        SELECT game_id, team_id, COUNT(*) AS cnt
        FROM base
        GROUP BY game_id, team_id
        HAVING COUNT(*) > 1
      )
    )
    SELECT
      (SELECT COUNT(*) FROM base) AS total_rows,
      (SELECT COUNT(*) FROM base WHERE game_id IS NULL OR team_id IS NULL OR team_abbr IS NULL) AS null_key_rows,
      (SELECT duplicate_keys FROM dups) AS duplicate_key_rows,
      (SELECT COUNT(*) FROM base WHERE season != @season OR season IS NULL) AS invalid_season_rows,
      (SELECT COUNT(*) FROM base WHERE pts IS NULL OR pts < 0) AS invalid_points_rows
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
    if dq["total_rows"] == 0:
        raise ValueError("DQ failed: game line score staging table has zero rows")
    if dq["null_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['null_key_rows']} game line score rows with null keys"
        )
    if dq["duplicate_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['duplicate_key_rows']} duplicate game line score rows"
        )
    if dq["invalid_season_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['invalid_season_rows']} game line score rows outside season {season}"
        )
    if dq["invalid_points_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['invalid_points_rows']} game line score rows with invalid points"
        )
    return dq


def run_player_reference_quality_checks(
    bq_client: bigquery.Client,
    staging_table: str,
) -> dict:
    """Run data quality checks on player reference staging rows."""
    dq_query = f"""
    WITH base AS (
      SELECT *
      FROM `{staging_table}`
    ),
    dups AS (
      SELECT COUNT(*) AS duplicate_keys
      FROM (
        SELECT player_id, COUNT(*) AS cnt
        FROM base
        GROUP BY player_id
        HAVING COUNT(*) > 1
      )
    )
    SELECT
      (SELECT COUNT(*) FROM base) AS total_rows,
      (SELECT COUNT(*) FROM base WHERE player_id IS NULL) AS null_key_rows,
      (SELECT duplicate_keys FROM dups) AS duplicate_key_rows,
      (SELECT COUNT(*) FROM base WHERE player_name IS NULL OR trim(player_name) = '') AS missing_name_rows
    """
    dq = (
        bq_client.query(dq_query)
        .to_dataframe()
        .iloc[0]
        .to_dict()
    )
    if dq["total_rows"] == 0:
        raise ValueError("DQ failed: player reference staging table has zero rows")
    if dq["null_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['null_key_rows']} player reference rows with null player_id"
        )
    if dq["duplicate_key_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['duplicate_key_rows']} duplicate player reference rows"
        )
    if dq["missing_name_rows"] > 0:
        raise ValueError(
            f"DQ failed: found {dq['missing_name_rows']} player reference rows without a player_name"
        )
    return dq


def validate_merge_reconciliation(
    *,
    domain: str,
    rows_loaded: int,
    pre_count: int,
    post_count: int,
    inserted: int,
    updated: int,
) -> Dict[str, int]:
    """Validate that merge accounting is internally consistent."""
    if rows_loaded < 0:
        raise ValueError(f"Reconciliation failed for {domain}: rows_loaded cannot be negative")
    if any(value < 0 for value in (pre_count, post_count, inserted, updated)):
        raise ValueError(
            f"Reconciliation failed for {domain}: merge counts cannot be negative"
        )

    if inserted + updated > rows_loaded:
        raise ValueError(
            f"Reconciliation failed for {domain}: inserted+updated "
            f"({inserted + updated}) exceeds rows_loaded ({rows_loaded})"
        )

    expected_post_count = pre_count + inserted
    if post_count != expected_post_count:
        raise ValueError(
            f"Reconciliation failed for {domain}: expected post_count "
            f"{expected_post_count} from pre_count {pre_count} + inserted {inserted}, "
            f"got {post_count}"
        )

    result = {
        "rows_loaded": int(rows_loaded),
        "pre_count": int(pre_count),
        "post_count": int(post_count),
        "inserted": int(inserted),
        "updated": int(updated),
        "unchanged": int(rows_loaded - inserted - updated),
    }
    logger.info("Reconciliation passed for %s: %s", domain, result)
    return result


def create_and_merge_raw_table(
    bq_client: bigquery.Client, staging_table: str, raw_table: str
) -> Dict[str, int]:
    """Create raw table if needed and MERGE staging data into it."""
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{raw_table}` (
      game_id STRING,
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
          COALESCE(t.game_id, '') != COALESCE(s.game_id, '')
          OR
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
      COALESCE(T.game_id, '') != COALESCE(S.game_id, '')
      OR
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
        game_id = S.game_id,
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
      INSERT (game_id, game_date, matchup, wl, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
              ftm, fta, ft_pct, oreb, dreb, pts, reb, ast, stl, blk, tov, pf, plus_minus,
              season, ingested_at_utc, player_id, player_name)
      VALUES (S.game_id, S.game_date, S.matchup, S.wl, S.min, S.fgm, S.fga, S.fg_pct, S.fg3m, S.fg3a,
              S.fg3_pct, S.ftm, S.fta, S.ft_pct, S.oreb, S.dreb, S.pts, S.reb, S.ast, S.stl,
              S.blk, S.tov, S.pf, S.plus_minus, S.season, S.ingested_at_utc, S.player_id,
              S.player_name)
    """

    bq_client.query(create_ddl).result()
    bq_client.query(
        f"ALTER TABLE `{raw_table}` ADD COLUMN IF NOT EXISTS game_id STRING"
    ).result()
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


def create_and_merge_game_line_scores_table(
    bq_client: bigquery.Client, staging_table: str, raw_table: str
) -> Dict[str, int]:
    """Create line score raw table if needed and MERGE staging data into it."""
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{raw_table}` (
      game_date DATE,
      game_id STRING,
      season STRING,
      team_id INT64,
      team_abbr STRING,
      team_city_name STRING,
      team_nickname STRING,
      team_wins_losses STRING,
      pts_qtr1 INT64,
      pts_qtr2 INT64,
      pts_qtr3 INT64,
      pts_qtr4 INT64,
      pts_ot1 INT64,
      pts_ot2 INT64,
      pts_ot3 INT64,
      pts_ot4 INT64,
      pts_ot5 INT64,
      pts_ot6 INT64,
      pts_ot7 INT64,
      pts_ot8 INT64,
      pts_ot9 INT64,
      pts_ot10 INT64,
      pts INT64,
      ingested_at_utc TIMESTAMP
    )
    PARTITION BY game_date
    CLUSTER BY game_id, team_id
    """
    change_predicate = """
      COALESCE(T.game_date, DATE '1970-01-01') != COALESCE(S.game_date, DATE '1970-01-01')
      OR COALESCE(T.season, '') != COALESCE(S.season, '')
      OR COALESCE(T.team_abbr, '') != COALESCE(S.team_abbr, '')
      OR COALESCE(T.team_city_name, '') != COALESCE(S.team_city_name, '')
      OR COALESCE(T.team_nickname, '') != COALESCE(S.team_nickname, '')
      OR COALESCE(T.team_wins_losses, '') != COALESCE(S.team_wins_losses, '')
      OR COALESCE(T.pts_qtr1, 0) != COALESCE(S.pts_qtr1, 0)
      OR COALESCE(T.pts_qtr2, 0) != COALESCE(S.pts_qtr2, 0)
      OR COALESCE(T.pts_qtr3, 0) != COALESCE(S.pts_qtr3, 0)
      OR COALESCE(T.pts_qtr4, 0) != COALESCE(S.pts_qtr4, 0)
      OR COALESCE(T.pts_ot1, 0) != COALESCE(S.pts_ot1, 0)
      OR COALESCE(T.pts_ot2, 0) != COALESCE(S.pts_ot2, 0)
      OR COALESCE(T.pts_ot3, 0) != COALESCE(S.pts_ot3, 0)
      OR COALESCE(T.pts_ot4, 0) != COALESCE(S.pts_ot4, 0)
      OR COALESCE(T.pts_ot5, 0) != COALESCE(S.pts_ot5, 0)
      OR COALESCE(T.pts_ot6, 0) != COALESCE(S.pts_ot6, 0)
      OR COALESCE(T.pts_ot7, 0) != COALESCE(S.pts_ot7, 0)
      OR COALESCE(T.pts_ot8, 0) != COALESCE(S.pts_ot8, 0)
      OR COALESCE(T.pts_ot9, 0) != COALESCE(S.pts_ot9, 0)
      OR COALESCE(T.pts_ot10, 0) != COALESCE(S.pts_ot10, 0)
      OR COALESCE(T.pts, 0) != COALESCE(S.pts, 0)
    """
    stats_sql = f"""
    SELECT
      COUNTIF(t.game_id IS NULL) AS inserted,
      COUNTIF(t.game_id IS NOT NULL AND ({change_predicate.replace('T.', 't.').replace('S.', 's.')})) AS updated
    FROM `{staging_table}` s
    LEFT JOIN `{raw_table}` t
      ON t.game_id = s.game_id
     AND t.team_id = s.team_id
    """
    merge_sql = f"""
    MERGE `{raw_table}` T
    USING `{staging_table}` S
    ON T.game_id = S.game_id
    AND T.team_id = S.team_id
    WHEN MATCHED AND ({change_predicate}) THEN
      UPDATE SET
        game_date = S.game_date,
        season = S.season,
        team_abbr = S.team_abbr,
        team_city_name = S.team_city_name,
        team_nickname = S.team_nickname,
        team_wins_losses = S.team_wins_losses,
        pts_qtr1 = S.pts_qtr1,
        pts_qtr2 = S.pts_qtr2,
        pts_qtr3 = S.pts_qtr3,
        pts_qtr4 = S.pts_qtr4,
        pts_ot1 = S.pts_ot1,
        pts_ot2 = S.pts_ot2,
        pts_ot3 = S.pts_ot3,
        pts_ot4 = S.pts_ot4,
        pts_ot5 = S.pts_ot5,
        pts_ot6 = S.pts_ot6,
        pts_ot7 = S.pts_ot7,
        pts_ot8 = S.pts_ot8,
        pts_ot9 = S.pts_ot9,
        pts_ot10 = S.pts_ot10,
        pts = S.pts,
        ingested_at_utc = S.ingested_at_utc
    WHEN NOT MATCHED THEN
      INSERT (
        game_date, game_id, season, team_id, team_abbr, team_city_name, team_nickname,
        team_wins_losses, pts_qtr1, pts_qtr2, pts_qtr3, pts_qtr4, pts_ot1, pts_ot2,
        pts_ot3, pts_ot4, pts_ot5, pts_ot6, pts_ot7, pts_ot8, pts_ot9, pts_ot10, pts,
        ingested_at_utc
      )
      VALUES (
        S.game_date, S.game_id, S.season, S.team_id, S.team_abbr, S.team_city_name,
        S.team_nickname, S.team_wins_losses, S.pts_qtr1, S.pts_qtr2, S.pts_qtr3,
        S.pts_qtr4, S.pts_ot1, S.pts_ot2, S.pts_ot3, S.pts_ot4, S.pts_ot5, S.pts_ot6,
        S.pts_ot7, S.pts_ot8, S.pts_ot9, S.pts_ot10, S.pts, S.ingested_at_utc
      )
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


def create_and_merge_player_reference_table(
    bq_client: bigquery.Client, staging_table: str, raw_table: str
) -> Dict[str, int]:
    """Create player reference raw table if needed and MERGE staging data into it."""
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{raw_table}` (
      player_id INT64,
      first_name STRING,
      last_name STRING,
      player_name STRING,
      player_slug STRING,
      birthdate DATE,
      school STRING,
      country STRING,
      last_affiliation STRING,
      height STRING,
      weight INT64,
      season_exp INT64,
      jersey STRING,
      position STRING,
      roster_status BOOL,
      team_id INT64,
      team_name STRING,
      team_abbr STRING,
      team_code STRING,
      team_city STRING,
      from_year INT64,
      to_year INT64,
      draft_year STRING,
      draft_round STRING,
      draft_number STRING,
      ingested_at_utc TIMESTAMP
    )
    CLUSTER BY player_id
    """
    change_predicate = """
      COALESCE(T.first_name, '') != COALESCE(S.first_name, '')
      OR COALESCE(T.last_name, '') != COALESCE(S.last_name, '')
      OR COALESCE(T.player_name, '') != COALESCE(S.player_name, '')
      OR COALESCE(T.player_slug, '') != COALESCE(S.player_slug, '')
      OR COALESCE(T.birthdate, DATE '1970-01-01') != COALESCE(S.birthdate, DATE '1970-01-01')
      OR COALESCE(T.school, '') != COALESCE(S.school, '')
      OR COALESCE(T.country, '') != COALESCE(S.country, '')
      OR COALESCE(T.last_affiliation, '') != COALESCE(S.last_affiliation, '')
      OR COALESCE(T.height, '') != COALESCE(S.height, '')
      OR COALESCE(T.weight, -1) != COALESCE(S.weight, -1)
      OR COALESCE(T.season_exp, -1) != COALESCE(S.season_exp, -1)
      OR COALESCE(T.jersey, '') != COALESCE(S.jersey, '')
      OR COALESCE(T.position, '') != COALESCE(S.position, '')
      OR COALESCE(T.roster_status, FALSE) != COALESCE(S.roster_status, FALSE)
      OR COALESCE(T.team_id, -1) != COALESCE(S.team_id, -1)
      OR COALESCE(T.team_name, '') != COALESCE(S.team_name, '')
      OR COALESCE(T.team_abbr, '') != COALESCE(S.team_abbr, '')
      OR COALESCE(T.team_code, '') != COALESCE(S.team_code, '')
      OR COALESCE(T.team_city, '') != COALESCE(S.team_city, '')
      OR COALESCE(T.from_year, -1) != COALESCE(S.from_year, -1)
      OR COALESCE(T.to_year, -1) != COALESCE(S.to_year, -1)
      OR COALESCE(T.draft_year, '') != COALESCE(S.draft_year, '')
      OR COALESCE(T.draft_round, '') != COALESCE(S.draft_round, '')
      OR COALESCE(T.draft_number, '') != COALESCE(S.draft_number, '')
    """
    stats_sql = f"""
    SELECT
      COUNTIF(t.player_id IS NULL) AS inserted,
      COUNTIF(t.player_id IS NOT NULL AND ({change_predicate.replace('T.', 't.').replace('S.', 's.')})) AS updated
    FROM `{staging_table}` s
    LEFT JOIN `{raw_table}` t
      ON t.player_id = s.player_id
    """
    merge_sql = f"""
    MERGE `{raw_table}` T
    USING `{staging_table}` S
    ON T.player_id = S.player_id
    WHEN MATCHED AND ({change_predicate}) THEN
      UPDATE SET
        first_name = S.first_name,
        last_name = S.last_name,
        player_name = S.player_name,
        player_slug = S.player_slug,
        birthdate = S.birthdate,
        school = S.school,
        country = S.country,
        last_affiliation = S.last_affiliation,
        height = S.height,
        weight = S.weight,
        season_exp = S.season_exp,
        jersey = S.jersey,
        position = S.position,
        roster_status = S.roster_status,
        team_id = S.team_id,
        team_name = S.team_name,
        team_abbr = S.team_abbr,
        team_code = S.team_code,
        team_city = S.team_city,
        from_year = S.from_year,
        to_year = S.to_year,
        draft_year = S.draft_year,
        draft_round = S.draft_round,
        draft_number = S.draft_number,
        ingested_at_utc = S.ingested_at_utc
    WHEN NOT MATCHED THEN
      INSERT (
        player_id, first_name, last_name, player_name, player_slug, birthdate,
        school, country, last_affiliation, height, weight, season_exp, jersey,
        position, roster_status, team_id, team_name, team_abbr, team_code,
        team_city, from_year, to_year, draft_year, draft_round, draft_number,
        ingested_at_utc
      )
      VALUES (
        S.player_id, S.first_name, S.last_name, S.player_name, S.player_slug,
        S.birthdate, S.school, S.country, S.last_affiliation, S.height, S.weight,
        S.season_exp, S.jersey, S.position, S.roster_status, S.team_id, S.team_name,
        S.team_abbr, S.team_code, S.team_city, S.from_year, S.to_year, S.draft_year,
        S.draft_round, S.draft_number, S.ingested_at_utc
      )
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
      contribution_player_id INT64,
      contribution_player_name STRING,
      contribution_team_abbr STRING,
      contribution_opponent_abbr STRING,
      contribution_matchup STRING,
      contribution_player_pts INT64,
      contribution_team_pts INT64,
      contribution_opponent_team_pts INT64,
      contribution_player_points_share_of_team FLOAT64,
      contribution_player_points_share_of_game FLOAT64,
      contribution_scoring_margin INT64,
      contribution_team_pts_qtr1 INT64,
      contribution_team_pts_qtr2 INT64,
      contribution_team_pts_qtr3 INT64,
      contribution_team_pts_qtr4 INT64,
      contribution_team_pts_ot_total INT64,
      contribution_game_date DATE,
      context_player_id INT64,
      context_player_name STRING,
      context_team_abbr STRING,
      context_team_name STRING,
      context_position STRING,
      context_height STRING,
      context_weight INT64,
      context_roster_status BOOL,
      context_season_exp INT64,
      context_draft_year STRING,
      context_draft_round STRING,
      context_draft_number STRING,
      freshness_ts TIMESTAMP,
      source_run_id STRING
    )
    PARTITION BY snapshot_date
    CLUSTER BY season
    """
    bq_client.query(ddl).result()
    for column_ddl in [
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_player_id INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_player_name STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_team_abbr STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_opponent_abbr STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_matchup STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_player_pts INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_team_pts INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_opponent_team_pts INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_player_points_share_of_team FLOAT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_player_points_share_of_game FLOAT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_scoring_margin INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_team_pts_qtr1 INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_team_pts_qtr2 INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_team_pts_qtr3 INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_team_pts_qtr4 INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_team_pts_ot_total INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS contribution_game_date DATE",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_player_id INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_player_name STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_team_abbr STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_team_name STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_position STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_height STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_weight INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_roster_status BOOL",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_season_exp INT64",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_draft_year STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_draft_round STRING",
        "ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS context_draft_number STRING",
    ]:
        bq_client.query(column_ddl.format(table=table_id)).result()


def build_analysis_snapshot_record(
    *,
    season: str,
    daily_leaders: pd.DataFrame,
    trends: pd.DataFrame,
    recommendations: Optional[pd.DataFrame] = None,
    rankings: Optional[pd.DataFrame] = None,
    score_contribution: Optional[pd.DataFrame] = None,
    player_context: Optional[pd.DataFrame] = None,
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
    score_contribution = (
        score_contribution.copy() if score_contribution is not None else pd.DataFrame()
    )
    player_context = (
        player_context.copy() if player_context is not None else pd.DataFrame()
    )

    trend_player = ""
    trend_stat = ""
    trend_delta = 0.0
    trend_sentence = "No player trend qualified for the latest snapshot window."
    contribution_sentence = (
        "No direct scoring contribution story qualified for the latest snapshot window."
    )
    context_sentence = "No enriched player context is available for the featured story."
    recommendation_sentence = (
        "No fantasy recommendation qualified for the latest snapshot window."
    )
    ranking_sentence = "No fantasy ranking summary is available yet."
    contribution_payload = {
        "player_id": None,
        "player_name": None,
        "team_abbr": None,
        "opponent_abbr": None,
        "matchup": None,
        "player_pts": None,
        "team_pts": None,
        "opponent_team_pts": None,
        "player_points_share_of_team": None,
        "player_points_share_of_game": None,
        "scoring_margin": None,
        "team_pts_qtr1": None,
        "team_pts_qtr2": None,
        "team_pts_qtr3": None,
        "team_pts_qtr4": None,
        "team_pts_ot_total": None,
        "game_date": None,
    }
    context_payload = {
        "player_id": None,
        "player_name": None,
        "team_abbr": None,
        "team_name": None,
        "position": None,
        "height": None,
        "weight": None,
        "roster_status": None,
        "season_exp": None,
        "draft_year": None,
        "draft_round": None,
        "draft_number": None,
    }

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

    featured_contribution = None
    if not score_contribution.empty:
        contribution_working = score_contribution.copy()
        contribution_working["game_date"] = pd.to_datetime(
            contribution_working["game_date"], errors="coerce"
        )
        for col in ("player_points_share_of_team", "player_points_share_of_game"):
            if col in contribution_working.columns:
                contribution_working[col] = pd.to_numeric(
                    contribution_working[col], errors="coerce"
                )
        if "player_pts" in contribution_working.columns:
            contribution_working["player_pts"] = pd.to_numeric(
                contribution_working["player_pts"], errors="coerce"
            )
        contribution_working = contribution_working.dropna(
            subset=["game_date", "player_points_share_of_team", "player_pts"]
        ).copy()
        if not contribution_working.empty:
            contribution_working = contribution_working.sort_values(
                ["game_date", "player_points_share_of_team", "player_pts", "player_name"],
                ascending=[False, False, False, True],
            )
            featured_contribution = contribution_working.iloc[0]
            contribution_payload = {
                "player_id": int(featured_contribution["player_id"])
                if pd.notna(featured_contribution.get("player_id"))
                else None,
                "player_name": str(featured_contribution.get("player_name") or ""),
                "team_abbr": str(featured_contribution.get("team_abbr") or ""),
                "opponent_abbr": str(featured_contribution.get("opponent_abbr") or ""),
                "matchup": str(featured_contribution.get("matchup") or ""),
                "player_pts": int(featured_contribution["player_pts"])
                if pd.notna(featured_contribution.get("player_pts"))
                else None,
                "team_pts": int(featured_contribution["team_pts"])
                if pd.notna(featured_contribution.get("team_pts"))
                else None,
                "opponent_team_pts": int(featured_contribution["opponent_team_pts"])
                if pd.notna(featured_contribution.get("opponent_team_pts"))
                else None,
                "player_points_share_of_team": round(
                    float(featured_contribution["player_points_share_of_team"]), 4
                ),
                "player_points_share_of_game": round(
                    float(featured_contribution["player_points_share_of_game"]), 4
                )
                if pd.notna(featured_contribution.get("player_points_share_of_game"))
                else None,
                "scoring_margin": int(featured_contribution["scoring_margin"])
                if pd.notna(featured_contribution.get("scoring_margin"))
                else None,
                "team_pts_qtr1": int(featured_contribution["team_pts_qtr1"])
                if pd.notna(featured_contribution.get("team_pts_qtr1"))
                else None,
                "team_pts_qtr2": int(featured_contribution["team_pts_qtr2"])
                if pd.notna(featured_contribution.get("team_pts_qtr2"))
                else None,
                "team_pts_qtr3": int(featured_contribution["team_pts_qtr3"])
                if pd.notna(featured_contribution.get("team_pts_qtr3"))
                else None,
                "team_pts_qtr4": int(featured_contribution["team_pts_qtr4"])
                if pd.notna(featured_contribution.get("team_pts_qtr4"))
                else None,
                "team_pts_ot_total": int(featured_contribution["team_pts_ot_total"])
                if pd.notna(featured_contribution.get("team_pts_ot_total"))
                else None,
                "game_date": featured_contribution["game_date"].date().isoformat(),
            }
            contribution_sentence = (
                f"{contribution_payload['player_name']} supplied "
                f"{contribution_payload['player_pts']} of {contribution_payload['team_pts']} "
                f"{contribution_payload['team_abbr']} points against "
                f"{contribution_payload['opponent_abbr']}, a "
                f"{contribution_payload['player_points_share_of_team']:.1%} share of team scoring. "
                f"Quarter totals landed at {contribution_payload['team_pts_qtr1']}-"
                f"{contribution_payload['team_pts_qtr2']}-"
                f"{contribution_payload['team_pts_qtr3']}-"
                f"{contribution_payload['team_pts_qtr4']}"
                + (
                    f" with {contribution_payload['team_pts_ot_total']} overtime points."
                    if contribution_payload["team_pts_ot_total"]
                    else "."
                )
            )

    if featured_contribution is not None and not player_context.empty:
        context_working = player_context.copy()
        if "player_id" in context_working.columns:
            context_working["player_id"] = pd.to_numeric(
                context_working["player_id"], errors="coerce"
            )
            context_working = context_working[
                context_working["player_id"] == contribution_payload["player_id"]
            ].copy()
        if not context_working.empty:
            featured_context = context_working.iloc[0]
            roster_status = featured_context.get("roster_status")
            if isinstance(roster_status, str):
                roster_status = roster_status.lower() == "true"
            context_payload = {
                "player_id": int(featured_context["player_id"])
                if pd.notna(featured_context.get("player_id"))
                else None,
                "player_name": str(featured_context.get("player_name") or ""),
                "team_abbr": str(featured_context.get("latest_team_abbr") or ""),
                "team_name": str(featured_context.get("team_name") or ""),
                "position": str(featured_context.get("position") or ""),
                "height": str(featured_context.get("height") or ""),
                "weight": int(featured_context["weight"])
                if pd.notna(featured_context.get("weight"))
                else None,
                "roster_status": bool(roster_status)
                if roster_status in (True, False)
                else None,
                "season_exp": int(featured_context["season_exp"])
                if pd.notna(featured_context.get("season_exp"))
                else None,
                "draft_year": str(featured_context.get("draft_year") or ""),
                "draft_round": str(featured_context.get("draft_round") or ""),
                "draft_number": str(featured_context.get("draft_number") or ""),
            }
            roster_label = (
                "active" if context_payload["roster_status"] else "inactive"
                if context_payload["roster_status"] is not None
                else "unknown roster status"
            )
            context_sentence = (
                f"{context_payload['player_name']} is listed as a "
                f"{context_payload['position']} for {context_payload['team_name']} "
                f"({context_payload['team_abbr']}), stands {context_payload['height']}, "
                f"weighs {context_payload['weight']} pounds, and carries {roster_label} "
                f"status with {context_payload['season_exp']} seasons of experience."
            )

    headline = (
        f"{contribution_payload['player_name']} drives the latest {season} scoring snapshot"
        if featured_contribution is not None
        else f"{top_recommendation['player_name']} headlines the {season} fantasy board"
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
        f"and {latest_row['ast_leader']} as the top playmaker. "
        f"{contribution_sentence if featured_contribution is not None else recommendation_sentence}"
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
            contribution_sentence,
            context_sentence,
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
        "contribution_player_id": contribution_payload["player_id"],
        "contribution_player_name": contribution_payload["player_name"],
        "contribution_team_abbr": contribution_payload["team_abbr"],
        "contribution_opponent_abbr": contribution_payload["opponent_abbr"],
        "contribution_matchup": contribution_payload["matchup"],
        "contribution_player_pts": contribution_payload["player_pts"],
        "contribution_team_pts": contribution_payload["team_pts"],
        "contribution_opponent_team_pts": contribution_payload["opponent_team_pts"],
        "contribution_player_points_share_of_team": contribution_payload[
            "player_points_share_of_team"
        ],
        "contribution_player_points_share_of_game": contribution_payload[
            "player_points_share_of_game"
        ],
        "contribution_scoring_margin": contribution_payload["scoring_margin"],
        "contribution_team_pts_qtr1": contribution_payload["team_pts_qtr1"],
        "contribution_team_pts_qtr2": contribution_payload["team_pts_qtr2"],
        "contribution_team_pts_qtr3": contribution_payload["team_pts_qtr3"],
        "contribution_team_pts_qtr4": contribution_payload["team_pts_qtr4"],
        "contribution_team_pts_ot_total": contribution_payload["team_pts_ot_total"],
        "contribution_game_date": contribution_payload["game_date"],
        "context_player_id": context_payload["player_id"],
        "context_player_name": context_payload["player_name"],
        "context_team_abbr": context_payload["team_abbr"],
        "context_team_name": context_payload["team_name"],
        "context_position": context_payload["position"],
        "context_height": context_payload["height"],
        "context_weight": context_payload["weight"],
        "context_roster_status": context_payload["roster_status"],
        "context_season_exp": context_payload["season_exp"],
        "context_draft_year": context_payload["draft_year"],
        "context_draft_round": context_payload["draft_round"],
        "context_draft_number": context_payload["draft_number"],
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
        @contribution_player_id AS contribution_player_id,
        @contribution_player_name AS contribution_player_name,
        @contribution_team_abbr AS contribution_team_abbr,
        @contribution_opponent_abbr AS contribution_opponent_abbr,
        @contribution_matchup AS contribution_matchup,
        @contribution_player_pts AS contribution_player_pts,
        @contribution_team_pts AS contribution_team_pts,
        @contribution_opponent_team_pts AS contribution_opponent_team_pts,
        @contribution_player_points_share_of_team AS contribution_player_points_share_of_team,
        @contribution_player_points_share_of_game AS contribution_player_points_share_of_game,
        @contribution_scoring_margin AS contribution_scoring_margin,
        @contribution_team_pts_qtr1 AS contribution_team_pts_qtr1,
        @contribution_team_pts_qtr2 AS contribution_team_pts_qtr2,
        @contribution_team_pts_qtr3 AS contribution_team_pts_qtr3,
        @contribution_team_pts_qtr4 AS contribution_team_pts_qtr4,
        @contribution_team_pts_ot_total AS contribution_team_pts_ot_total,
        @contribution_game_date AS contribution_game_date,
        @context_player_id AS context_player_id,
        @context_player_name AS context_player_name,
        @context_team_abbr AS context_team_abbr,
        @context_team_name AS context_team_name,
        @context_position AS context_position,
        @context_height AS context_height,
        @context_weight AS context_weight,
        @context_roster_status AS context_roster_status,
        @context_season_exp AS context_season_exp,
        @context_draft_year AS context_draft_year,
        @context_draft_round AS context_draft_round,
        @context_draft_number AS context_draft_number,
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
        contribution_player_id = S.contribution_player_id,
        contribution_player_name = S.contribution_player_name,
        contribution_team_abbr = S.contribution_team_abbr,
        contribution_opponent_abbr = S.contribution_opponent_abbr,
        contribution_matchup = S.contribution_matchup,
        contribution_player_pts = S.contribution_player_pts,
        contribution_team_pts = S.contribution_team_pts,
        contribution_opponent_team_pts = S.contribution_opponent_team_pts,
        contribution_player_points_share_of_team = S.contribution_player_points_share_of_team,
        contribution_player_points_share_of_game = S.contribution_player_points_share_of_game,
        contribution_scoring_margin = S.contribution_scoring_margin,
        contribution_team_pts_qtr1 = S.contribution_team_pts_qtr1,
        contribution_team_pts_qtr2 = S.contribution_team_pts_qtr2,
        contribution_team_pts_qtr3 = S.contribution_team_pts_qtr3,
        contribution_team_pts_qtr4 = S.contribution_team_pts_qtr4,
        contribution_team_pts_ot_total = S.contribution_team_pts_ot_total,
        contribution_game_date = S.contribution_game_date,
        context_player_id = S.context_player_id,
        context_player_name = S.context_player_name,
        context_team_abbr = S.context_team_abbr,
        context_team_name = S.context_team_name,
        context_position = S.context_position,
        context_height = S.context_height,
        context_weight = S.context_weight,
        context_roster_status = S.context_roster_status,
        context_season_exp = S.context_season_exp,
        context_draft_year = S.context_draft_year,
        context_draft_round = S.context_draft_round,
        context_draft_number = S.context_draft_number,
        freshness_ts = S.freshness_ts,
        source_run_id = S.source_run_id
    WHEN NOT MATCHED THEN
      INSERT (
        snapshot_id, snapshot_date, created_at_utc, season, headline, dek, body,
        trend_player, trend_stat, trend_delta, contribution_player_id,
        contribution_player_name, contribution_team_abbr, contribution_opponent_abbr,
        contribution_matchup, contribution_player_pts, contribution_team_pts,
        contribution_opponent_team_pts, contribution_player_points_share_of_team,
        contribution_player_points_share_of_game, contribution_scoring_margin,
        contribution_team_pts_qtr1, contribution_team_pts_qtr2,
        contribution_team_pts_qtr3, contribution_team_pts_qtr4,
        contribution_team_pts_ot_total, contribution_game_date, context_player_id,
        context_player_name, context_team_abbr, context_team_name, context_position,
        context_height, context_weight, context_roster_status, context_season_exp,
        context_draft_year, context_draft_round, context_draft_number,
        freshness_ts, source_run_id
      )
      VALUES (
        S.snapshot_id, S.snapshot_date, S.created_at_utc, S.season, S.headline, S.dek, S.body,
        S.trend_player, S.trend_stat, S.trend_delta, S.contribution_player_id,
        S.contribution_player_name, S.contribution_team_abbr, S.contribution_opponent_abbr,
        S.contribution_matchup, S.contribution_player_pts, S.contribution_team_pts,
        S.contribution_opponent_team_pts, S.contribution_player_points_share_of_team,
        S.contribution_player_points_share_of_game, S.contribution_scoring_margin,
        S.contribution_team_pts_qtr1, S.contribution_team_pts_qtr2,
        S.contribution_team_pts_qtr3, S.contribution_team_pts_qtr4,
        S.contribution_team_pts_ot_total, S.contribution_game_date, S.context_player_id,
        S.context_player_name, S.context_team_abbr, S.context_team_name, S.context_position,
        S.context_height, S.context_weight, S.context_roster_status, S.context_season_exp,
        S.context_draft_year, S.context_draft_round, S.context_draft_number,
        S.freshness_ts, S.source_run_id
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
                "contribution_player_id", "INT64", record["contribution_player_id"]
            ),
            bigquery.ScalarQueryParameter(
                "contribution_player_name",
                "STRING",
                record["contribution_player_name"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_team_abbr", "STRING", record["contribution_team_abbr"]
            ),
            bigquery.ScalarQueryParameter(
                "contribution_opponent_abbr",
                "STRING",
                record["contribution_opponent_abbr"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_matchup", "STRING", record["contribution_matchup"]
            ),
            bigquery.ScalarQueryParameter(
                "contribution_player_pts", "INT64", record["contribution_player_pts"]
            ),
            bigquery.ScalarQueryParameter(
                "contribution_team_pts", "INT64", record["contribution_team_pts"]
            ),
            bigquery.ScalarQueryParameter(
                "contribution_opponent_team_pts",
                "INT64",
                record["contribution_opponent_team_pts"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_player_points_share_of_team",
                "FLOAT64",
                record["contribution_player_points_share_of_team"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_player_points_share_of_game",
                "FLOAT64",
                record["contribution_player_points_share_of_game"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_scoring_margin",
                "INT64",
                record["contribution_scoring_margin"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_team_pts_qtr1",
                "INT64",
                record["contribution_team_pts_qtr1"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_team_pts_qtr2",
                "INT64",
                record["contribution_team_pts_qtr2"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_team_pts_qtr3",
                "INT64",
                record["contribution_team_pts_qtr3"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_team_pts_qtr4",
                "INT64",
                record["contribution_team_pts_qtr4"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_team_pts_ot_total",
                "INT64",
                record["contribution_team_pts_ot_total"],
            ),
            bigquery.ScalarQueryParameter(
                "contribution_game_date", "DATE", record["contribution_game_date"]
            ),
            bigquery.ScalarQueryParameter(
                "context_player_id", "INT64", record["context_player_id"]
            ),
            bigquery.ScalarQueryParameter(
                "context_player_name", "STRING", record["context_player_name"]
            ),
            bigquery.ScalarQueryParameter(
                "context_team_abbr", "STRING", record["context_team_abbr"]
            ),
            bigquery.ScalarQueryParameter(
                "context_team_name", "STRING", record["context_team_name"]
            ),
            bigquery.ScalarQueryParameter(
                "context_position", "STRING", record["context_position"]
            ),
            bigquery.ScalarQueryParameter(
                "context_height", "STRING", record["context_height"]
            ),
            bigquery.ScalarQueryParameter(
                "context_weight", "INT64", record["context_weight"]
            ),
            bigquery.ScalarQueryParameter(
                "context_roster_status", "BOOL", record["context_roster_status"]
            ),
            bigquery.ScalarQueryParameter(
                "context_season_exp", "INT64", record["context_season_exp"]
            ),
            bigquery.ScalarQueryParameter(
                "context_draft_year", "STRING", record["context_draft_year"]
            ),
            bigquery.ScalarQueryParameter(
                "context_draft_round", "STRING", record["context_draft_round"]
            ),
            bigquery.ScalarQueryParameter(
                "context_draft_number", "STRING", record["context_draft_number"]
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
