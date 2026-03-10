from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from google.cloud import bigquery

from app.config import SUPPORTED_SEASON, Settings


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)


def build_freshness_payload(
    latest_run: dict[str, Any] | None,
    *,
    now: datetime,
    freshness_threshold_hours: int,
) -> dict[str, Any]:
    if latest_run is None:
        return {
            "status": "missing",
            "is_fresh": False,
            "checked_at_utc": now.astimezone(UTC).isoformat(),
            "latest_successful_run": None,
            "age_hours": None,
            "threshold_hours": freshness_threshold_hours,
        }

    finished_at = latest_run.get("finished_at_utc")
    if isinstance(finished_at, str):
        finished_at = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    if finished_at is None:
        return {
            "status": "unknown",
            "is_fresh": False,
            "checked_at_utc": now.astimezone(UTC).isoformat(),
            "latest_successful_run": latest_run,
            "age_hours": None,
            "threshold_hours": freshness_threshold_hours,
        }

    if finished_at.tzinfo is None:
        finished_at = finished_at.replace(tzinfo=UTC)
    age = now.astimezone(UTC) - finished_at.astimezone(UTC)
    age_hours = round(age.total_seconds() / 3600, 1)
    is_fresh = age <= timedelta(hours=freshness_threshold_hours)
    return {
        "status": "fresh" if is_fresh else "stale",
        "is_fresh": is_fresh,
        "checked_at_utc": now.astimezone(UTC).isoformat(),
        "latest_successful_run": latest_run,
        "age_hours": age_hours,
        "threshold_hours": freshness_threshold_hours,
    }


class WarehouseRepository(Protocol):
    def get_leaderboard(self, limit: int = 10) -> list[dict[str, Any]]:
        ...

    def get_trends(self, limit: int = 10) -> list[dict[str, Any]]:
        ...

    def get_latest_analysis(self) -> dict[str, Any] | None:
        ...

    def get_latest_successful_run(self) -> dict[str, Any] | None:
        ...

    def get_health(self) -> dict[str, Any]:
        ...


@dataclass
class BigQueryWarehouseRepository:
    settings: Settings
    client: bigquery.Client | None = None

    def __post_init__(self) -> None:
        if self.client is None:
            self.client = bigquery.Client(project=self.settings.project_id or None)

    def _query(
        self, sql: str, params: list[bigquery.ScalarQueryParameter] | None = None
    ) -> list[dict[str, Any]]:
        job_config = None
        if params:
            job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = self.client.query(sql, job_config=job_config).result()
        rows: list[dict[str, Any]] = []
        for row in result:
            rows.append({key: _to_iso(value) for key, value in dict(row).items()})
        return rows

    def get_leaderboard(self, limit: int = 10) -> list[dict[str, Any]]:
        table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.daily_leaderboard`"
        sql = f"""
        SELECT
          season,
          game_date,
          pts_leader,
          pts_matchup,
          pts,
          reb_leader,
          reb,
          ast_leader,
          ast
        FROM {table}
        WHERE season = @season
        ORDER BY game_date DESC, pts DESC, pts_leader
        LIMIT @limit
        """
        return self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ],
        )

    def get_trends(self, limit: int = 10) -> list[dict[str, Any]]:
        table = (
            f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_trends`"
        )
        sql = f"""
        SELECT
          season,
          player_id,
          player_name,
          stat,
          recent_games,
          prior_games,
          recent_avg,
          prior_avg,
          delta,
          pct_change
        FROM {table}
        WHERE season = @season
        ORDER BY ABS(delta) DESC, player_name, stat
        LIMIT @limit
        """
        return self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ],
        )

    def get_latest_analysis(self) -> dict[str, Any] | None:
        table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.analysis_snapshots`"
        sql = f"""
        SELECT
          snapshot_id,
          snapshot_date,
          created_at_utc,
          season,
          headline,
          dek,
          body,
          trend_player,
          trend_stat,
          trend_delta,
          freshness_ts,
          source_run_id
        FROM {table}
        WHERE season = @season
        ORDER BY snapshot_date DESC, created_at_utc DESC, snapshot_id DESC
        LIMIT 1
        """
        rows = self._query(
            sql, [bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON)]
        )
        return rows[0] if rows else None

    def get_latest_successful_run(self) -> dict[str, Any] | None:
        table = f"`{self.settings.project_id}.{self.settings.metadata_dataset}.pipeline_run_log`"
        sql = f"""
        SELECT
          dag_run_id,
          season,
          status,
          gcs_uri,
          rows_extracted,
          rows_loaded,
          rows_inserted,
          rows_updated,
          watermark_before,
          watermark_after,
          started_at_utc,
          finished_at_utc,
          details
        FROM {table}
        WHERE season = @season
          AND status = 'success'
        ORDER BY finished_at_utc DESC
        LIMIT 1
        """
        rows = self._query(
            sql, [bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON)]
        )
        return rows[0] if rows else None

    def get_health(self) -> dict[str, Any]:
        latest_run = self.get_latest_successful_run()
        payload = build_freshness_payload(
            latest_run,
            now=datetime.now(tz=UTC),
            freshness_threshold_hours=self.settings.freshness_threshold_hours,
        )
        payload["season"] = SUPPORTED_SEASON
        return payload
