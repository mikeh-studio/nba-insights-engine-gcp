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

    def get_recommendations(
        self, limit: int = 10, insight_type: str | None = None
    ) -> list[dict[str, Any]]:
        ...

    def get_rankings(self, limit: int = 25) -> list[dict[str, Any]]:
        ...

    def search_players(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        ...

    def get_player_detail(self, player_id: int) -> dict[str, Any] | None:
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

    def get_recommendations(
        self, limit: int = 10, insight_type: str | None = None
    ) -> list[dict[str, Any]]:
        table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.fantasy_insights`"
        filters = ["season = @season"]
        params: list[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
        if insight_type:
            filters.append("insight_type = @insight_type")
            params.append(
                bigquery.ScalarQueryParameter("insight_type", "STRING", insight_type)
            )
        sql = f"""
        SELECT
          insight_id,
          as_of_date,
          player_id,
          player_name,
          insight_type,
          priority_score,
          confidence_score,
          category_focus,
          recommendation,
          title,
          summary,
          evidence_json,
          source_label
        FROM {table}
        WHERE {" AND ".join(filters)}
        ORDER BY as_of_date DESC, priority_score DESC, confidence_score DESC, player_name
        LIMIT @limit
        """
        return self._query(sql, params)

    def get_rankings(self, limit: int = 25) -> list[dict[str, Any]]:
        table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_fantasy_rankings`"
        sql = f"""
        SELECT
          season,
          player_id,
          player_name,
          overall_rank,
          recommendation_score,
          category_strengths,
          category_risks,
          games_next_7d,
          back_to_backs_next_7d
        FROM {table}
        WHERE season = @season
        ORDER BY overall_rank ASC, recommendation_score DESC, player_name
        LIMIT @limit
        """
        return self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ],
        )

    def search_players(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.dim_player`"
        sql = f"""
        SELECT
          player_id,
          player_name,
          latest_season,
          last_seen_at_utc
        FROM {table}
        WHERE latest_season = @season
          AND LOWER(player_name) LIKE CONCAT('%', LOWER(@query), '%')
        ORDER BY player_name
        LIMIT @limit
        """
        return self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("query", "STRING", query.strip()),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ],
        )

    def get_player_detail(self, player_id: int) -> dict[str, Any] | None:
        ranking_table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_fantasy_rankings`"
        form_table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_recent_form`"
        profile_table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_category_profile`"
        outlook_table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_opportunity_outlook`"
        insights_table = f"`{self.settings.project_id}.{self.settings.gold_dataset}.fantasy_insights`"

        ranking_rows = self._query(
            f"""
            SELECT
              season,
              player_id,
              player_name,
              overall_rank,
              recommendation_score,
              category_strengths,
              category_risks,
              games_next_7d,
              back_to_backs_next_7d
            FROM {ranking_table}
            WHERE season = @season
              AND player_id = @player_id
            ORDER BY overall_rank ASC
            LIMIT 1
            """,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            ],
        )
        if not ranking_rows:
            return None

        recent_form_rows = self._query(
            f"""
            SELECT
              avg_min_last_3,
              avg_min_last_5,
              avg_min_last_10,
              fantasy_points_last_3,
              fantasy_points_last_5,
              fantasy_points_last_10,
              pts_last_3,
              pts_last_5,
              pts_last_10,
              reb_last_3,
              reb_last_5,
              reb_last_10,
              ast_last_3,
              ast_last_5,
              ast_last_10,
              stl_last_3,
              stl_last_5,
              stl_last_10,
              blk_last_3,
              blk_last_5,
              blk_last_10,
              fg3m_last_3,
              fg3m_last_5,
              fg3m_last_10
            FROM {form_table}
            WHERE season = @season
              AND player_id = @player_id
            LIMIT 1
            """,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            ],
        )
        recent_form: list[dict[str, Any]] = []
        if recent_form_rows:
            form = recent_form_rows[0]
            recent_form = [
                {
                    "window_label": "Last 3",
                    "avg_pts": form.get("pts_last_3"),
                    "avg_reb": form.get("reb_last_3"),
                    "avg_ast": form.get("ast_last_3"),
                    "avg_stl": form.get("stl_last_3"),
                    "avg_blk": form.get("blk_last_3"),
                    "avg_fg3m": form.get("fg3m_last_3"),
                    "avg_minutes": form.get("avg_min_last_3"),
                    "fantasy_points": form.get("fantasy_points_last_3"),
                },
                {
                    "window_label": "Last 5",
                    "avg_pts": form.get("pts_last_5"),
                    "avg_reb": form.get("reb_last_5"),
                    "avg_ast": form.get("ast_last_5"),
                    "avg_stl": form.get("stl_last_5"),
                    "avg_blk": form.get("blk_last_5"),
                    "avg_fg3m": form.get("fg3m_last_5"),
                    "avg_minutes": form.get("avg_min_last_5"),
                    "fantasy_points": form.get("fantasy_points_last_5"),
                },
                {
                    "window_label": "Last 10",
                    "avg_pts": form.get("pts_last_10"),
                    "avg_reb": form.get("reb_last_10"),
                    "avg_ast": form.get("ast_last_10"),
                    "avg_stl": form.get("stl_last_10"),
                    "avg_blk": form.get("blk_last_10"),
                    "avg_fg3m": form.get("fg3m_last_10"),
                    "avg_minutes": form.get("avg_min_last_10"),
                    "fantasy_points": form.get("fantasy_points_last_10"),
                },
            ]

        category_profile_rows = self._query(
            f"""
            SELECT
              z_pts,
              z_reb,
              z_ast,
              z_stl,
              z_blk,
              z_tov
            FROM {profile_table}
            WHERE season = @season
              AND player_id = @player_id
            LIMIT 1
            """,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            ],
        )
        category_profile: list[dict[str, Any]] = []
        if category_profile_rows:
            profile = category_profile_rows[0]
            for category, field_name in [
                ("PTS", "z_pts"),
                ("REB", "z_reb"),
                ("AST", "z_ast"),
                ("STL", "z_stl"),
                ("BLK", "z_blk"),
                ("TOV", "z_tov"),
            ]:
                raw_value = profile.get(field_name)
                impact = float(raw_value) if raw_value not in (None, "") else 0.0
                if impact >= 0.75:
                    tier = "plus"
                elif impact <= -0.5:
                    tier = "minus"
                else:
                    tier = "neutral"
                direction = "up" if impact > 0 else "down" if impact < 0 else "flat"
                category_profile.append(
                    {
                        "category": category,
                        "impact_score": round(impact, 2),
                        "category_tier": tier,
                        "category_direction": direction,
                    }
                )
            category_profile.sort(
                key=lambda item: abs(float(item["impact_score"])), reverse=True
            )
        opportunity_rows = self._query(
            f"""
            SELECT
              next_7d_games as games_next_7d,
              next_7d_back_to_backs as back_to_backs_next_7d,
              first_game_date,
              next_opponent,
              opportunity_score
            FROM {outlook_table}
            WHERE season = @season
              AND player_id = @player_id
            ORDER BY as_of_date DESC
            LIMIT 1
            """,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            ],
        )
        insights = self._query(
            f"""
            SELECT
              insight_id,
              as_of_date,
              player_id,
              player_name,
              insight_type,
              priority_score,
              confidence_score,
              category_focus,
              recommendation,
              title,
              summary
            FROM {insights_table}
            WHERE season = @season
              AND player_id = @player_id
            ORDER BY as_of_date DESC, priority_score DESC, confidence_score DESC
            LIMIT 6
            """,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            ],
        )

        return {
            "player": ranking_rows[0],
            "recent_form": recent_form,
            "category_profile": category_profile,
            "opportunity": opportunity_rows[0] if opportunity_rows else None,
            "insights": insights,
        }

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
