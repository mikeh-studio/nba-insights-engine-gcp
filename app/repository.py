from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from math import sqrt
from typing import Any, Literal, Protocol

from google.api_core.exceptions import GoogleAPIError as BQAPIError
from google.cloud import bigquery

from app.config import SUPPORTED_SEASON, Settings

STATE_FRESH = "fresh"
STATE_STALE = "stale"
STATE_MISSING = "missing"
STATE_INSUFFICIENT_SAMPLE = "insufficient_sample"
STATE_UNAVAILABLE = "unavailable"

CompareWindow = Literal["last_3", "last_5", "last_7", "prior_5", "last_10"]
CompareFocus = Literal["balanced", "scoring", "playmaking", "defense"]

COMPARE_WINDOW_CONFIG: dict[CompareWindow, dict[str, Any]] = {
    "last_3": {"label": "Last 3", "expected_games": 3},
    "last_5": {"label": "Last 5", "expected_games": 5},
    "last_7": {"label": "Last 7", "expected_games": 7},
    "prior_5": {"label": "Prior 5", "expected_games": 5},
    "last_10": {"label": "Last 10", "expected_games": 10},
}

COMPARE_METRIC_LABELS: dict[str, str] = {
    "fantasy_proxy_score": "Box Score Index",
    "avg_min": "Minutes",
    "avg_pts": "PTS",
    "avg_reb": "REB",
    "avg_ast": "AST",
    "avg_stl": "STL",
    "avg_blk": "BLK",
    "avg_fg3m": "3PM",
    "avg_tov": "TOV",
}

COMPARE_FOCUS_CONFIG: dict[CompareFocus, dict[str, Any]] = {
    "balanced": {
        "label": "Balanced",
        "description": "Keeps the full stat mix in the default read order.",
        "metric_keys": [
            "fantasy_proxy_score",
            "avg_min",
            "avg_pts",
            "avg_reb",
            "avg_ast",
            "avg_stl",
            "avg_blk",
            "avg_fg3m",
            "avg_tov",
        ],
    },
    "scoring": {
        "label": "Scoring",
        "description": "Pushes scoring volume and shot-making to the top of the comparison.",
        "metric_keys": [
            "fantasy_proxy_score",
            "avg_pts",
            "avg_fg3m",
            "avg_min",
            "avg_ast",
            "avg_reb",
            "avg_stl",
            "avg_blk",
            "avg_tov",
        ],
    },
    "playmaking": {
        "label": "Playmaking",
        "description": "Prioritizes creation load first, then supporting scoring context.",
        "metric_keys": [
            "avg_ast",
            "avg_min",
            "avg_tov",
            "fantasy_proxy_score",
            "avg_pts",
            "avg_reb",
            "avg_stl",
            "avg_blk",
            "avg_fg3m",
        ],
    },
    "defense": {
        "label": "Defense",
        "description": "Highlights defensive events and rebounding before offense-first stats.",
        "metric_keys": [
            "avg_stl",
            "avg_blk",
            "avg_reb",
            "fantasy_proxy_score",
            "avg_min",
            "avg_pts",
            "avg_ast",
            "avg_fg3m",
            "avg_tov",
        ],
    },
}

SIMILARITY_RESULT_LIMIT = 6
SIMILARITY_FEATURE_COLUMNS = [
    "season_avg_pts",
    "season_avg_reb",
    "season_avg_ast",
    "season_avg_stl",
    "season_avg_blk",
    "season_avg_fg3m",
    "season_avg_tov",
    "season_avg_min",
    "recent_pts",
    "recent_reb",
    "recent_ast",
    "recent_stl",
    "recent_blk",
    "recent_fg3m",
    "recent_tov",
    "recent_min",
    "recent_points_share_of_team",
    "recent_points_share_of_game",
    "minutes_delta_vs_season",
]

SIMILARITY_TRAIT_LABELS: dict[str, str] = {
    "season_avg_pts": "scoring volume",
    "season_avg_reb": "rebounding",
    "season_avg_ast": "playmaking",
    "season_avg_stl": "steals pressure",
    "season_avg_blk": "rim protection",
    "season_avg_fg3m": "three-point volume",
    "season_avg_min": "minutes load",
    "recent_points_share_of_team": "usage share",
    "recent_points_share_of_game": "game scoring share",
    "minutes_delta_vs_season": "minutes trend",
}


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _parse_iso_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _stringify_reason_value(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _reason_label(code: str | None) -> str | None:
    labels = {
        "recent_fp_delta": "recent box score production",
        "minutes_delta": "minutes trend",
        "games_next_7d": "next 7 days",
        "back_to_back_count": "back-to-backs",
        "trend_stat_delta": "recent trend",
        "category_edge": "category edge",
    }
    if code is None:
        return None
    return labels.get(code, code.replace("_", " "))


def get_compare_window_options() -> list[dict[str, Any]]:
    return [
        {
            "key": key,
            "label": str(config["label"]),
            "expected_games": int(config["expected_games"]),
        }
        for key, config in COMPARE_WINDOW_CONFIG.items()
    ]


def get_compare_focus_options() -> list[dict[str, str]]:
    return [
        {
            "key": key,
            "label": str(config["label"]),
            "description": str(config["description"]),
        }
        for key, config in COMPARE_FOCUS_CONFIG.items()
    ]


def _compare_window_label(window: CompareWindow) -> str:
    return str(COMPARE_WINDOW_CONFIG[window]["label"])


def _compare_window_expected_games(window: CompareWindow) -> int:
    return int(COMPARE_WINDOW_CONFIG[window]["expected_games"])


def _empty_compare_metrics() -> dict[str, Any]:
    return {key: None for key in COMPARE_METRIC_LABELS}


def _build_compare_metric_rows(
    metrics: dict[str, Any], focus: CompareFocus
) -> list[dict[str, Any]]:
    ordered_keys = list(COMPARE_FOCUS_CONFIG[focus]["metric_keys"])
    return [
        {
            "key": key,
            "label": COMPARE_METRIC_LABELS[key],
            "value": metrics.get(key),
            "is_focus": index < 3,
        }
        for index, key in enumerate(ordered_keys)
    ]


def build_reason_summary(row: dict[str, Any]) -> str | None:
    parts: list[str] = []
    for code_key, value_key in (
        ("reason_primary_code", "reason_primary_value"),
        ("reason_secondary_code", "reason_secondary_value"),
        ("reason_context_code", "reason_context_value"),
    ):
        label = _reason_label(row.get(code_key))
        value = _stringify_reason_value(row.get(value_key))
        if label and value:
            parts.append(f"{label}: {value}")
        elif label:
            parts.append(label)
    return " | ".join(parts) if parts else None


def build_headshot_url(player_id: Any) -> str | None:
    normalized_player_id = _to_int(player_id)
    if normalized_player_id is None:
        return None
    return (
        "https://cdn.nba.com/headshots/nba/latest/1040x760/"
        f"{normalized_player_id}.png"
    )


def build_player_initials(player_name: Any) -> str:
    if not isinstance(player_name, str) or not player_name.strip():
        return "NBA"
    parts = [part[0].upper() for part in player_name.split() if part]
    if not parts:
        return "NBA"
    return "".join(parts[:2])


def _format_home_date_label(value: str) -> str:
    parsed = _parse_iso_date(value)
    if parsed is None:
        return value
    return parsed.strftime("%a %b %d")


def _build_top_improvement_chips(row: dict[str, Any]) -> list[dict[str, Any]]:
    deltas: list[tuple[str, float]] = []
    for label, key in (
        ("PTS", "pts_delta"),
        ("REB", "reb_delta"),
        ("AST", "ast_delta"),
        ("STL", "stl_delta"),
        ("BLK", "blk_delta"),
        ("3PM", "fg3m_delta"),
        ("MIN", "min_delta"),
    ):
        value = _to_float(row.get(key))
        if value is None or value <= 0:
            continue
        deltas.append((label, round(value, 1)))
    deltas.sort(key=lambda item: item[1], reverse=True)
    return [{"label": label, "delta": value} for label, value in deltas[:3]]


def _sanitize_category_list(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    filtered = [item for item in items if item != "TOV"]
    if not filtered:
        return None
    return ", ".join(filtered)


def _split_display_list(value: Any) -> list[str]:
    if not isinstance(value, str) or not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _similarity_state_from_sample_status(sample_status: Any) -> str:
    if sample_status == "insufficient_sample":
        return STATE_INSUFFICIENT_SAMPLE
    if sample_status in ("ready", "limited_sample"):
        return STATE_FRESH
    return STATE_UNAVAILABLE


def _similarity_feature_value(row: dict[str, Any], feature_name: str) -> float | None:
    return _to_float(row.get(f"norm_{feature_name}"))


def _shared_similarity_traits(
    anchor_row: dict[str, Any], candidate_row: dict[str, Any], *, limit: int = 3
) -> list[str]:
    ranked: list[tuple[str, float, float]] = []
    for feature_name, label in SIMILARITY_TRAIT_LABELS.items():
        anchor_value = _similarity_feature_value(anchor_row, feature_name)
        candidate_value = _similarity_feature_value(candidate_row, feature_name)
        if anchor_value is None or candidate_value is None:
            continue
        if anchor_value <= 0 or candidate_value <= 0:
            continue
        diff = abs(anchor_value - candidate_value)
        strength = ((anchor_value + candidate_value) / 2.0) - diff
        if strength <= 0:
            continue
        ranked.append((label, strength, diff))

    ranked.sort(key=lambda item: (item[1], -item[2]), reverse=True)
    return [label for label, _, _ in ranked[:limit]]


def _contrasting_similarity_traits(
    anchor_row: dict[str, Any], candidate_row: dict[str, Any], *, limit: int = 2
) -> list[str]:
    ranked: list[tuple[str, float]] = []
    for feature_name, label in SIMILARITY_TRAIT_LABELS.items():
        anchor_value = _similarity_feature_value(anchor_row, feature_name)
        candidate_value = _similarity_feature_value(candidate_row, feature_name)
        if anchor_value is None or candidate_value is None:
            continue
        difference = abs(anchor_value - candidate_value)
        if difference < 0.75:
            continue
        ranked.append((label, difference))

    ranked.sort(key=lambda item: item[1], reverse=True)
    return [label for label, _ in ranked[:limit]]


def _trend_direction(status: Any) -> str:
    if status == "rising":
        return "up"
    if status == "falling":
        return "down"
    return "flat"


def build_freshness_payload(
    latest_run: dict[str, Any] | None,
    *,
    now: datetime,
    freshness_threshold_hours: int,
) -> dict[str, Any]:
    checked_at = now.astimezone(UTC).isoformat()
    if latest_run is None:
        return {
            "status": STATE_MISSING,
            "is_fresh": False,
            "checked_at_utc": checked_at,
            "age_hours": None,
            "threshold_hours": freshness_threshold_hours,
            "last_successful_finished_at_utc": None,
        }

    finished_at = _parse_iso_datetime(latest_run.get("finished_at_utc"))
    if finished_at is None:
        return {
            "status": STATE_UNAVAILABLE,
            "is_fresh": False,
            "checked_at_utc": checked_at,
            "age_hours": None,
            "threshold_hours": freshness_threshold_hours,
            "last_successful_finished_at_utc": None,
        }

    age = now.astimezone(UTC) - finished_at
    age_hours = round(age.total_seconds() / 3600, 1)
    is_fresh = age <= timedelta(hours=freshness_threshold_hours)
    return {
        "status": STATE_FRESH if is_fresh else STATE_STALE,
        "is_fresh": is_fresh,
        "checked_at_utc": checked_at,
        "age_hours": age_hours,
        "threshold_hours": freshness_threshold_hours,
        "last_successful_finished_at_utc": finished_at.isoformat(),
    }


def _opportunity_state_from_row(row: dict[str, Any]) -> str:
    if row.get("games_next_7d") in (None, "") and row.get("opportunity_score") in (
        None,
        "",
    ):
        return STATE_UNAVAILABLE
    return STATE_FRESH


def _format_category_profile(row: dict[str, Any]) -> list[dict[str, Any]]:
    categories: list[dict[str, Any]] = []
    for category, field_name in (
        ("PTS", "z_pts"),
        ("REB", "z_reb"),
        ("AST", "z_ast"),
        ("STL", "z_stl"),
        ("BLK", "z_blk"),
        ("3PM", "z_fg3m"),
    ):
        impact = _to_float(row.get(field_name))
        if impact is None:
            continue
        if impact >= 0.75:
            tier = "plus"
        elif impact <= -0.5:
            tier = "minus"
        else:
            tier = "neutral"
        direction = "up" if impact > 0 else "down" if impact < 0 else "flat"
        categories.append(
            {
                "category": category,
                "impact_score": round(impact, 2),
                "category_tier": tier,
                "category_direction": direction,
            }
        )
    categories.sort(key=lambda item: abs(float(item["impact_score"])), reverse=True)
    return categories


def _window_state(games_in_window: int | None, expected_games: int | None) -> str:
    if games_in_window is None:
        return STATE_UNAVAILABLE
    if expected_games is not None and games_in_window < expected_games:
        return STATE_INSUFFICIENT_SAMPLE
    return STATE_FRESH


def _window_reason(state: str, window_label: str) -> str | None:
    if state == STATE_INSUFFICIENT_SAMPLE:
        return f"Limited comparison data for {window_label}"
    if state == STATE_UNAVAILABLE:
        return f"No {window_label} data is available yet"
    return None


def _default_recent_form() -> list[dict[str, Any]]:
    windows = (
        ("last_5", "Last 5", 5),
        ("prior_5", "Prior 5", 5),
        ("last_10", "Last 10", 10),
    )
    items: list[dict[str, Any]] = []
    for key, label, expected_games in windows:
        items.append(
            {
                "window_key": key,
                "window_label": label,
                "games_in_window": None,
                "window_games_expected": expected_games,
                "state": STATE_UNAVAILABLE,
                "state_reason": _window_reason(STATE_UNAVAILABLE, label),
                "avg_pts": None,
                "avg_reb": None,
                "avg_ast": None,
                "avg_stl": None,
                "avg_blk": None,
                "avg_fg3m": None,
                "avg_tov": None,
                "avg_minutes": None,
                "fantasy_proxy": None,
            }
        )
    return items


class WarehouseRepository(Protocol):
    def get_dashboard(self, as_of_date: str | None = None) -> dict[str, Any]:
        ...

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

    def get_compare(
        self,
        player_a_id: int,
        player_b_id: int,
        *,
        window: CompareWindow = "last_5",
        focus: CompareFocus = "balanced",
    ) -> dict[str, Any]:
        ...

    def get_latest_analysis(self) -> dict[str, Any] | None:
        ...

    def get_latest_successful_run(self) -> dict[str, Any] | None:
        ...

    def get_player_game_log(
        self, player_id: int, limit: int = 30
    ) -> dict[str, Any] | None:
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

    def _dashboard_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.workbench_dashboard`"

    def _home_dashboard_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.workbench_home_dashboard`"

    def _detail_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.workbench_player_detail`"

    def _compare_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.workbench_compare`"

    def _similarity_feature_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_similarity_features`"

    def _archetype_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.player_archetypes`"

    def _dim_player_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.dim_player`"

    def _fct_game_stats_table(self) -> str:
        return f"`{self.settings.project_id}.{self.settings.gold_dataset}.fct_player_game_stats`"

    def _decorate_dashboard_row(self, row: dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        item["category_strengths"] = _sanitize_category_list(
            row.get("category_strengths")
        )
        item["category_risks"] = _sanitize_category_list(row.get("category_risks"))
        item["reason_summary"] = build_reason_summary(row)
        item["opportunity_state"] = _opportunity_state_from_row(row)
        item["headshot_url"] = build_headshot_url(row.get("player_id"))
        item["player_initials"] = build_player_initials(row.get("player_name"))
        item["top_improvements"] = _build_top_improvement_chips(row)
        item["trend_direction"] = _trend_direction(row.get("trend_status"))
        return item

    def _fetch_dashboard_rows(
        self,
        *,
        limit: int,
        order_by: str,
        where_clause: str = "",
        extra_params: list[bigquery.ScalarQueryParameter] | None = None,
    ) -> list[dict[str, Any]]:
        params: list[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
        if extra_params:
            params.extend(extra_params)
        sql = f"""
        SELECT
          season,
          as_of_date,
          player_id,
          player_name,
          latest_team_abbr,
          latest_game_date,
          overall_rank,
          recommendation_score,
          recommendation_tier,
          category_strengths,
          category_risks,
          last_5_games,
          prior_5_games,
          last_10_games,
          fantasy_proxy_last_5,
          fantasy_proxy_prior_5,
          fantasy_proxy_last_10,
          trend_delta,
          trend_pct_change,
          trend_status,
          next_game_date,
          next_opponent_abbr,
          games_next_7d,
          back_to_backs_next_7d,
          opportunity_score,
          reason_primary_code,
          reason_primary_value,
          reason_secondary_code,
          reason_secondary_value,
          reason_context_code,
          reason_context_value
        FROM {self._dashboard_table()}
        WHERE season = @season
          {where_clause}
        ORDER BY {order_by}
        LIMIT @limit
        """
        return [self._decorate_dashboard_row(row) for row in self._query(sql, params)]

    def _fetch_home_date_options(self) -> list[str]:
        sql = f"""
        SELECT DISTINCT as_of_date
        FROM {self._home_dashboard_table()}
        WHERE season = @season
        ORDER BY as_of_date DESC
        LIMIT 7
        """
        try:
            return [
                str(row["as_of_date"])
                for row in self._query(
                    sql,
                    [
                        bigquery.ScalarQueryParameter(
                            "season", "STRING", SUPPORTED_SEASON
                        )
                    ],
                )
            ]
        except BQAPIError:
            return []

    def _resolve_home_as_of_date(
        self, requested: str | None
    ) -> tuple[str | None, list[str]]:
        options = self._fetch_home_date_options()
        if not options:
            return None, []
        if requested and requested in options:
            return requested, options
        parsed_requested = _parse_iso_date(requested)
        if parsed_requested is not None:
            normalized = parsed_requested.isoformat()
            if normalized in options:
                return normalized, options
        return options[0], options

    def _fetch_home_dashboard_rows(
        self,
        *,
        as_of_date: str,
        limit: int,
        order_by: str,
        where_clause: str = "",
    ) -> list[dict[str, Any]]:
        parsed_as_of_date = _parse_iso_date(as_of_date)
        params: list[bigquery.ScalarQueryParameter] = [
            bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
            bigquery.ScalarQueryParameter("as_of_date", "DATE", parsed_as_of_date),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
        sql = f"""
        SELECT
          season,
          as_of_date,
          player_id,
          player_name,
          latest_team_abbr,
          latest_game_date,
          overall_rank,
          recommendation_score,
          recommendation_tier,
          category_strengths,
          category_risks,
          last_5_games,
          prior_5_games,
          last_10_games,
          fantasy_proxy_last_5,
          fantasy_proxy_prior_5,
          fantasy_proxy_last_10,
          trend_delta,
          trend_pct_change,
          trend_status,
          next_game_date,
          next_opponent_abbr,
          games_next_7d,
          back_to_backs_next_7d,
          opportunity_score,
          pts_delta,
          reb_delta,
          ast_delta,
          stl_delta,
          blk_delta,
          fg3m_delta,
          min_delta,
          reason_primary_code,
          reason_primary_value,
          reason_secondary_code,
          reason_secondary_value,
          reason_context_code,
          reason_context_value
        FROM {self._home_dashboard_table()}
        WHERE season = @season
          AND as_of_date = @as_of_date
          {where_clause}
        ORDER BY {order_by}
        LIMIT @limit
        """
        return [self._decorate_dashboard_row(row) for row in self._query(sql, params)]

    def _fetch_player_identity(self, player_id: int) -> dict[str, Any] | None:
        sql = f"""
        SELECT
          player_id,
          player_name,
          latest_season,
          last_seen_at_utc
        FROM {self._dim_player_table()}
        WHERE latest_season = @season
          AND player_id = @player_id
        LIMIT 1
        """
        rows = self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            ],
        )
        return rows[0] if rows else None

    def _similarity_distance_sql(self, anchor_alias: str, candidate_alias: str) -> str:
        terms = [
            (
                f"POW(COALESCE({candidate_alias}.norm_{feature_name}, 0) "
                f"- COALESCE({anchor_alias}.norm_{feature_name}, 0), 2)"
            )
            for feature_name in SIMILARITY_FEATURE_COLUMNS
        ]
        return " + ".join(terms)

    def _fetch_similarity_anchor(self, player_id: int) -> dict[str, Any] | None:
        normalized_fields = ",\n          ".join(
            [f"norm_{feature_name}" for feature_name in SIMILARITY_FEATURE_COLUMNS]
        )
        sql = f"""
        SELECT
          season,
          as_of_date,
          player_id,
          player_name,
          team_abbr,
          position,
          games_sampled,
          sample_status,
          archetype_id,
          archetype_label,
          cluster_confidence,
          top_traits,
          contrasting_traits,
          archetype_summary,
          {normalized_fields}
        FROM {self._similarity_feature_table()}
        WHERE season = @season
          AND player_id = @player_id
        LIMIT 1
        """
        try:
            rows = self._query(
                sql,
                [
                    bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                    bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
                ],
            )
        except BQAPIError:
            return None
        return rows[0] if rows else None

    def _get_similar_players(
        self,
        player_id: int,
        *,
        anchor: dict[str, Any] | None = None,
        limit: int = SIMILARITY_RESULT_LIMIT,
    ) -> tuple[str, str | None, list[dict[str, Any]]]:
        if anchor is None:
            anchor = self._fetch_similarity_anchor(player_id)
        if anchor is None:
            return STATE_UNAVAILABLE, "Similarity profile is unavailable.", []

        anchor_state = _similarity_state_from_sample_status(anchor.get("sample_status"))
        if anchor_state == STATE_INSUFFICIENT_SAMPLE:
            return (
                anchor_state,
                "Not enough games are available to generate similar players yet.",
                [],
            )

        normalized_fields = ",\n          ".join(
            [
                f"candidate.norm_{feature_name}"
                for feature_name in SIMILARITY_FEATURE_COLUMNS
            ]
        )
        distance_sql = self._similarity_distance_sql("anchor", "candidate")
        sql = f"""
        WITH anchor AS (
          SELECT *
          FROM {self._similarity_feature_table()}
          WHERE season = @season
            AND player_id = @player_id
          LIMIT 1
        ),
        scored AS (
          SELECT
            candidate.player_id,
            candidate.player_name,
            candidate.team_abbr,
            candidate.archetype_label,
            candidate.cluster_confidence,
            candidate.top_traits,
            candidate.contrasting_traits,
            candidate.sample_status,
            {normalized_fields},
            SQRT({distance_sql}) AS euclidean_distance
          FROM {self._similarity_feature_table()} candidate
          CROSS JOIN anchor
          WHERE candidate.season = @season
            AND candidate.player_id != anchor.player_id
            AND candidate.sample_status IN ('ready', 'limited_sample')
        )
        SELECT
          *,
          ROUND(1 / (1 + euclidean_distance), 4) AS similarity_score
        FROM scored
        ORDER BY euclidean_distance ASC, player_name
        LIMIT @limit
        """
        try:
            rows = self._query(
                sql,
                [
                    bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                    bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
                    bigquery.ScalarQueryParameter("limit", "INT64", limit),
                ],
            )
        except BQAPIError:
            return STATE_UNAVAILABLE, "Similarity profile is unavailable.", []

        items: list[dict[str, Any]] = []
        for row in rows:
            shared_traits = _shared_similarity_traits(anchor, row)
            if not shared_traits:
                shared_traits = [
                    trait
                    for trait in _split_display_list(anchor.get("top_traits"))
                    if trait in _split_display_list(row.get("top_traits"))
                ][:3]
            items.append(
                {
                    "player_id": _to_int(row.get("player_id")),
                    "player_name": row.get("player_name"),
                    "team_abbr": row.get("team_abbr"),
                    "headshot_url": build_headshot_url(row.get("player_id")),
                    "player_initials": build_player_initials(row.get("player_name")),
                    "similarity_score": _to_float(row.get("similarity_score")),
                    "archetype_label": row.get("archetype_label"),
                    "shared_traits": shared_traits,
                    "contrasting_traits": _contrasting_similarity_traits(anchor, row),
                }
            )

        if not items:
            return STATE_UNAVAILABLE, "No similar-player matches are available.", []
        return STATE_FRESH, None, items

    def _get_pair_similarity(
        self, player_a_id: int, player_b_id: int
    ) -> dict[str, Any]:
        player_a = self._fetch_similarity_anchor(player_a_id)
        player_b = self._fetch_similarity_anchor(player_b_id)
        if player_a is None or player_b is None:
            return {
                "state": STATE_UNAVAILABLE,
                "score": None,
                "summary": "Similarity profile is unavailable for at least one player.",
                "same_archetype": False,
                "archetype_labels": [],
                "shared_traits": [],
                "contrasting_traits": [],
            }

        state_a = _similarity_state_from_sample_status(player_a.get("sample_status"))
        state_b = _similarity_state_from_sample_status(player_b.get("sample_status"))
        if STATE_INSUFFICIENT_SAMPLE in (state_a, state_b):
            return {
                "state": STATE_INSUFFICIENT_SAMPLE,
                "score": None,
                "summary": "One player does not have enough games for a stable similarity read yet.",
                "same_archetype": False,
                "archetype_labels": [],
                "shared_traits": [],
                "contrasting_traits": [],
            }

        squared_distance = 0.0
        for feature_name in SIMILARITY_FEATURE_COLUMNS:
            player_a_value = _similarity_feature_value(player_a, feature_name) or 0.0
            player_b_value = _similarity_feature_value(player_b, feature_name) or 0.0
            squared_distance += (player_b_value - player_a_value) ** 2
        score = round(1 / (1 + sqrt(squared_distance)), 4)
        if score is None:
            return {
                "state": STATE_UNAVAILABLE,
                "score": None,
                "summary": "Similarity profile is unavailable for at least one player.",
                "same_archetype": False,
                "archetype_labels": [],
                "shared_traits": [],
                "contrasting_traits": [],
            }
        same_archetype = player_a.get("archetype_label") is not None and player_a.get(
            "archetype_label"
        ) == player_b.get("archetype_label")
        if same_archetype:
            summary = (
                f"Shared archetype: {player_a.get('archetype_label')}. "
                f"Current stat-profile similarity is {score}."
            )
        else:
            summary = (
                f"Archetypes diverge: {player_a.get('archetype_label')} vs "
                f"{player_b.get('archetype_label')}. Current stat-profile similarity is {score}."
            )
        return {
            "state": STATE_FRESH,
            "score": score,
            "summary": summary,
            "same_archetype": same_archetype,
            "archetype_labels": [
                player_a.get("archetype_label"),
                player_b.get("archetype_label"),
            ],
            "shared_traits": _shared_similarity_traits(player_a, player_b),
            "contrasting_traits": _contrasting_similarity_traits(player_a, player_b),
        }

    def _build_player_detail_payload(
        self,
        *,
        identity: dict[str, Any],
        row: dict[str, Any] | None,
        archetype_row: dict[str, Any] | None,
        similarity_state: str,
        similarity_reason: str | None,
        similar_players: list[dict[str, Any]],
    ) -> dict[str, Any]:
        archetype_state = STATE_UNAVAILABLE
        archetype_payload = {
            "state": STATE_UNAVAILABLE,
            "archetype_id": None,
            "archetype_label": None,
            "cluster_confidence": None,
            "top_traits": [],
            "summary": None,
        }
        if archetype_row is not None:
            archetype_state = _similarity_state_from_sample_status(
                archetype_row.get("sample_status")
            )
            archetype_payload = {
                "state": archetype_state,
                "archetype_id": archetype_row.get("archetype_id"),
                "archetype_label": archetype_row.get("archetype_label"),
                "cluster_confidence": _to_float(
                    archetype_row.get("cluster_confidence")
                ),
                "top_traits": _split_display_list(archetype_row.get("top_traits")),
                "summary": archetype_row.get("archetype_summary"),
            }

        if row is None:
            return {
                "player": {
                    "season": identity.get("latest_season", SUPPORTED_SEASON),
                    "player_id": identity.get("player_id"),
                    "player_name": identity.get("player_name"),
                    "headshot_url": build_headshot_url(identity.get("player_id")),
                    "player_initials": build_player_initials(
                        identity.get("player_name")
                    ),
                    "team_abbr": None,
                    "latest_game_date": None,
                    "overall_rank": None,
                    "recommendation_score": None,
                    "recommendation_tier": None,
                    "category_strengths": None,
                    "category_risks": None,
                    "is_ranked": False,
                },
                "availability_state": STATE_UNAVAILABLE,
                "availability_reason": "Not currently ranked",
                "reason_summary": None,
                "trend": {
                    "status": STATE_UNAVAILABLE,
                    "delta": None,
                    "pct_change": None,
                },
                "panel_states": {
                    "recent_form": STATE_UNAVAILABLE,
                    "category_profile": STATE_UNAVAILABLE,
                    "opportunity": STATE_UNAVAILABLE,
                    "archetype": archetype_state,
                    "similarity": similarity_state,
                },
                "recent_form": _default_recent_form(),
                "category_profile": [],
                "opportunity": None,
                "archetype": archetype_payload,
                "similarity_reason": similarity_reason,
                "similar_players": similar_players,
            }

        recent_form = []
        for key, label, expected_games in (
            ("last_5", "Last 5", 5),
            ("prior_5", "Prior 5", 5),
            ("last_10", "Last 10", 10),
        ):
            games_in_window = _to_int(row.get(f"{key}_games"))
            state = _window_state(games_in_window, expected_games)
            recent_form.append(
                {
                    "window_key": key,
                    "window_label": label,
                    "games_in_window": games_in_window,
                    "window_games_expected": expected_games,
                    "state": state,
                    "state_reason": _window_reason(state, label),
                    "avg_pts": row.get(f"{key}_avg_pts"),
                    "avg_reb": row.get(f"{key}_avg_reb"),
                    "avg_ast": row.get(f"{key}_avg_ast"),
                    "avg_stl": row.get(f"{key}_avg_stl"),
                    "avg_blk": row.get(f"{key}_avg_blk"),
                    "avg_fg3m": row.get(f"{key}_avg_fg3m"),
                    "avg_tov": row.get(f"{key}_avg_tov"),
                    "avg_minutes": row.get(f"{key}_avg_min"),
                    "fantasy_proxy": row.get(f"{key}_fantasy_proxy"),
                }
            )

        category_profile = _format_category_profile(row)
        opportunity_state = _opportunity_state_from_row(row)
        recent_form_state = (
            STATE_FRESH
            if any(item["state"] == STATE_FRESH for item in recent_form)
            else STATE_INSUFFICIENT_SAMPLE
            if any(item["state"] == STATE_INSUFFICIENT_SAMPLE for item in recent_form)
            else STATE_UNAVAILABLE
        )
        category_profile_state = STATE_FRESH if category_profile else STATE_UNAVAILABLE
        opportunity = None
        if opportunity_state != STATE_UNAVAILABLE:
            opportunity = {
                "games_next_7d": row.get("games_next_7d"),
                "back_to_backs_next_7d": row.get("back_to_backs_next_7d"),
                "next_opponent": row.get("next_opponent_abbr"),
                "next_game_date": row.get("next_game_date"),
                "opportunity_score": row.get("opportunity_score"),
            }

        return {
            "player": {
                "season": row.get("season"),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "headshot_url": build_headshot_url(row.get("player_id")),
                "player_initials": build_player_initials(row.get("player_name")),
                "team_abbr": row.get("latest_team_abbr"),
                "latest_game_date": row.get("latest_game_date"),
                "overall_rank": row.get("overall_rank"),
                "recommendation_score": row.get("recommendation_score"),
                "recommendation_tier": row.get("recommendation_tier"),
                "category_strengths": _sanitize_category_list(
                    row.get("category_strengths")
                ),
                "category_risks": _sanitize_category_list(row.get("category_risks")),
                "is_ranked": row.get("overall_rank") is not None,
            },
            "availability_state": (
                STATE_FRESH
                if row.get("overall_rank") is not None
                else STATE_UNAVAILABLE
            ),
            "availability_reason": (
                None if row.get("overall_rank") is not None else "Not currently ranked"
            ),
            "reason_summary": build_reason_summary(row),
            "trend": {
                "status": row.get("trend_status"),
                "delta": row.get("trend_delta"),
                "pct_change": row.get("trend_pct_change"),
            },
            "panel_states": {
                "recent_form": recent_form_state,
                "category_profile": category_profile_state,
                "opportunity": opportunity_state,
                "archetype": archetype_state,
                "similarity": similarity_state,
            },
            "recent_form": recent_form,
            "category_profile": category_profile,
            "opportunity": opportunity,
            "archetype": archetype_payload,
            "similarity_reason": similarity_reason,
            "similar_players": similar_players,
        }

    def get_dashboard(self, as_of_date: str | None = None) -> dict[str, Any]:
        selected_as_of_date, date_options = self._resolve_home_as_of_date(as_of_date)
        if selected_as_of_date is None:
            return {
                "selected_as_of_date": None,
                "date_options": [],
                "signals": [],
                "rankings": [],
                "trends": [],
                "opportunity": [],
            }

        rows = self._fetch_home_dashboard_rows(
            as_of_date=selected_as_of_date,
            limit=18,
            order_by="recommendation_score DESC, overall_rank ASC, player_name",
        )
        signals = rows[:6]
        rankings = sorted(
            [item for item in rows if item.get("overall_rank") is not None],
            key=lambda item: int(item["overall_rank"]),
        )[:8]
        trends = sorted(
            rows,
            key=lambda item: abs(_to_float(item.get("trend_delta")) or 0.0),
            reverse=True,
        )[:6]
        opportunity = [
            item for item in rows if item["opportunity_state"] != STATE_UNAVAILABLE
        ][:6]
        return {
            "selected_as_of_date": selected_as_of_date,
            "date_options": [
                {
                    "value": value,
                    "label": _format_home_date_label(value),
                    "is_selected": value == selected_as_of_date,
                }
                for value in date_options
            ],
            "signals": signals,
            "rankings": rankings,
            "trends": trends,
            "opportunity": opportunity,
        }

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
        return self._fetch_dashboard_rows(
            limit=limit,
            order_by="ABS(trend_delta) DESC, player_name",
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
        return self._fetch_dashboard_rows(
            limit=limit,
            order_by="overall_rank ASC, recommendation_score DESC, player_name",
        )

    def search_players(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        table = self._dim_player_table()
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
        identity = self._fetch_player_identity(player_id)
        if identity is None:
            return None

        sql = f"""
        SELECT
          season,
          as_of_date,
          player_id,
          player_name,
          latest_team_abbr,
          latest_game_date,
          overall_rank,
          recommendation_score,
          recommendation_tier,
          category_strengths,
          category_risks,
          trend_delta,
          trend_pct_change,
          trend_status,
          next_game_date,
          next_opponent_abbr,
          games_next_7d,
          back_to_backs_next_7d,
          opportunity_score,
          reason_primary_code,
          reason_primary_value,
          reason_secondary_code,
          reason_secondary_value,
          reason_context_code,
          reason_context_value,
          z_pts,
          z_reb,
          z_ast,
          z_stl,
          z_blk,
          z_fg3m,
          z_tov,
          category_score_7cat,
          category_coverage_status,
          last_5_games,
          last_5_avg_min,
          last_5_avg_pts,
          last_5_avg_reb,
          last_5_avg_ast,
          last_5_avg_stl,
          last_5_avg_blk,
          last_5_avg_fg3m,
          last_5_avg_tov,
          last_5_fantasy_proxy,
          prior_5_games,
          prior_5_avg_min,
          prior_5_avg_pts,
          prior_5_avg_reb,
          prior_5_avg_ast,
          prior_5_avg_stl,
          prior_5_avg_blk,
          prior_5_avg_fg3m,
          prior_5_avg_tov,
          prior_5_fantasy_proxy,
          last_10_games,
          last_10_avg_min,
          last_10_avg_pts,
          last_10_avg_reb,
          last_10_avg_ast,
          last_10_avg_stl,
          last_10_avg_blk,
          last_10_avg_fg3m,
          last_10_avg_tov,
          last_10_fantasy_proxy
        FROM {self._detail_table()}
        WHERE season = @season
          AND player_id = @player_id
        LIMIT 1
        """
        rows = self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
            ],
        )
        row = rows[0] if rows else None
        anchor = self._fetch_similarity_anchor(player_id)
        (
            similarity_state,
            similarity_reason,
            similar_players,
        ) = self._get_similar_players(
            player_id,
            anchor=anchor,
        )
        return self._build_player_detail_payload(
            identity=identity,
            row=row,
            archetype_row=anchor,
            similarity_state=similarity_state,
            similarity_reason=similarity_reason,
            similar_players=similar_players,
        )

    def get_compare(
        self,
        player_a_id: int,
        player_b_id: int,
        *,
        window: CompareWindow = "last_5",
        focus: CompareFocus = "balanced",
    ) -> dict[str, Any]:
        sql = f"""
        SELECT
          season,
          as_of_date,
          player_id,
          player_name,
          latest_team_abbr,
          latest_game_date,
          window_key,
          window_games_expected,
          games_in_window,
          has_full_window,
          avg_min,
          avg_pts,
          avg_reb,
          avg_ast,
          avg_stl,
          avg_blk,
          avg_fg3m,
          avg_tov,
          fantasy_proxy_score
        FROM {self._compare_table()}
        WHERE season = @season
          AND window_key = @window
          AND player_id IN (@player_a_id, @player_b_id)
        """
        rows = self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("window", "STRING", window),
                bigquery.ScalarQueryParameter("player_a_id", "INT64", player_a_id),
                bigquery.ScalarQueryParameter("player_b_id", "INT64", player_b_id),
            ],
        )
        rows_by_player = {int(row["player_id"]): row for row in rows}
        pair_similarity = self._get_pair_similarity(player_a_id, player_b_id)

        def build_side(player_id: int) -> dict[str, Any]:
            row = rows_by_player.get(player_id)
            detail = self.get_player_detail(player_id)
            if detail is None:
                metrics = _empty_compare_metrics()
                return {
                    "player_id": player_id,
                    "player_name": None,
                    "headshot_url": None,
                    "player_initials": "NBA",
                    "latest_team_abbr": None,
                    "latest_game_date": None,
                    "window": window,
                    "window_label": _compare_window_label(window),
                    "state": STATE_UNAVAILABLE,
                    "state_reason": "Player not found",
                    "games_in_window": None,
                    "window_games_expected": _compare_window_expected_games(window),
                    "has_full_window": False,
                    "metrics": metrics,
                    "metric_rows": _build_compare_metric_rows(metrics, focus),
                }

            games_in_window = _to_int((row or {}).get("games_in_window"))
            expected_games = _to_int((row or {}).get("window_games_expected"))
            if expected_games is None:
                expected_games = _compare_window_expected_games(window)
            if row is None:
                state = STATE_UNAVAILABLE
                state_reason = (
                    detail["availability_reason"]
                    if detail["availability_state"] == STATE_UNAVAILABLE
                    else _window_reason(state, _compare_window_label(window))
                )
            else:
                state = _window_state(games_in_window, expected_games)
                state_reason = _window_reason(state, _compare_window_label(window))
            metrics = {
                "fantasy_proxy_score": (row or {}).get("fantasy_proxy_score"),
                "avg_min": (row or {}).get("avg_min"),
                "avg_pts": (row or {}).get("avg_pts"),
                "avg_reb": (row or {}).get("avg_reb"),
                "avg_ast": (row or {}).get("avg_ast"),
                "avg_stl": (row or {}).get("avg_stl"),
                "avg_blk": (row or {}).get("avg_blk"),
                "avg_fg3m": (row or {}).get("avg_fg3m"),
                "avg_tov": (row or {}).get("avg_tov"),
            }
            return {
                "player_id": player_id,
                "player_name": detail["player"]["player_name"],
                "headshot_url": detail["player"].get("headshot_url"),
                "player_initials": detail["player"].get("player_initials"),
                "latest_team_abbr": (row or {}).get(
                    "latest_team_abbr", detail["player"]["team_abbr"]
                ),
                "latest_game_date": (row or {}).get(
                    "latest_game_date", detail["player"]["latest_game_date"]
                ),
                "window": window,
                "window_label": _compare_window_label(window),
                "state": state,
                "state_reason": state_reason,
                "games_in_window": games_in_window,
                "window_games_expected": expected_games,
                "has_full_window": bool((row or {}).get("has_full_window")),
                "availability_state": detail["availability_state"],
                "metrics": metrics,
                "metric_rows": _build_compare_metric_rows(metrics, focus),
                "player_detail": detail,
            }

        return {
            "season": SUPPORTED_SEASON,
            "window": window,
            "window_label": _compare_window_label(window),
            "focus": focus,
            "focus_label": str(COMPARE_FOCUS_CONFIG[focus]["label"]),
            "focus_description": str(COMPARE_FOCUS_CONFIG[focus]["description"]),
            "similarity": pair_similarity,
            "comparison": {
                "player_a": build_side(player_a_id),
                "player_b": build_side(player_b_id),
            },
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
          contribution_player_id,
          contribution_player_name,
          contribution_team_abbr,
          contribution_opponent_abbr,
          contribution_matchup,
          contribution_player_pts,
          contribution_team_pts,
          contribution_opponent_team_pts,
          contribution_player_points_share_of_team,
          contribution_player_points_share_of_game,
          contribution_scoring_margin,
          contribution_team_pts_qtr1,
          contribution_team_pts_qtr2,
          contribution_team_pts_qtr3,
          contribution_team_pts_qtr4,
          contribution_team_pts_ot_total,
          contribution_game_date,
          context_player_id,
          context_player_name,
          context_team_abbr,
          context_team_name,
          context_position,
          context_height,
          context_weight,
          context_roster_status,
          context_season_exp,
          context_draft_year,
          context_draft_round,
          context_draft_number,
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
        return build_analysis_payload(rows[0]) if rows else None

    def get_latest_successful_run(self) -> dict[str, Any] | None:
        table = f"`{self.settings.project_id}.{self.settings.metadata_dataset}.pipeline_run_log`"
        sql = f"""
        SELECT
          season,
          finished_at_utc
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

    def get_player_game_log(
        self, player_id: int, limit: int = 30
    ) -> dict[str, Any] | None:
        identity = self._fetch_player_identity(player_id)
        if identity is None:
            return None

        sql = f"""
        SELECT
          game_date, opponent_abbr, home_away, wl, min,
          pts, reb, ast, stl, blk, tov, fg3m,
          fgm, fga, fg_pct, ftm, fta, ft_pct,
          fantasy_points_simple
        FROM {self._fct_game_stats_table()}
        WHERE season = @season AND player_id = @player_id
        ORDER BY game_date DESC
        LIMIT @limit
        """
        rows = self._query(
            sql,
            [
                bigquery.ScalarQueryParameter("season", "STRING", SUPPORTED_SEASON),
                bigquery.ScalarQueryParameter("player_id", "INT64", player_id),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ],
        )
        rows.reverse()
        return {
            "player_id": identity.get("player_id"),
            "player_name": identity.get("player_name"),
            "season": SUPPORTED_SEASON,
            "games": rows,
        }

    def get_health(self) -> dict[str, Any]:
        latest_run = self.get_latest_successful_run()
        payload = build_freshness_payload(
            latest_run,
            now=datetime.now(tz=UTC),
            freshness_threshold_hours=self.settings.freshness_threshold_hours,
        )
        payload["season"] = SUPPORTED_SEASON
        return payload


def build_analysis_payload(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None

    item = dict(row)
    item["score_contribution"] = {
        "player_id": _to_int(row.get("contribution_player_id")),
        "player_name": row.get("contribution_player_name"),
        "team_abbr": row.get("contribution_team_abbr"),
        "opponent_abbr": row.get("contribution_opponent_abbr"),
        "matchup": row.get("contribution_matchup"),
        "player_pts": _to_int(row.get("contribution_player_pts")),
        "team_pts": _to_int(row.get("contribution_team_pts")),
        "opponent_team_pts": _to_int(row.get("contribution_opponent_team_pts")),
        "player_points_share_of_team": _to_float(
            row.get("contribution_player_points_share_of_team")
        ),
        "player_points_share_of_game": _to_float(
            row.get("contribution_player_points_share_of_game")
        ),
        "scoring_margin": _to_int(row.get("contribution_scoring_margin")),
        "team_pts_qtr1": _to_int(row.get("contribution_team_pts_qtr1")),
        "team_pts_qtr2": _to_int(row.get("contribution_team_pts_qtr2")),
        "team_pts_qtr3": _to_int(row.get("contribution_team_pts_qtr3")),
        "team_pts_qtr4": _to_int(row.get("contribution_team_pts_qtr4")),
        "team_pts_ot_total": _to_int(row.get("contribution_team_pts_ot_total")),
        "game_date": row.get("contribution_game_date"),
    }
    item["player_context"] = {
        "player_id": _to_int(row.get("context_player_id")),
        "player_name": row.get("context_player_name"),
        "team_abbr": row.get("context_team_abbr"),
        "team_name": row.get("context_team_name"),
        "position": row.get("context_position"),
        "height": row.get("context_height"),
        "weight": _to_int(row.get("context_weight")),
        "roster_status": (
            row.get("context_roster_status")
            if row.get("context_roster_status") in (True, False)
            else (
                str(row.get("context_roster_status")).lower() == "true"
                if row.get("context_roster_status") not in (None, "")
                else None
            )
        ),
        "season_exp": _to_int(row.get("context_season_exp")),
        "draft_year": row.get("context_draft_year"),
        "draft_round": row.get("context_draft_round"),
        "draft_number": row.get("context_draft_number"),
    }
    return item
