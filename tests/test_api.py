from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app, get_repository
from app.repository import WarehouseRepository


class FakeRepository(WarehouseRepository):
    def get_dashboard(self) -> dict:
        return {
            "signals": [
                {
                    "player_id": 7,
                    "player_name": "Tyrese Maxey",
                    "latest_team_abbr": "PHI",
                    "overall_rank": 12,
                    "recommendation_score": 91.2,
                    "category_strengths": "PTS, AST, 3PM",
                    "category_risks": "FG%",
                    "reason_summary": "recent box score production: +5.2 | next 7 days: 4",
                    "trend_status": "rising",
                    "headshot_url": "https://cdn.nba.com/headshots/nba/latest/1040x760/7.png",
                    "player_initials": "TM",
                }
            ],
            "rankings": [
                {
                    "season": "2025-26",
                    "player_id": 7,
                    "player_name": "Tyrese Maxey",
                    "overall_rank": 12,
                    "recommendation_score": 91.2,
                }
            ],
            "trends": [
                {
                    "season": "2025-26",
                    "player_id": 7,
                    "player_name": "Tyrese Maxey",
                    "trend_status": "rising",
                    "trend_delta": 6.4,
                }
            ],
            "opportunity": [
                {
                    "season": "2025-26",
                    "player_id": 7,
                    "player_name": "Tyrese Maxey",
                    "games_next_7d": 4,
                    "back_to_backs_next_7d": 1,
                    "next_opponent_abbr": "NYK",
                    "opportunity_score": 84.0,
                }
            ],
        }

    def get_leaderboard(self, limit: int = 10) -> list[dict]:
        return [
            {
                "season": "2025-26",
                "game_date": "2026-02-10",
                "pts_leader": "Jayson Tatum",
                "pts_matchup": "BOS vs. NYK",
                "pts": 34,
                "reb_leader": "Karl-Anthony Towns",
                "reb": 14,
                "ast_leader": "Trae Young",
                "ast": 11,
            }
        ][:limit]

    def get_trends(self, limit: int = 10) -> list[dict]:
        return [
            {
                "season": "2025-26",
                "player_id": 7,
                "player_name": "Tyrese Maxey",
                "trend_status": "rising",
                "trend_delta": 6.4,
                "reason_summary": "recent box score production: +5.2",
            }
        ][:limit]

    def get_recommendations(
        self, limit: int = 10, insight_type: str | None = None
    ) -> list[dict]:
        items = [
            {
                "insight_id": "insight_1",
                "as_of_date": "2026-02-11",
                "player_id": 7,
                "player_name": "Tyrese Maxey",
                "insight_type": "waiver_add",
                "priority_score": 94.0,
                "confidence_score": 88.0,
                "category_focus": "PTS, AST, 3PM",
                "recommendation": "add",
                "title": "Tyrese Maxey is a high-priority add",
                "summary": "Minutes and assist creation are both trending up.",
            }
        ]
        if insight_type:
            items = [item for item in items if item["insight_type"] == insight_type]
        return items[:limit]

    def get_rankings(self, limit: int = 25) -> list[dict]:
        return [
            {
                "season": "2025-26",
                "player_id": 7,
                "player_name": "Tyrese Maxey",
                "overall_rank": 12,
                "recommendation_score": 91.2,
                "category_strengths": "PTS, AST, 3PM",
                "category_risks": "FG%",
                "games_next_7d": 4,
                "back_to_backs_next_7d": 1,
                "reason_summary": "recent box score production: +5.2",
            }
        ][:limit]

    def search_players(self, query: str, limit: int = 10) -> list[dict]:
        if "bridges" in query.lower():
            return [
                {
                    "player_id": 9,
                    "player_name": "Mikal Bridges",
                    "latest_season": "2025-26",
                    "last_seen_at_utc": "2026-02-10T13:00:00+00:00",
                }
            ][:limit]
        return [
            {
                "player_id": 7,
                "player_name": "Tyrese Maxey",
                "latest_season": "2025-26",
                "last_seen_at_utc": "2026-02-10T13:00:00+00:00",
            }
        ][:limit]

    def get_player_detail(self, player_id: int) -> dict | None:
        if player_id == 7:
            return {
                "player": {
                    "season": "2025-26",
                    "player_id": 7,
                    "player_name": "Tyrese Maxey",
                    "headshot_url": "https://cdn.nba.com/headshots/nba/latest/1040x760/7.png",
                    "player_initials": "TM",
                    "team_abbr": "PHI",
                    "latest_game_date": "2026-02-10",
                    "overall_rank": 12,
                    "recommendation_score": 91.2,
                    "recommendation_tier": "hold",
                    "category_strengths": "PTS, AST, 3PM",
                    "category_risks": "FG%",
                    "is_ranked": True,
                },
                "availability_state": "fresh",
                "availability_reason": None,
                "reason_summary": "recent box score production: +5.2 | next 7 days: 4",
                "trend": {
                    "status": "rising",
                    "delta": 6.4,
                    "pct_change": 29.1,
                },
                "panel_states": {
                    "recent_form": "fresh",
                    "category_profile": "fresh",
                    "opportunity": "fresh",
                },
                "recent_form": [
                    {
                        "window_key": "last_5",
                        "window_label": "Last 5",
                        "games_in_window": 5,
                        "window_games_expected": 5,
                        "state": "fresh",
                        "state_reason": None,
                        "avg_pts": 28.4,
                        "avg_reb": 4.8,
                        "avg_ast": 7.4,
                        "avg_stl": 1.3,
                        "avg_blk": 0.3,
                        "avg_fg3m": 3.1,
                        "avg_tov": 2.1,
                        "avg_minutes": 36.1,
                        "fantasy_proxy": 45.2,
                    },
                    {
                        "window_key": "prior_5",
                        "window_label": "Prior 5",
                        "games_in_window": 5,
                        "window_games_expected": 5,
                        "state": "fresh",
                        "state_reason": None,
                        "avg_pts": 22.0,
                        "avg_reb": 4.1,
                        "avg_ast": 6.0,
                        "avg_stl": 1.0,
                        "avg_blk": 0.2,
                        "avg_fg3m": 2.5,
                        "avg_tov": 2.4,
                        "avg_minutes": 34.0,
                        "fantasy_proxy": 40.0,
                    },
                    {
                        "window_key": "last_10",
                        "window_label": "Last 10",
                        "games_in_window": 10,
                        "window_games_expected": 10,
                        "state": "fresh",
                        "state_reason": None,
                        "avg_pts": 25.2,
                        "avg_reb": 4.4,
                        "avg_ast": 6.7,
                        "avg_stl": 1.1,
                        "avg_blk": 0.2,
                        "avg_fg3m": 2.8,
                        "avg_tov": 2.2,
                        "avg_minutes": 35.0,
                        "fantasy_proxy": 42.6,
                    },
                ],
                "category_profile": [
                    {
                        "category": "AST",
                        "impact_score": 1.8,
                        "category_tier": "plus",
                        "category_direction": "up",
                    }
                ],
                "opportunity": {
                    "games_next_7d": 4,
                    "back_to_backs_next_7d": 1,
                    "next_opponent": "NYK",
                    "next_game_date": "2026-02-13",
                    "opportunity_score": 84.0,
                },
            }
        if player_id == 9:
            return {
                "player": {
                    "season": "2025-26",
                    "player_id": 9,
                    "player_name": "Mikal Bridges",
                    "headshot_url": "https://cdn.nba.com/headshots/nba/latest/1040x760/9.png",
                    "player_initials": "MB",
                    "team_abbr": "NYK",
                    "latest_game_date": None,
                    "overall_rank": None,
                    "recommendation_score": None,
                    "recommendation_tier": None,
                    "category_strengths": None,
                    "category_risks": None,
                    "is_ranked": False,
                },
                "availability_state": "unavailable",
                "availability_reason": "Not currently ranked",
                "reason_summary": None,
                "trend": {
                    "status": "unavailable",
                    "delta": None,
                    "pct_change": None,
                },
                "panel_states": {
                    "recent_form": "unavailable",
                    "category_profile": "unavailable",
                    "opportunity": "unavailable",
                },
                "recent_form": [],
                "category_profile": [],
                "opportunity": None,
            }
        return None

    def get_compare(
        self,
        player_a_id: int,
        player_b_id: int,
        *,
        window: str = "last_5",
    ) -> dict:
        return {
            "season": "2025-26",
            "window": window,
            "comparison": {
                "player_a": {
                    "player_id": player_a_id,
                    "player_name": "Tyrese Maxey",
                    "headshot_url": "https://cdn.nba.com/headshots/nba/latest/1040x760/7.png",
                    "player_initials": "TM",
                    "latest_team_abbr": "PHI",
                    "latest_game_date": "2026-02-10",
                    "window": window,
                    "state": "fresh",
                    "state_reason": None,
                    "games_in_window": 5,
                    "window_games_expected": 5,
                    "has_full_window": True,
                    "availability_state": "fresh",
                    "player_detail": self.get_player_detail(player_a_id),
                    "metrics": {
                        "fantasy_proxy_score": 45.2,
                        "avg_min": 36.1,
                        "avg_pts": 28.4,
                        "avg_reb": 4.8,
                        "avg_ast": 7.4,
                        "avg_stl": 1.3,
                        "avg_blk": 0.3,
                        "avg_fg3m": 3.1,
                        "avg_tov": 2.1,
                    },
                },
                "player_b": {
                    "player_id": player_b_id,
                    "player_name": "Mikal Bridges",
                    "headshot_url": "https://cdn.nba.com/headshots/nba/latest/1040x760/9.png",
                    "player_initials": "MB",
                    "latest_team_abbr": "NYK",
                    "latest_game_date": None,
                    "window": window,
                    "state": "unavailable",
                    "state_reason": "Not currently ranked",
                    "games_in_window": None,
                    "window_games_expected": 5,
                    "has_full_window": False,
                    "availability_state": "unavailable",
                    "player_detail": self.get_player_detail(player_b_id),
                    "metrics": {
                        "fantasy_proxy_score": None,
                        "avg_min": None,
                        "avg_pts": None,
                        "avg_reb": None,
                        "avg_ast": None,
                        "avg_stl": None,
                        "avg_blk": None,
                        "avg_fg3m": None,
                        "avg_tov": None,
                    },
                },
            },
        }

    def get_latest_analysis(self) -> dict | None:
        return {
            "snapshot_id": "202526_20260211",
            "snapshot_date": "2026-02-11",
            "created_at_utc": "2026-02-11T01:02:03+00:00",
            "season": "2025-26",
            "headline": "Tyrese Maxey headlines the 2025-26 trend watch",
            "dek": "Latest leaders from 2026-02-10 are anchored by Jayson Tatum in scoring.",
            "body": "Deterministic analysis body.",
            "trend_player": "Tyrese Maxey",
            "trend_stat": "PTS",
            "trend_delta": 6.4,
            "freshness_ts": "2026-02-10T13:00:00+00:00",
            "source_run_id": "manual__2026-02-11T01:02:03+00:00",
        }

    def get_latest_successful_run(self) -> dict | None:
        return {"season": "2025-26", "finished_at_utc": "2026-02-11T01:15:00+00:00"}

    def get_health(self) -> dict:
        return {
            "season": "2025-26",
            "status": "fresh",
            "is_fresh": True,
            "checked_at_utc": "2026-02-11T02:00:00+00:00",
            "age_hours": 0.8,
            "threshold_hours": 36,
            "last_successful_finished_at_utc": "2026-02-11T01:15:00+00:00",
        }


class StaleRepository(FakeRepository):
    def get_health(self) -> dict:
        return {
            "season": "2025-26",
            "status": "stale",
            "is_fresh": False,
            "checked_at_utc": "2026-02-11T12:00:00+00:00",
            "age_hours": 48.0,
            "threshold_hours": 36,
            "last_successful_finished_at_utc": "2026-02-09T12:00:00+00:00",
        }


class MissingOpportunityRepository(FakeRepository):
    def get_dashboard(self) -> dict:
        payload = super().get_dashboard()
        payload["opportunity"] = []
        return payload

    def get_player_detail(self, player_id: int) -> dict | None:
        detail = super().get_player_detail(player_id)
        if detail is None:
            return None
        if player_id == 7:
            detail["panel_states"]["opportunity"] = "unavailable"
            detail["opportunity"] = None
        return detail


def build_client(repo: WarehouseRepository | None = None) -> TestClient:
    app.dependency_overrides[get_repository] = lambda: repo or FakeRepository()
    return TestClient(app)


def test_home_page_smoke() -> None:
    client = build_client()
    response = client.get("/")

    assert response.status_code == 200
    assert "Stats Dashboard" in response.text
    assert "Signal Board" in response.text
    assert "Tracked Players" in response.text
    assert "https://cdn.nba.com/headshots/nba/latest/1040x760/7.png" in response.text


def test_home_page_shows_stale_notice() -> None:
    client = build_client(StaleRepository())
    response = client.get("/")

    assert response.status_code == 200
    assert "Use the board with caution." in response.text


def test_home_page_missing_opportunity_state() -> None:
    client = build_client(MissingOpportunityRepository())
    response = client.get("/")

    assert response.status_code == 200
    assert "Opportunity context is unavailable." in response.text


def test_analysis_page_smoke() -> None:
    client = build_client()
    response = client.get("/analysis")

    assert response.status_code == 200
    assert "Latest Snapshot" in response.text
    assert "Tyrese Maxey headlines the 2025-26 trend watch" in response.text


def test_recommendations_page_smoke() -> None:
    client = build_client()
    response = client.get("/recommendations")

    assert response.status_code == 200
    assert "Recommendation Feed" in response.text
    assert "Tyrese Maxey is a high-priority add" in response.text


def test_player_page_smoke() -> None:
    client = build_client()
    response = client.get("/players/7")

    assert response.status_code == 200
    assert "Why This Player Matters" in response.text
    assert "Compare player" in response.text
    assert "Recent Windows" in response.text
    assert "Box Score Index" in response.text
    assert "https://cdn.nba.com/headshots/nba/latest/1040x760/7.png" in response.text


def test_player_page_unavailable_state_smoke() -> None:
    client = build_client()
    response = client.get("/players/9")

    assert response.status_code == 200
    assert "This player is not currently ranked." in response.text


def test_compare_page_with_only_player_a() -> None:
    client = build_client()
    response = client.get("/compare?player_a_id=7")

    assert response.status_code == 200
    assert "Build Comparison" in response.text
    assert "Pick a second player to finish the comparison." in response.text


def test_compare_page_full_surface() -> None:
    client = build_client()
    response = client.get("/compare?player_a_id=7&player_b_id=9&window=last_5")

    assert response.status_code == 200
    assert "Comparison Surface" in response.text
    assert "Mikal Bridges" in response.text
    assert "Limited comparison data." in response.text
    assert "Box Score Index" in response.text
    assert "https://cdn.nba.com/headshots/nba/latest/1040x760/7.png" in response.text


def test_compare_page_duplicate_validation() -> None:
    client = build_client()
    response = client.get("/compare?player_a_id=7&player_b_id=7")

    assert response.status_code == 200
    assert "Compare players must be different." in response.text


def test_api_health_smoke() -> None:
    client = build_client()
    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "fresh"
    assert payload["last_successful_finished_at_utc"] == "2026-02-11T01:15:00+00:00"
    assert "latest_successful_run" not in payload


def test_api_player_search_rejects_blank_query() -> None:
    client = build_client()
    response = client.get("/api/players/search?q=%20%20%20")

    assert response.status_code == 400
    assert response.json()["detail"] == "Search query must not be blank"


def test_api_player_detail_available_player() -> None:
    client = build_client()
    response = client.get("/api/players/7")

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["availability_state"] == "fresh"
    assert payload["item"]["player"]["overall_rank"] == 12
    assert payload["item"]["player"]["headshot_url"].endswith("/7.png")


def test_api_player_detail_unavailable_known_player() -> None:
    client = build_client()
    response = client.get("/api/players/9")

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["availability_state"] == "unavailable"
    assert payload["item"]["availability_reason"] == "Not currently ranked"


def test_api_player_detail_404() -> None:
    client = build_client()
    response = client.get("/api/players/999")

    assert response.status_code == 404


def test_api_compare_smoke() -> None:
    client = build_client()
    response = client.get("/api/compare?player_a_id=7&player_b_id=9&window=last_5")

    assert response.status_code == 200
    payload = response.json()
    assert payload["window"] == "last_5"
    assert payload["comparison"]["player_a"]["player_name"] == "Tyrese Maxey"
    assert payload["comparison"]["player_b"]["state"] == "unavailable"


def test_api_compare_rejects_duplicate_selection() -> None:
    client = build_client()
    response = client.get("/api/compare?player_a_id=7&player_b_id=7")

    assert response.status_code == 400
    assert response.json()["detail"] == "Compare players must be different"
