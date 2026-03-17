from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app, get_repository
from app.repository import WarehouseRepository


class FakeRepository(WarehouseRepository):
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
                "stat": "PTS",
                "recent_games": 5,
                "prior_games": 5,
                "recent_avg": 28.4,
                "prior_avg": 22.0,
                "delta": 6.4,
                "pct_change": 29.1,
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
                "source_run_id": "manual__2026-02-11T01:02:03+00:00",
            },
            {
                "insight_id": "insight_2",
                "as_of_date": "2026-02-11",
                "player_id": 9,
                "player_name": "Mikal Bridges",
                "insight_type": "sell_high",
                "priority_score": 86.0,
                "confidence_score": 77.0,
                "category_focus": "FG%, STL",
                "recommendation": "sell",
                "title": "Mikal Bridges looks like a sell-high",
                "summary": "Efficiency has surged above baseline while opportunity is flat.",
                "source_run_id": "manual__2026-02-11T01:02:03+00:00",
            },
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
            }
        ][:limit]

    def search_players(self, query: str, limit: int = 10) -> list[dict]:
        return [
            {
                "player_id": 7,
                "player_name": "Tyrese Maxey",
                "latest_season": "2025-26",
                "last_seen_at_utc": "2026-02-10T13:00:00+00:00",
            }
        ][:limit]

    def get_player_detail(self, player_id: int) -> dict | None:
        if player_id != 7:
            return None
        return {
            "player": self.get_rankings(1)[0],
            "recent_form": [
                {
                    "window_label": "Last 3",
                    "avg_pts": 30.0,
                    "avg_reb": 5.0,
                    "avg_ast": 8.0,
                    "avg_stl": 1.7,
                    "avg_blk": 0.3,
                    "avg_fg3m": 3.7,
                },
                {
                    "window_label": "Last 5",
                    "avg_pts": 28.4,
                    "avg_reb": 4.8,
                    "avg_ast": 7.4,
                    "avg_stl": 1.3,
                    "avg_blk": 0.3,
                    "avg_fg3m": 3.1,
                },
                {
                    "window_label": "Last 10",
                    "avg_pts": 25.8,
                    "avg_reb": 4.3,
                    "avg_ast": 6.9,
                    "avg_stl": 1.1,
                    "avg_blk": 0.2,
                    "avg_fg3m": 2.8,
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
                "opportunity_score": 84.0,
            },
            "insights": self.get_recommendations(limit=1),
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
        return {
            "dag_run_id": "manual__2026-02-11T01:02:03+00:00",
            "season": "2025-26",
            "status": "success",
            "rows_loaded": 123,
            "finished_at_utc": "2026-02-11T01:15:00+00:00",
        }

    def get_health(self) -> dict:
        return {
            "season": "2025-26",
            "status": "fresh",
            "is_fresh": True,
            "checked_at_utc": "2026-02-11T02:00:00+00:00",
            "latest_successful_run": self.get_latest_successful_run(),
            "age_hours": 0.8,
            "threshold_hours": 36,
        }


def build_client() -> TestClient:
    app.dependency_overrides[get_repository] = lambda: FakeRepository()
    return TestClient(app)


def test_home_page_smoke() -> None:
    client = build_client()
    response = client.get("/")

    assert response.status_code == 200
    assert "NBA 2025-26 Fantasy Dashboard" in response.text
    assert "Freshness: fresh" in response.text
    assert "Tyrese Maxey is a high-priority add" in response.text


def test_analysis_page_smoke() -> None:
    client = build_client()
    response = client.get("/analysis")

    assert response.status_code == 200
    assert "Deterministic Snapshot" in response.text
    assert "Tyrese Maxey headlines the 2025-26 trend watch" in response.text


def test_api_leaderboard_smoke() -> None:
    client = build_client()
    response = client.get("/api/leaderboard")

    assert response.status_code == 200
    payload = response.json()
    assert payload["season"] == "2025-26"
    assert payload["items"][0]["pts_leader"] == "Jayson Tatum"


def test_api_trends_smoke() -> None:
    client = build_client()
    response = client.get("/api/trends")

    assert response.status_code == 200
    payload = response.json()
    assert payload["season"] == "2025-26"
    assert payload["items"][0]["player_name"] == "Tyrese Maxey"


def test_api_analysis_latest_smoke() -> None:
    client = build_client()
    response = client.get("/api/analysis/latest")

    assert response.status_code == 200
    payload = response.json()
    assert payload["season"] == "2025-26"
    assert payload["item"]["snapshot_id"] == "202526_20260211"


def test_api_health_smoke() -> None:
    client = build_client()
    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["season"] == "2025-26"
    assert payload["status"] == "fresh"


def test_recommendations_page_smoke() -> None:
    client = build_client()
    response = client.get("/recommendations")

    assert response.status_code == 200
    assert "Fantasy Recommendations" in response.text
    assert "Tyrese Maxey is a high-priority add" in response.text


def test_player_page_smoke() -> None:
    client = build_client()
    response = client.get("/players/7")

    assert response.status_code == 200
    assert "Tyrese Maxey Fantasy Outlook" in response.text
    assert "Opportunity Outlook" in response.text
    assert "Last 3" in response.text
    assert "Last 10" in response.text


def test_api_recommendations_smoke() -> None:
    client = build_client()
    response = client.get("/api/recommendations?limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["season"] == "2025-26"
    assert payload["items"][0]["insight_type"] == "waiver_add"


def test_api_rankings_smoke() -> None:
    client = build_client()
    response = client.get("/api/rankings")

    assert response.status_code == 200
    payload = response.json()
    assert payload["season"] == "2025-26"
    assert payload["items"][0]["player_name"] == "Tyrese Maxey"


def test_api_player_search_smoke() -> None:
    client = build_client()
    response = client.get("/api/players/search?q=maxey")

    assert response.status_code == 200
    payload = response.json()
    assert payload["query"] == "maxey"
    assert payload["items"][0]["player_name"] == "Tyrese Maxey"


def test_api_player_detail_smoke() -> None:
    client = build_client()
    response = client.get("/api/players/7")

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["player"]["overall_rank"] == 12


def test_api_player_detail_404() -> None:
    client = build_client()
    response = client.get("/api/players/999")

    assert response.status_code == 404
