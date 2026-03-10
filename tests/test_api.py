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
    assert "NBA 2025-26 Daily Board" in response.text
    assert "Freshness: fresh" in response.text


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
