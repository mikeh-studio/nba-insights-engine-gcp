from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

from google.api_core.exceptions import BadRequest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings
from app.repository import (
    BigQueryWarehouseRepository,
    build_analysis_payload,
    build_freshness_payload,
)


def _build_repository() -> BigQueryWarehouseRepository:
    return BigQueryWarehouseRepository(
        Settings(
            project_id="local-project",
            gold_dataset="nba_gold",
            metadata_dataset="nba_metadata",
            freshness_threshold_hours=36,
            max_search_results=12,
        ),
        client=object(),
    )


def test_build_freshness_payload_fresh() -> None:
    payload = build_freshness_payload(
        {"finished_at_utc": "2026-02-11T01:15:00+00:00"},
        now=datetime(2026, 2, 11, 2, 0, tzinfo=UTC),
        freshness_threshold_hours=36,
    )

    assert payload["status"] == "fresh"
    assert payload["is_fresh"] is True
    assert payload["age_hours"] == 0.8


def test_build_freshness_payload_stale() -> None:
    payload = build_freshness_payload(
        {"finished_at_utc": "2026-02-09T01:15:00+00:00"},
        now=datetime(2026, 2, 11, 2, 0, tzinfo=UTC),
        freshness_threshold_hours=36,
    )

    assert payload["status"] == "stale"
    assert payload["is_fresh"] is False


def test_build_freshness_payload_missing() -> None:
    payload = build_freshness_payload(
        None,
        now=datetime(2026, 2, 11, 2, 0, tzinfo=UTC),
        freshness_threshold_hours=36,
    )

    assert payload["status"] == "missing"
    assert payload["last_successful_finished_at_utc"] is None


def test_build_freshness_payload_missing_finished_at_is_unavailable() -> None:
    payload = build_freshness_payload(
        {"finished_at_utc": None},
        now=datetime(2026, 2, 11, 2, 0, tzinfo=UTC),
        freshness_threshold_hours=36,
    )

    assert payload["status"] == "unavailable"
    assert payload["is_fresh"] is False


def test_build_analysis_payload_nests_structured_sections() -> None:
    payload = build_analysis_payload(
        {
            "snapshot_id": "202526_20260211",
            "season": "2025-26",
            "contribution_player_id": "7",
            "contribution_player_name": "Tyrese Maxey",
            "contribution_team_abbr": "PHI",
            "contribution_player_points_share_of_team": "0.2768",
            "contribution_team_pts_qtr4": "30",
            "context_player_id": "7",
            "context_player_name": "Tyrese Maxey",
            "context_position": "G",
            "context_roster_status": "true",
            "context_weight": "200",
        }
    )

    assert payload is not None
    assert payload["score_contribution"]["player_id"] == 7
    assert payload["score_contribution"]["player_points_share_of_team"] == 0.2768
    assert payload["score_contribution"]["team_pts_qtr4"] == 30
    assert payload["player_context"]["position"] == "G"
    assert payload["player_context"]["roster_status"] is True
    assert payload["player_context"]["weight"] == 200


def test_fetch_similarity_anchor_returns_none_on_bigquery_error(monkeypatch) -> None:
    repo = _build_repository()

    def fake_query(*_args, **_kwargs):
        raise BadRequest("similarity table missing")

    monkeypatch.setattr(repo, "_query", fake_query)

    assert repo._fetch_similarity_anchor(7) is None


def test_get_similar_players_returns_unavailable_on_bigquery_error(
    monkeypatch,
) -> None:
    repo = _build_repository()

    def fake_query(*_args, **_kwargs):
        raise BadRequest("similarity table missing")

    monkeypatch.setattr(repo, "_query", fake_query)

    state, reason, players = repo._get_similar_players(
        7,
        anchor={
            "sample_status": "ready",
            "top_traits": "playmaking, usage share",
        },
    )

    assert state == "unavailable"
    assert reason == "Similarity profile is unavailable."
    assert players == []


def test_get_pair_similarity_returns_unavailable_when_similarity_query_fails(
    monkeypatch,
) -> None:
    repo = _build_repository()

    def fake_query(*_args, **_kwargs):
        raise BadRequest("similarity table missing")

    monkeypatch.setattr(repo, "_query", fake_query)

    payload = repo._get_pair_similarity(7, 11)

    assert payload["state"] == "unavailable"
    assert payload["score"] is None
    assert (
        payload["summary"]
        == "Similarity profile is unavailable for at least one player."
    )
