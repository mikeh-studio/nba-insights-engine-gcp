from __future__ import annotations

from datetime import UTC, datetime

from app.repository import build_analysis_payload, build_freshness_payload


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
