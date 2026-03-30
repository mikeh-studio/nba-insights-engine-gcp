from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import nba_pipeline as pipeline


def test_compute_replay_start_uses_inclusive_window():
    assert (
        pipeline.compute_replay_start("2026-02-10", replay_days=3).isoformat()
        == "2026-02-08"
    )


def test_filter_incremental_game_logs_applies_replay_window_and_dedupes():
    df = pd.DataFrame(
        [
            {
                "PLAYER_ID": 1,
                "PLAYER_NAME": "A",
                "GAME_DATE": "2026-02-07",
                "MATCHUP": "LAL vs. BOS",
                "PTS": 10,
                "REB": 5,
                "AST": 2,
                "STL": 1,
                "BLK": 0,
                "WL": "W",
                "SEASON": "2025-26",
            },
            {
                "PLAYER_ID": 1,
                "PLAYER_NAME": "A",
                "GAME_DATE": "2026-02-08",
                "MATCHUP": "LAL vs. BOS",
                "PTS": 12,
                "REB": 5,
                "AST": 2,
                "STL": 1,
                "BLK": 0,
                "WL": "l",
                "SEASON": "2025-26",
            },
            {
                "PLAYER_ID": 1,
                "PLAYER_NAME": "A",
                "GAME_DATE": "2026-02-08",
                "MATCHUP": "LAL vs. BOS",
                "PTS": 12,
                "REB": 5,
                "AST": 2,
                "STL": 1,
                "BLK": 0,
                "WL": "L",
                "SEASON": "2025-26",
            },
            {
                "PLAYER_ID": 1,
                "PLAYER_NAME": "A",
                "GAME_DATE": "2026-02-10",
                "MATCHUP": "LAL @ NYK",
                "PTS": 18,
                "REB": 6,
                "AST": 4,
                "STL": 2,
                "BLK": 1,
                "WL": "W",
                "SEASON": "2025-26",
            },
        ]
    )

    result = pipeline.filter_incremental_game_logs(
        df,
        watermark_date="2026-02-10",
        replay_days=3,
        season="2025-26",
    )

    assert len(result) == 2
    assert result["GAME_DATE"].dt.strftime("%Y-%m-%d").tolist() == [
        "2026-02-10",
        "2026-02-08",
    ]
    assert result["WL"].tolist() == ["W", "L"]


def test_filter_incremental_game_logs_keeps_only_2025_26_rows():
    df = pd.DataFrame(
        [
            {
                "PLAYER_ID": 1,
                "PLAYER_NAME": "A",
                "GAME_DATE": "2026-02-08",
                "MATCHUP": "LAL vs. BOS",
                "PTS": 12,
                "REB": 5,
                "AST": 2,
                "STL": 1,
                "BLK": 0,
                "WL": "W",
                "SEASON": "2024-25",
            },
            {
                "PLAYER_ID": 2,
                "PLAYER_NAME": "B",
                "GAME_DATE": "2026-02-09",
                "MATCHUP": "NYK @ MIA",
                "PTS": 20,
                "REB": 7,
                "AST": 8,
                "STL": 1,
                "BLK": 0,
                "WL": "L",
                "SEASON": "2025-26",
            },
        ]
    )

    result = pipeline.filter_incremental_game_logs(df, season="2025-26")

    assert len(result) == 1
    assert result.iloc[0]["PLAYER_ID"] == 2
    assert result.iloc[0]["SEASON"] == "2025-26"


def test_filter_incremental_game_logs_rejects_rows_outside_2025_26_window():
    df = pd.DataFrame(
        [
            {
                "PLAYER_ID": 1,
                "PLAYER_NAME": "A",
                "GAME_DATE": "2025-06-30",
                "MATCHUP": "LAL vs. BOS",
                "PTS": 10,
                "REB": 5,
                "AST": 2,
                "STL": 1,
                "BLK": 0,
                "WL": "W",
                "SEASON": "2025-26",
            },
            {
                "PLAYER_ID": 2,
                "PLAYER_NAME": "B",
                "GAME_DATE": "2026-02-09",
                "MATCHUP": "NYK @ MIA",
                "PTS": 20,
                "REB": 7,
                "AST": 8,
                "STL": 1,
                "BLK": 0,
                "WL": "L",
                "SEASON": "2025-26",
            },
            {
                "PLAYER_ID": 3,
                "PLAYER_NAME": "C",
                "GAME_DATE": "2026-07-01",
                "MATCHUP": "BOS @ PHI",
                "PTS": 22,
                "REB": 6,
                "AST": 4,
                "STL": 1,
                "BLK": 0,
                "WL": "W",
                "SEASON": "2025-26",
            },
        ]
    )

    result = pipeline.filter_incremental_game_logs(df, season="2025-26")

    assert len(result) == 1
    assert result.iloc[0]["PLAYER_ID"] == 2


def test_build_run_metadata_record_serializes_dates():
    record = pipeline.build_run_metadata_record(
        dag_run_id="manual__2025-02-10T00:00:00+00:00",
        season="2025-26",
        status="success",
        watermark_before="2025-02-08",
        watermark_after="2025-02-10",
        rows_extracted=10,
        rows_loaded=10,
        rows_inserted=4,
        rows_updated=6,
    )

    assert record["watermark_before"] == "2025-02-08"
    assert record["watermark_after"] == "2025-02-10"
    assert record["rows_updated"] == 6


def test_validate_merge_reconciliation_returns_unchanged_rows():
    result = pipeline.validate_merge_reconciliation(
        domain="game_logs",
        rows_loaded=10,
        pre_count=100,
        post_count=104,
        inserted=4,
        updated=3,
    )

    assert result["rows_loaded"] == 10
    assert result["pre_count"] == 100
    assert result["post_count"] == 104
    assert result["inserted"] == 4
    assert result["updated"] == 3
    assert result["unchanged"] == 3


def test_validate_merge_reconciliation_rejects_insert_update_overflow():
    try:
        pipeline.validate_merge_reconciliation(
            domain="schedule",
            rows_loaded=5,
            pre_count=20,
            post_count=23,
            inserted=3,
            updated=3,
        )
    except ValueError as exc:
        assert "inserted+updated" in str(exc)
    else:
        raise AssertionError("Expected reconciliation overflow to raise ValueError")


def test_validate_merge_reconciliation_rejects_post_count_mismatch():
    try:
        pipeline.validate_merge_reconciliation(
            domain="game_logs",
            rows_loaded=4,
            pre_count=10,
            post_count=15,
            inserted=3,
            updated=1,
        )
    except ValueError as exc:
        assert "expected post_count 13" in str(exc)
    else:
        raise AssertionError("Expected post_count mismatch to raise ValueError")


def test_build_analysis_snapshot_record_is_deterministic():
    daily_leaders = pd.DataFrame(
        [
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
        ]
    )
    trends = pd.DataFrame(
        [
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
        ]
    )

    record = pipeline.build_analysis_snapshot_record(
        season="2025-26",
        daily_leaders=daily_leaders,
        trends=trends,
        source_run_id="manual__2026-02-10T00:00:00+00:00",
        created_at_utc="2026-02-11T01:02:03+00:00",
        freshness_ts="2026-02-10T13:00:00+00:00",
    )

    assert record["snapshot_id"] == "202526_20260211"
    assert record["snapshot_date"] == "2026-02-11"
    assert record["season"] == "2025-26"
    assert record["trend_player"] == "Tyrese Maxey"
    assert record["trend_stat"] == "PTS"
    assert record["trend_delta"] == 6.4
    assert "Tyrese Maxey is trending up in PTS" in record["body"]
    assert "Jayson Tatum led scoring with 34 points" in record["body"]
    assert (
        "The latest completed game day in the 2025-26 warehouse is 2026-02-10."
        in record["body"]
    )


def test_build_analysis_snapshot_record_includes_fantasy_recommendations():
    daily_leaders = pd.DataFrame(
        [
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
        ]
    )
    trends = pd.DataFrame(
        [
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
        ]
    )
    recommendations = pd.DataFrame(
        [
            {
                "player_name": "Tyrese Maxey",
                "insight_type": "waiver_add",
                "recommendation": "add",
                "priority_score": 94.0,
                "confidence_score": 88.0,
            }
        ]
    )
    rankings = pd.DataFrame(
        [
            {
                "player_name": "Nikola Jokic",
                "fantasy_rank_9cat_proxy": 1,
                "recommendation_tier": "strong_add",
            }
        ]
    )

    record = pipeline.build_analysis_snapshot_record(
        season="2025-26",
        daily_leaders=daily_leaders,
        trends=trends,
        recommendations=recommendations,
        rankings=rankings,
        source_run_id="manual__2026-02-10T00:00:00+00:00",
        created_at_utc="2026-02-11T01:02:03+00:00",
        freshness_ts="2026-02-10T13:00:00+00:00",
    )

    assert record["headline"] == "Tyrese Maxey headlines the 2025-26 fantasy board"
    assert "Top fantasy signal: Tyrese Maxey profiles as waiver_add" in record["body"]
    assert "Current fantasy leader: Nikola Jokic sits at rank 1" in record["body"]
