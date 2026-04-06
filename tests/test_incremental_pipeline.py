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


def test_game_logs_schema_includes_game_id():
    schema_names = [field.name for field in pipeline.get_game_logs_schema()]
    assert schema_names[0] == "GAME_ID"


def test_get_all_game_line_scores_dedupes_by_game_and_team(monkeypatch):
    def fake_get_game_line_scores(game_id: str, *, season: str = "2025-26"):
        return pd.DataFrame(
            [
                {
                    "GAME_DATE": "2026-02-10",
                    "GAME_ID": game_id,
                    "SEASON": season,
                    "TEAM_ID": 1,
                    "TEAM_ABBR": "BOS",
                    "TEAM_CITY_NAME": "Boston",
                    "TEAM_NICKNAME": "Celtics",
                    "TEAM_WINS_LOSSES": "40-10",
                    "PTS_QTR1": 30,
                    "PTS_QTR2": 25,
                    "PTS_QTR3": 20,
                    "PTS_QTR4": 35,
                    "PTS_OT1": 0,
                    "PTS_OT2": 0,
                    "PTS_OT3": 0,
                    "PTS_OT4": 0,
                    "PTS_OT5": 0,
                    "PTS_OT6": 0,
                    "PTS_OT7": 0,
                    "PTS_OT8": 0,
                    "PTS_OT9": 0,
                    "PTS_OT10": 0,
                    "PTS": 110,
                    "INGESTED_AT_UTC": "2026-02-10T12:00:00+00:00",
                },
                {
                    "GAME_DATE": "2026-02-10",
                    "GAME_ID": game_id,
                    "SEASON": season,
                    "TEAM_ID": 1,
                    "TEAM_ABBR": "BOS",
                    "TEAM_CITY_NAME": "Boston",
                    "TEAM_NICKNAME": "Celtics",
                    "TEAM_WINS_LOSSES": "40-10",
                    "PTS_QTR1": 30,
                    "PTS_QTR2": 25,
                    "PTS_QTR3": 20,
                    "PTS_QTR4": 35,
                    "PTS_OT1": 0,
                    "PTS_OT2": 0,
                    "PTS_OT3": 0,
                    "PTS_OT4": 0,
                    "PTS_OT5": 0,
                    "PTS_OT6": 0,
                    "PTS_OT7": 0,
                    "PTS_OT8": 0,
                    "PTS_OT9": 0,
                    "PTS_OT10": 0,
                    "PTS": 110,
                    "INGESTED_AT_UTC": "2026-02-10T12:00:00+00:00",
                },
            ]
        )

    monkeypatch.setattr(pipeline, "get_game_line_scores", fake_get_game_line_scores)
    result = pipeline.get_all_game_line_scores(["001", "001"], season="2025-26", delay=0)

    assert len(result) == 1
    assert result.iloc[0]["GAME_ID"] == "001"
    assert result.iloc[0]["TEAM_ID"] == 1


def test_get_all_player_references_dedupes_by_player_id(monkeypatch):
    def fake_get_player_reference(player_id: int):
        return pd.DataFrame(
            [
                {
                    "PLAYER_ID": player_id,
                    "FIRST_NAME": "Test",
                    "LAST_NAME": "Player",
                    "PLAYER_NAME": "Test Player",
                    "PLAYER_SLUG": "test-player",
                    "BIRTHDATE": "2000-01-01",
                    "SCHOOL": "Example U",
                    "COUNTRY": "USA",
                    "LAST_AFFILIATION": "Example U",
                    "HEIGHT": "6-5",
                    "WEIGHT": 210,
                    "SEASON_EXP": 2,
                    "JERSEY": "7",
                    "POSITION": "G",
                    "ROSTER_STATUS": True,
                    "TEAM_ID": 1610612738,
                    "TEAM_NAME": "Celtics",
                    "TEAM_ABBR": "BOS",
                    "TEAM_CODE": "celtics",
                    "TEAM_CITY": "Boston",
                    "FROM_YEAR": 2023,
                    "TO_YEAR": 2026,
                    "DRAFT_YEAR": "2023",
                    "DRAFT_ROUND": "1",
                    "DRAFT_NUMBER": "7",
                    "INGESTED_AT_UTC": "2026-02-10T12:00:00+00:00",
                }
            ]
        )

    monkeypatch.setattr(pipeline, "get_player_reference", fake_get_player_reference)
    players = [{"id": 7, "full_name": "Test Player"}, {"id": 7, "full_name": "Test Player"}]
    result = pipeline.get_all_player_references(players, delay=0)

    assert len(result) == 1
    assert result.iloc[0]["PLAYER_ID"] == 7


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


def test_build_analysis_snapshot_record_includes_scoring_contribution_and_context():
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
    score_contribution = pd.DataFrame(
        [
            {
                "season": "2025-26",
                "game_id": "001",
                "game_date": "2026-02-10",
                "player_id": 7,
                "player_name": "Tyrese Maxey",
                "team_abbr": "PHI",
                "opponent_abbr": "NYK",
                "matchup": "PHI vs. NYK",
                "player_pts": 31,
                "team_pts": 112,
                "opponent_team_pts": 108,
                "team_pts_qtr1": 28,
                "team_pts_qtr2": 24,
                "team_pts_qtr3": 30,
                "team_pts_qtr4": 30,
                "team_pts_ot_total": 0,
                "scoring_margin": 4,
                "player_points_share_of_team": 0.2768,
                "player_points_share_of_game": 0.1416,
            }
        ]
    )
    player_context = pd.DataFrame(
        [
            {
                "player_id": 7,
                "player_name": "Tyrese Maxey",
                "latest_team_abbr": "PHI",
                "team_name": "76ers",
                "position": "G",
                "height": "6-2",
                "weight": 200,
                "roster_status": True,
                "season_exp": 5,
                "draft_year": "2020",
                "draft_round": "1",
                "draft_number": "21",
            }
        ]
    )

    record = pipeline.build_analysis_snapshot_record(
        season="2025-26",
        daily_leaders=daily_leaders,
        trends=trends,
        score_contribution=score_contribution,
        player_context=player_context,
        source_run_id="manual__2026-02-10T00:00:00+00:00",
        created_at_utc="2026-02-11T01:02:03+00:00",
        freshness_ts="2026-02-10T13:00:00+00:00",
    )

    assert record["contribution_player_id"] == 7
    assert record["contribution_player_name"] == "Tyrese Maxey"
    assert record["contribution_player_points_share_of_team"] == 0.2768
    assert record["contribution_team_pts_qtr4"] == 30
    assert record["context_player_id"] == 7
    assert record["context_position"] == "G"
    assert record["context_roster_status"] is True
    assert "Tyrese Maxey supplied 31 of 112 PHI points" in record["body"]
    assert "Tyrese Maxey is listed as a G for 76ers (PHI)" in record["body"]
