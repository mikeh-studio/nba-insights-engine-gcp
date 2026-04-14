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


def test_ensure_table_has_columns_adds_expected_columns():
    class DummyJob:
        def result(self):
            return None

    class DummyClient:
        def __init__(self):
            self.statements = []

        def query(self, statement):
            self.statements.append(statement)
            return DummyJob()

    client = DummyClient()

    pipeline.ensure_table_has_columns(
        client,
        "project.dataset.raw_game_logs",
        [
            pipeline.bigquery.SchemaField("GAME_ID", "STRING"),
            pipeline.bigquery.SchemaField("FGM", "FLOAT"),
            pipeline.bigquery.SchemaField("PLAYER_ID", "INTEGER"),
            pipeline.bigquery.SchemaField("ROSTER_STATUS", "BOOLEAN"),
        ],
    )

    assert client.statements == [
        "ALTER TABLE `project.dataset.raw_game_logs` ADD COLUMN IF NOT EXISTS game_id STRING",
        "ALTER TABLE `project.dataset.raw_game_logs` ADD COLUMN IF NOT EXISTS fgm FLOAT64",
        "ALTER TABLE `project.dataset.raw_game_logs` ADD COLUMN IF NOT EXISTS player_id INT64",
        "ALTER TABLE `project.dataset.raw_game_logs` ADD COLUMN IF NOT EXISTS roster_status BOOL",
    ]


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
    result = pipeline.get_all_game_line_scores(
        ["001", "001"], season="2025-26", delay=0
    )

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
    players = [
        {"id": 7, "full_name": "Test Player"},
        {"id": 7, "full_name": "Test Player"},
    ]
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


def test_build_player_similarity_outputs_returns_normalized_feature_tables():
    feature_rows = pd.DataFrame(
        [
            {
                "season": "2025-26",
                "as_of_date": "2026-02-11",
                "player_id": player_id,
                "player_name": player_name,
                "team_abbr": team_abbr,
                "position": position,
                "games_sampled": 24,
                "sample_status": "ready",
                "season_avg_pts": season_avg_pts,
                "season_avg_reb": season_avg_reb,
                "season_avg_ast": season_avg_ast,
                "season_avg_stl": season_avg_stl,
                "season_avg_blk": season_avg_blk,
                "season_avg_fg3m": season_avg_fg3m,
                "season_avg_tov": season_avg_tov,
                "season_avg_min": season_avg_min,
                "recent_pts": recent_pts,
                "recent_reb": recent_reb,
                "recent_ast": recent_ast,
                "recent_stl": recent_stl,
                "recent_blk": recent_blk,
                "recent_fg3m": recent_fg3m,
                "recent_tov": recent_tov,
                "recent_min": recent_min,
                "recent_points_share_of_team": recent_points_share_of_team,
                "recent_points_share_of_game": recent_points_share_of_game,
                "minutes_delta_vs_season": minutes_delta_vs_season,
            }
            for (
                player_id,
                player_name,
                team_abbr,
                position,
                season_avg_pts,
                season_avg_reb,
                season_avg_ast,
                season_avg_stl,
                season_avg_blk,
                season_avg_fg3m,
                season_avg_tov,
                season_avg_min,
                recent_pts,
                recent_reb,
                recent_ast,
                recent_stl,
                recent_blk,
                recent_fg3m,
                recent_tov,
                recent_min,
                recent_points_share_of_team,
                recent_points_share_of_game,
                minutes_delta_vs_season,
            ) in [
                (
                    1,
                    "Tyrese Maxey",
                    "PHI",
                    "G",
                    25.0,
                    4.2,
                    7.1,
                    1.2,
                    0.3,
                    3.2,
                    2.4,
                    35.8,
                    28.4,
                    4.8,
                    7.6,
                    1.4,
                    0.2,
                    3.5,
                    2.2,
                    36.4,
                    0.28,
                    0.14,
                    1.6,
                ),
                (
                    2,
                    "Jalen Brunson",
                    "NYK",
                    "G",
                    26.2,
                    3.6,
                    6.9,
                    1.0,
                    0.2,
                    2.7,
                    2.6,
                    35.2,
                    27.8,
                    3.9,
                    7.3,
                    1.1,
                    0.2,
                    2.9,
                    2.4,
                    35.8,
                    0.29,
                    0.15,
                    1.1,
                ),
                (
                    3,
                    "Mikal Bridges",
                    "NYK",
                    "F",
                    19.3,
                    4.7,
                    3.6,
                    1.1,
                    0.7,
                    2.4,
                    1.6,
                    35.4,
                    20.2,
                    4.9,
                    3.3,
                    1.3,
                    0.8,
                    2.6,
                    1.5,
                    35.6,
                    0.21,
                    0.10,
                    0.5,
                ),
                (
                    4,
                    "Jaren Jackson Jr.",
                    "MEM",
                    "F-C",
                    22.1,
                    6.4,
                    2.1,
                    1.0,
                    1.9,
                    1.8,
                    1.9,
                    32.5,
                    23.4,
                    6.8,
                    2.4,
                    1.1,
                    2.1,
                    2.0,
                    1.8,
                    33.1,
                    0.25,
                    0.12,
                    0.8,
                ),
                (
                    5,
                    "Brook Lopez",
                    "MIL",
                    "C",
                    14.8,
                    6.1,
                    1.4,
                    0.6,
                    2.3,
                    2.1,
                    1.4,
                    29.5,
                    15.4,
                    6.3,
                    1.6,
                    0.7,
                    2.5,
                    2.2,
                    1.5,
                    30.2,
                    0.19,
                    0.09,
                    0.6,
                ),
                (
                    6,
                    "Josh Hart",
                    "NYK",
                    "F",
                    14.2,
                    9.1,
                    5.4,
                    1.4,
                    0.5,
                    1.3,
                    1.8,
                    37.4,
                    15.3,
                    10.1,
                    5.9,
                    1.5,
                    0.6,
                    1.6,
                    1.6,
                    38.1,
                    0.18,
                    0.08,
                    1.4,
                ),
            ]
        ]
    )

    outputs = pipeline.build_player_similarity_outputs(feature_rows, cluster_count=4)

    assert set(outputs) == {"features", "archetypes"}
    assert len(outputs["features"]) == 6
    assert len(outputs["archetypes"]) == 6
    assert not outputs["features"].duplicated(subset=["season", "player_id"]).any()
    assert set(outputs["archetypes"]["archetype_label"]).issubset(
        pipeline.ALLOWED_ARCHETYPE_LABELS
    )
    for feature_name in pipeline.SIMILARITY_FEATURE_COLUMNS:
        assert f"norm_{feature_name}" in outputs["features"].columns
    assert outputs["archetypes"]["top_traits"].str.len().gt(0).all()


def test_build_player_similarity_outputs_excludes_insufficient_sample_rows():
    feature_rows = pd.DataFrame(
        [
            {
                "season": "2025-26",
                "as_of_date": "2026-02-11",
                "player_id": 1,
                "player_name": "Ready Player",
                "team_abbr": "PHI",
                "position": "G",
                "games_sampled": 20,
                "sample_status": "ready",
                **{
                    feature_name: 1.0
                    for feature_name in pipeline.SIMILARITY_FEATURE_COLUMNS
                },
            },
            {
                "season": "2025-26",
                "as_of_date": "2026-02-11",
                "player_id": 2,
                "player_name": "Limited Player",
                "team_abbr": "NYK",
                "position": "F",
                "games_sampled": 6,
                "sample_status": "limited_sample",
                **{
                    feature_name: 2.0
                    for feature_name in pipeline.SIMILARITY_FEATURE_COLUMNS
                },
            },
            {
                "season": "2025-26",
                "as_of_date": "2026-02-11",
                "player_id": 3,
                "player_name": "Insufficient Player",
                "team_abbr": "BOS",
                "position": "C",
                "games_sampled": 2,
                "sample_status": "insufficient_sample",
                **{
                    feature_name: 3.0
                    for feature_name in pipeline.SIMILARITY_FEATURE_COLUMNS
                },
            },
        ]
    )

    outputs = pipeline.build_player_similarity_outputs(feature_rows, cluster_count=3)

    assert outputs["features"]["player_id"].tolist() == [1, 2]
    assert outputs["archetypes"]["player_id"].tolist() == [1, 2]
