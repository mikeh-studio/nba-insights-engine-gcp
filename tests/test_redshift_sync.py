from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import nba_pipeline as pipeline
import nba_redshift_sync as redshift_sync


def test_raw_game_logs_redshift_columns_match_bigquery_schema():
    expected = [field.name.lower() for field in pipeline.get_game_logs_schema()]
    assert redshift_sync.get_raw_table_column_names("raw_game_logs") == expected


def test_raw_schedule_redshift_columns_match_bigquery_schema():
    expected = [field.name.lower() for field in pipeline.get_schedule_schema()]
    assert redshift_sync.get_raw_table_column_names("raw_schedule") == expected


def test_missing_column_migration_is_additive():
    statements = redshift_sync._build_add_missing_column_ddls(
        "nba_bronze",
        "raw_game_logs",
        ["game_date", "matchup", "wl", "min"],
    )

    assert statements
    assert all(
        statement.startswith("ALTER TABLE nba_bronze.raw_game_logs ADD COLUMN")
        for statement in statements
    )
    assert not any("DROP TABLE" in statement for statement in statements)
    assert any(
        "ADD COLUMN fgm DOUBLE PRECISION;" in statement for statement in statements
    )
    assert any(
        "ADD COLUMN plus_minus DOUBLE PRECISION;" in statement
        for statement in statements
    )
