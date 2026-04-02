from __future__ import annotations

from datetime import UTC, datetime

from app.repository import build_freshness_payload


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
