from __future__ import annotations

import os
from dataclasses import dataclass

SUPPORTED_SEASON = "2025-26"


@dataclass(frozen=True)
class Settings:
    project_id: str
    gold_dataset: str
    metadata_dataset: str
    freshness_threshold_hours: int


def get_settings() -> Settings:
    return Settings(
        project_id=os.getenv("BQ_PROJECT", os.getenv("GCP_PROJECT_ID", "")),
        gold_dataset=os.getenv("BQ_DATASET_GOLD", "nba_gold"),
        metadata_dataset=os.getenv("BQ_METADATA_DATASET", "nba_metadata"),
        freshness_threshold_hours=int(os.getenv("API_FRESHNESS_THRESHOLD_HOURS", "36")),
    )
