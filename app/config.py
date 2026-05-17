from __future__ import annotations

import os
from dataclasses import dataclass

SUPPORTED_SEASON = "2025-26"


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    project_id: str
    gold_dataset: str
    metadata_dataset: str
    freshness_threshold_hours: int
    max_search_results: int
    agent_dataset: str = "nba_agent"
    openai_api_key: str | None = None
    openai_agent_model: str = "gpt-5.4-mini"
    openai_agent_enabled: bool = True
    agent_max_tool_calls: int = 6
    agent_rate_limit_per_minute: int = 12


def get_settings() -> Settings:
    return Settings(
        project_id=os.getenv("BQ_PROJECT", os.getenv("GCP_PROJECT_ID", "")),
        gold_dataset=os.getenv("BQ_DATASET_GOLD", "nba_gold"),
        agent_dataset=os.getenv("BQ_DATASET_AGENT", "nba_agent"),
        metadata_dataset=os.getenv("BQ_METADATA_DATASET", "nba_metadata"),
        freshness_threshold_hours=int(os.getenv("API_FRESHNESS_THRESHOLD_HOURS", "36")),
        max_search_results=int(os.getenv("API_MAX_SEARCH_RESULTS", "12")),
        openai_api_key=os.getenv("OPENAI_API_KEY") or None,
        openai_agent_model=os.getenv("OPENAI_AGENT_MODEL", "gpt-5.4-mini"),
        openai_agent_enabled=_env_bool("OPENAI_AGENT_ENABLED", True),
        agent_max_tool_calls=int(os.getenv("AGENT_MAX_TOOL_CALLS", "6")),
        agent_rate_limit_per_minute=int(os.getenv("AGENT_RATE_LIMIT_PER_MINUTE", "12")),
    )
