{{ config(
    materialized='view',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with ranked as (
    select
        snapshot_id,
        snapshot_date,
        created_at_utc,
        season,
        headline,
        dek,
        body,
        trend_player,
        trend_stat,
        trend_delta,
        freshness_ts,
        source_run_id,
        row_number() over (
            order by snapshot_date desc, created_at_utc desc, snapshot_id desc
        ) as row_num
    from {{ source('gold_runtime', 'analysis_snapshots') }}
    where season = '2025-26'
)
select
    snapshot_id,
    snapshot_date,
    created_at_utc,
    season,
    headline,
    dek,
    body,
    trend_player,
    trend_stat,
    trend_delta,
    freshness_ts,
    source_run_id
from ranked
where row_num = 1
