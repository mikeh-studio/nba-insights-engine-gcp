{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with ranked as (
    select
        player_id,
        player_name,
        season,
        ingested_at_utc,
        row_number() over (
            partition by player_id
            order by ingested_at_utc desc, game_date desc
        ) as row_num
    from {{ ref('int_player_game_enriched') }}
)
select
    player_id,
    player_name,
    season as latest_season,
    ingested_at_utc as last_seen_at_utc
from ranked
where row_num = 1
