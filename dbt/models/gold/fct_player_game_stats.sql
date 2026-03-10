{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

select
    player_id,
    player_name,
    team_abbr,
    opponent_abbr,
    home_away,
    game_date,
    matchup,
    season,
    wl,
    min,
    pts,
    reb,
    ast,
    stl,
    blk,
    tov,
    ingested_at_utc
from {{ ref('int_player_game_enriched') }}
