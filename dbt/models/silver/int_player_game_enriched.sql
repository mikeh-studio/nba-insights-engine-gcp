{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_SILVER', env_var('BQ_DATASET', 'nba_silver'))
) }}

select
    game_date,
    matchup,
    wl,
    min,
    pts,
    reb,
    ast,
    stl,
    blk,
    tov,
    season,
    player_id,
    player_name,
    upper(regexp_extract(matchup, r'^([A-Z]{2,3})')) as team_abbr,
    upper(regexp_extract(matchup, r'(?:vs\.|@)\s+([A-Z]{2,3})')) as opponent_abbr,
    case
        when regexp_contains(matchup, r'@') then 'AWAY'
        when regexp_contains(matchup, r'vs\.') then 'HOME'
        else 'UNKNOWN'
    end as home_away,
    ingested_at_utc
from {{ ref('stg_game_logs_clean') }}
