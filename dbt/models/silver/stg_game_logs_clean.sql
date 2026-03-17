{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_SILVER', env_var('BQ_DATASET', 'nba_silver'))
) }}

with source_data as (
    select *
    from {{ source('bronze', 'raw_game_logs') }}
    where cast(season as string) = '2025-26'
      and date(game_date) between date('2025-07-01') and date('2026-06-30')
),
deduped as (
    select
        date(game_date) as game_date,
        cast(matchup as string) as matchup,
        upper(cast(wl as string)) as wl,
        cast(min as float64) as min,
        cast(fgm as float64) as fgm,
        cast(fga as float64) as fga,
        cast(fg_pct as float64) as fg_pct,
        cast(ftm as float64) as ftm,
        cast(fta as float64) as fta,
        cast(ft_pct as float64) as ft_pct,
        cast(fg3m as float64) as fg3m,
        cast(fg3a as float64) as fg3a,
        cast(pts as int64) as pts,
        cast(reb as int64) as reb,
        cast(ast as int64) as ast,
        cast(stl as int64) as stl,
        cast(blk as int64) as blk,
        cast(tov as int64) as tov,
        cast(season as string) as season,
        cast(player_id as int64) as player_id,
        cast(player_name as string) as player_name,
        cast(ingested_at_utc as timestamp) as ingested_at_utc,
        row_number() over (
            partition by player_id, game_date, matchup
            order by ingested_at_utc desc
        ) as row_num
    from source_data
)
select
    game_date,
    matchup,
    wl,
    min,
    fgm,
    fga,
    fg_pct,
    ftm,
    fta,
    ft_pct,
    fg3m,
    fg3a,
    pts,
    reb,
    ast,
    stl,
    blk,
    tov,
    season,
    player_id,
    player_name,
    ingested_at_utc
from deduped
where row_num = 1
