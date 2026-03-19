{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_SILVER', env_var('BQ_DATASET', 'nba_silver'))
) }}

with source_data as (
    select *
    from {{ source('bronze', 'raw_game_logs') }}
    where cast(season as {{ varchar_type() }}) = '2025-26'
      and date(game_date) between date('2025-07-01') and date('2026-06-30')
),
deduped as (
    select
        date(game_date) as game_date,
        cast(matchup as {{ varchar_type() }}) as matchup,
        upper(cast(wl as {{ varchar_type() }})) as wl,
        cast(min as {{ float64_type() }}) as min,
        cast(fgm as {{ float64_type() }}) as fgm,
        cast(fga as {{ float64_type() }}) as fga,
        cast(fg_pct as {{ float64_type() }}) as fg_pct,
        cast(ftm as {{ float64_type() }}) as ftm,
        cast(fta as {{ float64_type() }}) as fta,
        cast(ft_pct as {{ float64_type() }}) as ft_pct,
        cast(fg3m as {{ float64_type() }}) as fg3m,
        cast(fg3a as {{ float64_type() }}) as fg3a,
        cast(pts as {{ int64_type() }}) as pts,
        cast(reb as {{ int64_type() }}) as reb,
        cast(ast as {{ int64_type() }}) as ast,
        cast(stl as {{ int64_type() }}) as stl,
        cast(blk as {{ int64_type() }}) as blk,
        cast(tov as {{ int64_type() }}) as tov,
        cast(season as {{ varchar_type() }}) as season,
        cast(player_id as {{ int64_type() }}) as player_id,
        cast(player_name as {{ varchar_type() }}) as player_name,
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
