{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_SILVER', env_var('BQ_DATASET', 'nba_silver'))
) }}

select
    cast(game_id as {{ varchar_type() }}) as game_id,
    date(game_date) as game_date,
    cast(season as {{ varchar_type() }}) as season,
    cast(team_id as {{ int64_type() }}) as team_id,
    upper(cast(team_abbr as {{ varchar_type() }})) as team_abbr,
    cast(team_city_name as {{ varchar_type() }}) as team_city_name,
    cast(team_nickname as {{ varchar_type() }}) as team_nickname,
    cast(team_wins_losses as {{ varchar_type() }}) as team_wins_losses,
    cast(pts_qtr1 as {{ int64_type() }}) as pts_qtr1,
    cast(pts_qtr2 as {{ int64_type() }}) as pts_qtr2,
    cast(pts_qtr3 as {{ int64_type() }}) as pts_qtr3,
    cast(pts_qtr4 as {{ int64_type() }}) as pts_qtr4,
    cast(pts_ot1 as {{ int64_type() }}) as pts_ot1,
    cast(pts_ot2 as {{ int64_type() }}) as pts_ot2,
    cast(pts_ot3 as {{ int64_type() }}) as pts_ot3,
    cast(pts_ot4 as {{ int64_type() }}) as pts_ot4,
    cast(pts_ot5 as {{ int64_type() }}) as pts_ot5,
    cast(pts_ot6 as {{ int64_type() }}) as pts_ot6,
    cast(pts_ot7 as {{ int64_type() }}) as pts_ot7,
    cast(pts_ot8 as {{ int64_type() }}) as pts_ot8,
    cast(pts_ot9 as {{ int64_type() }}) as pts_ot9,
    cast(pts_ot10 as {{ int64_type() }}) as pts_ot10,
    cast(pts as {{ int64_type() }}) as pts,
    cast(ingested_at_utc as timestamp) as ingested_at_utc
from {{ source('bronze', 'raw_game_line_scores') }}
where cast(season as {{ varchar_type() }}) = '2025-26'
  and date(game_date) between date('2025-07-01') and date('2026-06-30')
