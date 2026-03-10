{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with ranked as (
    select
        team_abbr,
        season,
        max(game_date) as last_game_date,
        row_number() over (
            partition by team_abbr
            order by max(game_date) desc, season desc
        ) as row_num
    from {{ ref('int_player_game_enriched') }}
    group by 1, 2
)
select
    team_abbr,
    season as latest_season,
    last_game_date
from ranked
where row_num = 1
