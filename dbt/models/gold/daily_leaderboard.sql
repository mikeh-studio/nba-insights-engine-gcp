{{ config(
    materialized='view',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with ranked as (
    select
        season,
        game_date,
        player_name,
        matchup,
        pts,
        reb,
        ast,
        row_number() over (
            partition by game_date
            order by pts desc, player_name
        ) as pts_rank,
        row_number() over (
            partition by game_date
            order by reb desc, player_name
        ) as reb_rank,
        row_number() over (
            partition by game_date
            order by ast desc, player_name
        ) as ast_rank
    from {{ ref('fct_player_game_stats') }}
)
select
    p.season,
    p.game_date,
    p.player_name as pts_leader,
    p.matchup as pts_matchup,
    p.pts,
    r.player_name as reb_leader,
    r.reb,
    a.player_name as ast_leader,
    a.ast
from ranked p
join ranked r on p.game_date = r.game_date and r.reb_rank = 1
join ranked a on p.game_date = a.game_date and a.ast_rank = 1
where p.pts_rank = 1
