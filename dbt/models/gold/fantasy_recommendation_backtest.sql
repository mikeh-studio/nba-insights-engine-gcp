{{ config(
    materialized='view',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with future_games as (
    select
        i.insight_id,
        i.player_id,
        i.player_name,
        i.insight_type,
        i.as_of_date,
        i.priority_score,
        f.game_date,
        f.fantasy_points_simple,
        row_number() over (
            partition by i.insight_id
            order by f.game_date
        ) as future_game_num
    from {{ ref('fantasy_insights') }} i
    join {{ ref('fct_player_game_stats') }} f
        on i.player_id = f.player_id
       and f.game_date > i.as_of_date
),
rolled as (
    select
        insight_id,
        player_id,
        any_value(player_name) as player_name,
        any_value(insight_type) as insight_type,
        any_value(as_of_date) as as_of_date,
        any_value(priority_score) as priority_score,
        {{ countif('future_game_num <= 3') }} as next_3_games_sample,
        round(avg(case when future_game_num <= 3 then fantasy_points_simple end), 1) as avg_fantasy_points_next_3_games,
        {{ countif('future_game_num <= 7') }} as next_7_games_sample,
        round(avg(case when future_game_num <= 7 then fantasy_points_simple end), 1) as avg_fantasy_points_next_7_games
    from future_games
    group by 1, 2
)
select
    insight_id,
    player_id,
    player_name,
    insight_type,
    as_of_date,
    priority_score,
    next_3_games_sample,
    avg_fantasy_points_next_3_games,
    next_7_games_sample,
    avg_fantasy_points_next_7_games,
    case
        when next_3_games_sample = 0 then 'pending'
        when avg_fantasy_points_next_3_games >= 25 then 'hit'
        else 'miss'
    end as backtest_status
from rolled
