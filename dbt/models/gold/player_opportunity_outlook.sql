{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

{% set today = 'current_date()' if target.type == 'bigquery' else 'current_date' %}
{% set next_week = 'date_add(current_date(), interval 7 day)' if target.type == 'bigquery' else "current_date + interval '7 day'" %}

with latest_team as (
    select
        player_id,
        player_name,
        season,
        team_abbr,
        row_number() over (
            partition by player_id
            order by game_date desc, ingested_at_utc desc
        ) as row_num
    from {{ ref('fct_player_game_stats') }}
),
player_base as (
    select
        player_id,
        player_name,
        season,
        team_abbr
    from latest_team
    where row_num = 1
),
next_opponent_cte as (
    select
        team_abbr,
        opponent_abbr,
        row_number() over (
            partition by team_abbr
            order by schedule_date, opponent_abbr
        ) as rn
    from {{ ref('stg_schedule_clean') }}
    where lower(game_status) = 'scheduled'
      and schedule_date >= {{ today }}
),
schedule_outlook as (
    select
        team_abbr,
        {{ countif('(schedule_date between ' ~ today ~ ' and ' ~ next_week ~ ')') }} as next_7d_games,
        {{ countif('(schedule_date between ' ~ today ~ ' and ' ~ next_week ~ ') and is_back_to_back') }} as next_7d_back_to_backs,
        min(case when schedule_date >= {{ today }} then schedule_date end) as first_game_date
    from {{ ref('stg_schedule_clean') }}
    where lower(game_status) = 'scheduled'
    group by 1
)
select
    {{ today }} as as_of_date,
    p.season,
    p.player_id,
    p.player_name,
    p.team_abbr,
    s.first_game_date,
    n.opponent_abbr as next_opponent,
    coalesce(s.next_7d_games, 0) as next_7d_games,
    coalesce(s.next_7d_back_to_backs, 0) as next_7d_back_to_backs,
    round(
        coalesce(s.next_7d_games, 0) * 1.8
        - coalesce(s.next_7d_back_to_backs, 0) * 0.5,
        2
    ) as opportunity_score
from player_base p
left join schedule_outlook s
    on p.team_abbr = s.team_abbr
left join next_opponent_cte n
    on p.team_abbr = n.team_abbr and n.rn = 1
